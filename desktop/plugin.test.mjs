import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { JSDOM } from 'jsdom';
import { createElement as h, act } from 'react';
import { createRoot } from 'react-dom/client';
import { Streamdown } from 'streamdown';

function atom(initial) {
  let value = initial;
  const listeners = new Set();
  return { get: () => value, listen: callback => { listeners.add(callback); return () => listeners.delete(callback); },
    set: next => { value = next; for (const callback of listeners) callback(); } };
}
const state = {
  focusedStoredSessionId: atom('session-a'), profile: atom('default'), connectionId: atom('mini'),
  focusedSessionOwner: atom({ profile: 'default', connectionId: 'mini' }), gateway: atom('open'),
};
globalThis.piTestSdk = { host: { state }, Streamdown, TRANSCRIPT_DIRECTIVE_AREA: 'transcript.directives' };
const require = createRequire(import.meta.url);
const source = (await readFile(new URL('./plugin.js', import.meta.url), 'utf8'))
  .replace("from 'react'", `from '${pathToFileURL(require.resolve('react')).href}'`)
  .replace("import * as sdk from '@hermes/plugin-sdk';", "const sdk = globalThis.piTestSdk;");
const { default: plugin, pollActivity, PiCard, activityEntries } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const view = (extra = {}) => ({ task_id: 'pi-test', session_id: 'session-a', execution_state: 'RUNNING', verification_state: 'NOT_RUN',
  activity: { seq: 1, updated_at: Date.now() / 1000, text: 'First chunk', tool: null, entries: [], tools_completed: 0 }, ...extra });

test('registers one native transcript directive', () => {
  const registered = [];
  plugin.register({ register: item => registered.push(item) });
  assert.equal(registered.length, 1);
  assert.equal(registered[0].area, 'transcript.directives');
  assert.equal(registered[0].data.name, 'pi-live');
});

test('polling is serial, updates after the parent turn, and stops after the final snapshot flush', async () => {
  let active = 0, maximum = 0, calls = 0;
  const received = [];
  const stop = pollActivity({ taskId: 'pi-test', sessionId: 'session-a', interval: 5,
    rest: async () => {
      maximum = Math.max(maximum, ++active);
      const call = ++calls;
      await sleep(8); active--;
      return view({ execution_state: call >= 2 ? 'SETTLED' : 'RUNNING',
        activity: { seq: call >= 3 ? 3 : call } });
    }, onData: data => received.push(data), onError: assert.fail });
  try {
    await sleep(150);
    assert.equal(maximum, 1);
    assert.equal(calls, 4);
    assert.equal(received.at(-1).activity.seq, 3);
  } finally { stop(); }
});

test('navigation discards a reply already in flight and never polls again', async () => {
  let resolve, calls = 0;
  const received = [];
  const stop = pollActivity({ taskId: 'pi-test', sessionId: 'session-a', interval: 2,
    rest: () => { calls++; return new Promise(done => { resolve = done; }); },
    onData: data => received.push(data), onError: assert.fail });
  stop();
  resolve(view());
  await sleep(20);
  assert.deepEqual(received, []);
  assert.equal(calls, 1);
});

test('a foreign session response never renders', async () => {
  const errors = [];
  const stop = pollActivity({ taskId: 'pi-test', sessionId: 'session-a',
    rest: async () => view({ session_id: 'foreign' }), onData: assert.fail, onError: error => errors.push(error) });
  await sleep(5); stop();
  assert.equal(errors.length, 1);
});

test('React card renders live tool output as text and clears on profile switch', async () => {
  const dom = new JSDOM('<!doctype html><div id="root"></div>', { url: 'http://localhost' });
  globalThis.window = dom.window;
  globalThis.document = dom.window.document;
  globalThis.IS_REACT_ACT_ENVIRONMENT = true;
  const root = createRoot(document.getElementById('root'));
  let calls = 0;
  const ctx = { rest: async () => { calls++; return view({ active_tool: 'bash', activity: {
    seq: 2, updated_at: Date.now() / 1000, tools_completed: 2, text: '', entries: [],
    tool: { name: 'bash', text: 'line 1\n<script>unsafe()</script>\nline 2' },
  } }); } };
  try {
    await act(async () => { root.render(h(PiCard, { ctx, taskId: 'pi-test' })); });
    assert.match(document.body.textContent, /line 1/);
    assert.equal(document.querySelectorAll('script').length, 0);
    await act(async () => { document.querySelector('button').click(); });
    assert.match(document.querySelector('pre').textContent, /line 2/);
    assert.equal(document.querySelector('button').getAttribute('aria-expanded'), 'true');
    await act(async () => { state.profile.set('other'); });
    assert.doesNotMatch(document.body.textContent, /line 1/);
    assert.match(document.body.textContent, /wstrzymany/);
    assert.equal(calls, 1);
  } finally {
    await act(async () => root.unmount());
    state.profile.set('default');
    dom.window.close();
  }
});

test('legacy clipped final text replaces its history copy and preserves chronology', () => {
  const text = '## Final result\n' + 'one sentence. '.repeat(150);
  const entries = [{ kind: 'assistant', text: '[… wcześniejszy tekst pominięty …]\n' + text.slice(-1024) },
    { kind: 'tool', id: 't1', name: 'bash', text: 'done' }];
  const result = activityEntries({ text, entries });
  assert.equal(result.length, 2);
  assert.equal(result[0].text, text);
  assert.equal(result[1].id, 't1');
  assert.notEqual(entries[0].text, text, 'does not mutate the received snapshot');
});

test('message IDs preserve repeated identical progress messages', () => {
  const entries = [{ kind: 'assistant', id: 'm1', text: 'Checking…' }];
  const result = activityEntries({ text: 'Checking…', text_id: 'm2', entries });
  assert.deepEqual(result.map(entry => entry.id), ['m1', 'm2']);
});

test('completed tools fold separately while the final answer renders once as Markdown', async () => {
  const dom = new JSDOM('<!doctype html><div id="root"></div>', { url: 'http://localhost' });
  globalThis.window = dom.window;
  globalThis.document = dom.window.document;
  globalThis.IS_REACT_ACT_ENVIRONMENT = true;
  const root = createRoot(document.getElementById('root'));
  const text = '## Wynik\n\n**12 testów zaliczonych**.\n\n- Naprawiono `backup_status.py`.\n- Zapisano wynik.\n\n' + 'Sprawdzono wynik. '.repeat(80);
  const activity = { seq: 2, tools_completed: 2, text, text_id: 'm2', tool: null, entries: [
    { kind: 'assistant', id: 'm1', text: 'Sprawdzam kontrakt.' },
    { kind: 'tool', id: 't1', name: 'bash', text: '<script>unsafe()</script>\n12 tests OK' },
    { kind: 'assistant', id: 'm2', text: '[… wcześniejszy tekst pominięty …]\n' + text.slice(-1024) },
  ] };
  try {
    await act(async () => { root.render(h(PiCard, { ctx: { rest: async () => view({ execution_state: 'SETTLED', activity }) }, taskId: 'pi-test' })); });
    await act(async () => { document.querySelector('button').click(); });
    assert.equal(document.querySelectorAll('h2').length, 1);
    assert.equal(document.querySelector('h2').textContent, 'Wynik');
    assert.equal(document.querySelector('.pi-prose strong').textContent, '12 testów zaliczonych');
    assert.equal(document.querySelectorAll('.pi-prose li').length, 2);
    assert.equal(document.querySelectorAll('.pi-prose code').length, 1);
    assert.equal(document.querySelectorAll('script').length, 0);
    assert.equal(document.querySelectorAll('pre').length, 1, 'only tool output is a log block');
    assert.equal(document.querySelector('details').open, false);
    assert.match(document.querySelector('summary').textContent, /Terminal.*Gotowe/);
    await act(async () => { document.querySelector('summary').click(); });
    assert.equal(document.querySelector('details').open, true);
    // The bounded history moves as a new snapshot arrives. Stable IDs retain
    // the user's opened tool instead of collapsing it on every update.
    await act(async () => { root.render(h(PiCard, { ctx: { rest: async () => view({ execution_state: 'SETTLED',
      activity: { ...activity, seq: 3, entries: activity.entries.slice(1) } }) }, taskId: 'pi-test' })); });
    assert.equal(document.querySelector('details').open, true);
  } finally {
    await act(async () => root.unmount());
    dom.window.close();
  }
});

test('Markdown cannot execute HTML or fetch worker-provided images', async () => {
  const dom = new JSDOM('<!doctype html><div id="root"></div>', { url: 'http://localhost' });
  globalThis.window = dom.window;
  globalThis.document = dom.window.document;
  globalThis.IS_REACT_ACT_ENVIRONMENT = true;
  const root = createRoot(document.getElementById('root'));
  try {
    const text = '![image](https://example.invalid/pixel)\n\n<script>unsafe()</script>\n\n[link](javascript:alert(1))';
    await act(async () => { root.render(h(PiCard, { ctx: { rest: async () => view({ activity: { seq: 1, text, entries: [] } }) }, taskId: 'pi-test' })); });
    await act(async () => { document.querySelector('button').click(); });
    assert.equal(document.querySelectorAll('script, img, iframe').length, 0);
    assert.equal(document.querySelectorAll('a[href^="javascript:"]').length, 0);
  } finally {
    await act(async () => root.unmount());
    dom.window.close();
  }
});
