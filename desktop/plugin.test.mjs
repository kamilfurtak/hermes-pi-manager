import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { JSDOM } from 'jsdom';
import { createElement as h, act } from 'react';
import { createRoot } from 'react-dom/client';

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
globalThis.piTestHost = { state };
const require = createRequire(import.meta.url);
const source = (await readFile(new URL('./plugin.js', import.meta.url), 'utf8'))
  .replace("from 'react'", `from '${pathToFileURL(require.resolve('react')).href}'`)
  .replace("import { host, TRANSCRIPT_DIRECTIVE_AREA } from '@hermes/plugin-sdk';", "const host = globalThis.piTestHost; const TRANSCRIPT_DIRECTIVE_AREA = 'transcript.directives';");
const { default: plugin, pollActivity, PiCard } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
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
