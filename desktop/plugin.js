import { createElement as h, useEffect, useLayoutEffect, useRef, useState, useSyncExternalStore } from 'react';
import * as sdk from '@hermes/plugin-sdk';

const { host, TRANSCRIPT_DIRECTIVE_AREA } = sdk;

const FINAL = new Set(['SETTLED', 'ABORTED', 'CRASHED']);
const LABELS = {
  STARTING: 'Uruchamianie', RUNNING: 'Pi pracuje', TOOL_RUNNING: 'Narzędzie pracuje',
  WAITING: 'Oczekiwanie', STALLED: 'Brak postępu', UNRESPONSIVE: 'Brak odpowiedzi',
  SETTLED: 'Pi zakończył pracę', ABORTED: 'Przerwano', CRASHED: 'Błąd procesu',
};
const box = { border: '1px solid var(--ui-border, var(--border, #666))', borderRadius: 10, padding: '12px 14px', margin: '8px 0', minWidth: 0 };
const muted = { opacity: 0.65, fontSize: 12 };
const pre = { whiteSpace: 'pre-wrap', overflowWrap: 'anywhere', fontSize: 12, lineHeight: 1.6,
  fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace', margin: '8px 0 0', padding: '10px 12px',
  maxHeight: 260, overflow: 'auto', background: 'transparent', color: 'inherit', border: box.border, borderRadius: 6, boxShadow: 'none' };
const OMITTED = '[… wcześniejszy tekst pominięty …]\n';
const MAX_TRANSCRIPT_CHARS = 256 * 1024;
const TOOL_NAMES = { bash: 'Terminal', read: 'Odczyt pliku', edit: 'Edycja pliku', write: 'Zapis pliku',
  grep: 'Wyszukiwanie w plikach', find: 'Wyszukiwanie plików', ls: 'Lista plików' };
const proseStyles = `
.pi-live-view .pi-prose { font-family: inherit; font-size: 13px; line-height: 1.65; overflow-wrap: anywhere; }
.pi-live-view .pi-prose p { margin: 6px 0; }
.pi-live-view .pi-prose :is(h1,h2,h3,h4,h5,h6) { font-size: 14px; line-height: 1.5; font-weight: 650; margin: 14px 0 6px; }
.pi-live-view .pi-prose :is(ul,ol) { margin: 6px 0; padding-inline-start: 22px; }
.pi-live-view .pi-prose ul { list-style-type: disc; }
.pi-live-view .pi-prose ol { list-style-type: decimal; }
.pi-live-view .pi-prose li { margin: 3px 0; }
.pi-live-view .pi-prose code { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.92em; }
.pi-live-view .pi-prose pre { font-size: 12px; white-space: pre-wrap; max-height: 260px; overflow: auto; }
.pi-live-view .pi-prose > :first-child { margin-top: 0; }
.pi-live-view .pi-tool-summary { cursor: pointer; font-size: 13px; line-height: 1.8; }
.pi-live-view .pi-tool-summary:focus-visible { outline: 2px solid currentColor; outline-offset: 4px; border-radius: 3px; }
`;
// Keep worker prose on the host's Markdown pipeline. Raw tool output stays text;
// image references are labels here so expanding a log does not fetch images.
const markdownComponents = {
  strong: ({ children }) => h('strong', null, children),
  img: ({ alt }) => h('span', { style: muted }, alt ? `[Obraz: ${alt}]` : '[Obraz]'),
};

export function activityEntries(activity) {
  const entries = [...(activity?.entries || [])];
  if (!activity?.text) return entries;
  const index = entries.findLastIndex(entry => entry.kind === 'assistant' && (
    activity.text_id ? entry.id === activity.text_id
      : entry.text === activity.text || entry.text === OMITTED + activity.text.slice(-1024)
  ));
  // The current message has a longer buffer than history. Replace its short
  // copy in place, including legacy snapshots which have no message IDs.
  const entry = { kind: 'assistant', id: activity.text_id || 'current', text: activity.text, current: true };
  if (index >= 0) entries[index] = { ...entries[index], ...entry };
  else entries.push(entry);
  return entries;
}

function PiText({ text, streaming }) {
  const shortened = text.startsWith(OMITTED);
  const body = shortened ? text.slice(OMITTED.length) : text;
  return h('div', null,
    shortened ? h('p', { style: muted }, 'Początek tego fragmentu pominięto.') : null,
    sdk.Streamdown
      ? h(sdk.Streamdown, { className: 'pi-prose', controls: false, mode: 'static',
        parseIncompleteMarkdown: streaming, components: markdownComponents }, body)
      : h('div', { className: 'pi-prose', style: { whiteSpace: 'pre-wrap' } }, body),
  );
}

function ToolEntry({ entry, live = false }) {
  const text = entry.text || '';
  const shortened = text.startsWith(OMITTED);
  const body = shortened ? text.slice(OMITTED.length) : text;
  const status = live ? 'Na żywo' : entry.error ? 'Błąd' : 'Gotowe';
  return h('details', { open: live || entry.error ? true : undefined, 'data-pi-tool': entry.id },
    h('summary', { className: 'pi-tool-summary' },
      h('strong', { style: { fontWeight: 550 } }, TOOL_NAMES[entry.name] || entry.name || 'Narzędzie'),
      h('span', { style: { ...muted, marginLeft: 10, ...(entry.error ? { color: 'var(--destructive, #dc6666)', opacity: 1 } : {}) } }, status)),
    shortened ? h('p', { style: { ...muted, margin: '4px 0' } }, 'Fragment wyjścia; wcześniejszą część pominięto.') : null,
    body ? h(sdk.LogView || 'pre', { style: sdk.LogView ? { maxHeight: 260, marginTop: 8 } : pre }, body)
      : h('p', { style: muted }, live ? 'Narzędzie jeszcze nie wysłało wyjścia.' : 'Bez wyjścia tekstowego.'),
  );
}

function ActivityTimeline({ activity, finished }) {
  const entries = activityEntries(activity);
  return h('div', { style: { marginTop: 14, borderTop: box.border, paddingTop: 12 } },
    h('div', { style: { ...muted, marginBottom: 14 } }, 'Ostatnie wpisy. Wyniki narzędzi rozwiniesz osobno.'),
    h('ol', { 'aria-label': 'Przebieg pracy Pi', style: { listStyle: 'none', padding: 0, margin: 0, display: 'grid', gap: 14 } },
      ...entries.map((entry, index) => h('li', { key: entry.id || `${entry.kind}:${entry.text}`, style: { minWidth: 0 } },
        entry.kind === 'tool' ? h(ToolEntry, { entry })
          : h('div', { style: { borderLeft: box.border, paddingLeft: 12 } },
            h('div', { style: { ...muted, marginBottom: 4, fontWeight: 600 } },
              entry.current && finished && index === entries.length - 1 ? 'Wynik Pi' : 'Pi'),
            h(PiText, { text: entry.text, streaming: entry.current && !finished })))),
      activity.tool ? h('li', { key: activity.tool.id || 'live', style: { minWidth: 0 } },
        h(ToolEntry, { entry: activity.tool, live: true })) : null),
  );
}

function LiveTranscript({ transcript }) {
  const scroll = useRef(null);
  const follow = useRef(true);
  const [following, setFollowing] = useState(true);
  const latest = () => {
    follow.current = true;
    setFollowing(true);
    if (scroll.current) scroll.current.scrollTop = scroll.current.scrollHeight;
  };
  useLayoutEffect(() => {
    if (follow.current && scroll.current) scroll.current.scrollTop = scroll.current.scrollHeight;
  }, [transcript.text]);
  return h('div', { style: { marginTop: 14, borderTop: box.border, paddingTop: 12 } },
    h('div', { style: { ...muted, display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12, marginBottom: 8 } },
      h('span', null, following ? 'Przebieg · śledzenie najnowszych wpisów' : 'Przebieg · czytasz wcześniejsze wpisy'),
      !following ? h(sdk.Button || 'button', { type: 'button', onClick: latest,
        ...(sdk.Button ? { variant: 'text', size: 'micro' } : {}) }, 'Do najnowszych') : null),
    transcript.truncated ? h('p', { style: muted }, 'Wyświetlono końcową część dziennika; wcześniejsze wpisy przekroczyły limit podglądu.') : null,
    h('div', { ref: scroll, tabIndex: 0, role: 'log', 'aria-label': 'Dziennik pracy Pi', 'aria-live': 'off',
      'data-selectable-text': 'true',
      style: { maxHeight: 'min(55vh, 520px)', overflow: 'auto', overflowAnchor: 'none', border: box.border, borderRadius: 6 },
      onScroll: event => {
        const el = event.currentTarget;
        follow.current = el.scrollHeight - el.scrollTop - el.clientHeight <= 32;
        setFollowing(follow.current);
      } },
      h(sdk.LogView || 'pre', { style: { ...(!sdk.LogView ? pre : {}),
        margin: 0, maxHeight: 'none', overflow: 'visible', border: 0,
        whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' } },
      transcript.text || 'Oczekiwanie na pierwsze wpisy…')),
  );
}

function mergeTranscript(previous, update) {
  if (update.error) return { ...(previous || update), error: update.error };
  if (!update.available) return update;
  const text = (update.reset ? '' : previous?.text || '') + update.text;
  const clipped = text.length > MAX_TRANSCRIPT_CHARS;
  const tail = clipped ? text.slice(-MAX_TRANSCRIPT_CHARS) : text;
  return { ...update, text: clipped ? tail.slice(tail.indexOf('\n') + 1) : tail,
    truncated: update.truncated || (!update.reset && previous?.truncated) || clipped };
}

function useAtom(atom) {
  return useSyncExternalStore(
    callback => atom?.listen?.(callback) || (() => {}),
    () => atom?.get?.() ?? null,
  );
}

/** Cancel scheduling and discard in-flight replies on navigation/unload. */
export function pollActivity({ rest, taskId, sessionId, onData, onError, interval = 1000, includeTranscript = false }) {
  let stopped = false;
  let timer;
  let terminalSnapshot;
  let transcript;
  async function tick() {
    let delay = interval;
    let done = false;
    try {
      const tail = includeTranscript ? `&transcript=true&cursor=${encodeURIComponent(transcript?.cursor || '')}` : '';
      const data = await rest(`/activity?task_id=${encodeURIComponent(taskId)}&session_id=${encodeURIComponent(sessionId)}${tail}`, { timeoutMs: 5000 });
      if (stopped) return;
      if (data?.task_id !== taskId || data?.session_id !== sessionId) throw new Error('Odpowiedź pochodzi z innego zadania.');
      if (includeTranscript && data.transcript) {
        transcript = mergeTranscript(transcript, data.transcript);
        data.transcript = transcript;
      } else if (includeTranscript) {
        data.transcript = { available: false, legacy_backend: true };
      }
      onData(data);
      // The task row can settle just before the coalescing writer flushes.
      // Observe two identical terminal snapshots before stopping the poll.
      const terminal = FINAL.has(data.execution_state) && data.verification_state !== 'PENDING' && !transcript?.error;
      const signature = JSON.stringify([data.execution_state, data.verification_state, data.activity?.seq,
        transcript?.available, transcript?.cursor]);
      done = terminal && signature === terminalSnapshot;
      terminalSnapshot = terminal ? signature : undefined;
    } catch {
      if (stopped) return;
      onError('Podgląd niedostępny. Sprawdzam ponownie…');
      delay = Math.max(3000, interval);
    }
    if (!stopped && !done) timer = setTimeout(tick, delay);
  }
  void tick();
  return () => { stopped = true; clearTimeout(timer); };
}

export function PiCard({ ctx, taskId }) {
  const sessionId = useAtom(host.state.focusedStoredSessionId);
  const profile = useAtom(host.state.profile);
  const connection = useAtom(host.state.connectionId);
  const owner = useAtom(host.state.focusedSessionOwner);
  const gateway = useAtom(host.state.gateway);
  const [view, setView] = useState(null);
  const [expanded, setExpanded] = useState(false);
  const valid = typeof taskId === 'string' && /^[A-Za-z0-9_-]{1,160}$/.test(taskId);
  // ctx.rest follows the active backend. A tile owned by another backend
  // must first be focused there; never silently route via the ambient profile.
  const paused = !sessionId || gateway !== 'open' || (owner && (
    owner.profile !== profile || (connection && owner.connectionId !== connection)
  ));
  const scope = JSON.stringify([taskId, sessionId, profile, connection, owner, paused]);
  const current = view?.scope === scope ? view : null;

  useEffect(() => {
    if (!valid || paused) return;
    const stop = pollActivity({
      rest: ctx.rest, taskId, sessionId, includeTranscript: expanded,
      onData: data => setView({ scope, data, error: null }),
      onError: error => setView(previous => ({ scope, data: previous?.scope === scope ? previous.data : null, error })),
    });
    return stop;
  }, [ctx, scope, valid, paused, expanded]);

  if (!valid) return h('p', { style: muted }, 'Nieprawidłowy identyfikator zadania Pi.');
  const data = current?.data;
  const activity = data?.activity;
  const tool = activity?.tool;
  const finished = data && FINAL.has(data.execution_state);
  const status = paused ? 'Podgląd wstrzymany — otwórz połączenie tej rozmowy'
    : data?.verification_state === 'PENDING' ? 'Weryfikacja w toku'
    : (LABELS[data?.execution_state] || 'Łączenie z zadaniem Pi…');
  const text = activity?.text || [...(activity?.entries || [])].reverse().find(e => e.kind === 'assistant' && e.text)?.text || tool?.text || '';
  const summary = text.replace(/^#+\s*/gm, '').replace(/[*`]/g, '').replace(/\s+/g, ' ').trim();
  const tone = ['ABORTED', 'CRASHED'].includes(data?.execution_state) || ['FAIL', 'UNRUNNABLE'].includes(data?.verification_state) ? 'bad'
    : ['STALLED', 'UNRESPONSIVE'].includes(data?.execution_state) ? 'warn' : data?.verification_state === 'PASS' ? 'good' : 'muted';
  const updated = activity?.updated_at ? new Date(activity.updated_at * 1000).toLocaleTimeString() : null;

  return h('section', { className: 'pi-live-view', style: box, 'aria-label': `Praca Pi ${taskId}` },
    h('style', null, proseStyles),
    h('div', { style: { display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', gap: 12 } },
      h('strong', { style: { display: 'flex', alignItems: 'center', gap: 8 } },
        sdk.StatusDot ? h(sdk.StatusDot, { tone }) : null, `Pi · ${status}`),
      h(sdk.Button || 'button', { type: 'button', ...(sdk.Button ? { variant: 'text', size: 'micro' } : {}),
        onClick: () => setExpanded(!expanded), 'aria-expanded': expanded,
        style: { cursor: 'pointer', fontSize: 12, flexShrink: 0 } }, expanded ? 'Zwiń' : 'Pokaż przebieg')),
    h('div', { style: muted }, taskId,
      activity ? ` · ukończone wywołania narzędzi: ${activity.tools_completed}` : '',
      updated ? ` · ostatnia aktywność ${updated}` : ''),
    data?.active_tool ? h('div', { style: { fontSize: 13, marginTop: 6 } }, `Narzędzie: ${data.active_tool}`) : null,
    current?.error ? h('p', { role: 'status', style: muted }, current.error) : null,
    expanded && data?.transcript?.error ? h('p', { role: 'status', style: muted }, data.transcript.error) : null,
    expanded && data?.transcript?.legacy_backend ? h('p', { role: 'status', style: muted },
      'Ten backend udostępnia jeszcze skrócony podgląd. Uruchom ponownie backend Hermesa, aby wczytać nowy dziennik.') : null,
    !paused && data && !activity ? h('p', { style: muted }, 'Brak zapisu strumienia dla tego zadania. Nowy worker utworzy podgląd po załadowaniu dodatku przez backend.') : null,
    !expanded && summary ? h('p', { style: { fontSize: 13, overflowWrap: 'anywhere', marginTop: 8 } },
      summary.length > 240 ? summary.slice(0, 239) + '…' : summary) : null,
    expanded ? data?.transcript?.available
      ? h(LiveTranscript, { key: scope, transcript: data.transcript })
      : activity ? h(ActivityTimeline, { activity, finished }) : null : null,
    finished ? h('div', { style: { ...muted, marginTop: 8 } },
      data.verification_state === 'PENDING' ? 'Weryfikacja w toku…'
        : data.verification_state === 'NOT_RUN' ? 'Weryfikator: nie uruchamiano'
          : `Weryfikator: ${data.verification_state}`) : null,
  );
}

export default {
  id: 'pi-manager',
  name: 'Pi · przebieg pracy',
  description: 'Status Pi i narastający dziennik komend, komunikatów i wyników narzędzi.',
  register(ctx) {
    ctx.register({ id: 'live', area: TRANSCRIPT_DIRECTIVE_AREA, data: {
      name: 'pi-live',
      render: ({ attrs }) => h(PiCard, { ctx, taskId: attrs.task }),
    } });
  },
};
