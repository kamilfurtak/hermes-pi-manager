import { createElement as h, useEffect, useState, useSyncExternalStore } from 'react';
import { host, TRANSCRIPT_DIRECTIVE_AREA } from '@hermes/plugin-sdk';

const FINAL = new Set(['SETTLED', 'ABORTED', 'CRASHED']);
const LABELS = {
  STARTING: 'Uruchamianie', RUNNING: 'Pi pracuje', TOOL_RUNNING: 'Narzędzie pracuje',
  WAITING: 'Oczekiwanie', STALLED: 'Brak postępu', UNRESPONSIVE: 'Brak odpowiedzi',
  SETTLED: 'Pi zakończył pracę', ABORTED: 'Przerwano', CRASHED: 'Błąd procesu',
};
const box = { border: '1px solid var(--ui-border, var(--border, #666))', borderRadius: 10, padding: '12px 14px', margin: '8px 0', minWidth: 0 };
const muted = { opacity: 0.65, fontSize: 12 };
const pre = { whiteSpace: 'pre-wrap', overflowWrap: 'anywhere', fontSize: 12, margin: '8px 0', maxHeight: 260, overflowY: 'auto' };

function useAtom(atom) {
  return useSyncExternalStore(
    callback => atom?.listen?.(callback) || (() => {}),
    () => atom?.get?.() ?? null,
  );
}

/** Cancel scheduling and discard in-flight replies on navigation/unload. */
export function pollActivity({ rest, taskId, sessionId, onData, onError, interval = 1000 }) {
  let stopped = false;
  let timer;
  let terminalSnapshot;
  async function tick() {
    let delay = interval;
    let done = false;
    try {
      const data = await rest(`/activity?task_id=${encodeURIComponent(taskId)}&session_id=${encodeURIComponent(sessionId)}`, { timeoutMs: 5000 });
      if (stopped) return;
      if (data?.task_id !== taskId || data?.session_id !== sessionId) throw new Error('Odpowiedź pochodzi z innego zadania.');
      onData(data);
      // The task row can settle just before the coalescing writer flushes.
      // Observe two identical terminal snapshots before stopping the poll.
      const terminal = FINAL.has(data.execution_state) && data.verification_state !== 'PENDING';
      const signature = JSON.stringify([data.execution_state, data.verification_state, data.activity?.seq]);
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
      rest: ctx.rest, taskId, sessionId,
      onData: data => setView({ scope, data, error: null }),
      onError: error => setView(previous => ({ scope, data: previous?.scope === scope ? previous.data : null, error })),
    });
    return stop;
  }, [ctx, scope, valid, paused]);

  if (!valid) return h('p', { style: muted }, 'Nieprawidłowy identyfikator zadania Pi.');
  const data = current?.data;
  const activity = data?.activity;
  const tool = activity?.tool;
  const finished = data && FINAL.has(data.execution_state);
  const status = paused ? 'Podgląd wstrzymany — otwórz połączenie tej rozmowy'
    : (LABELS[data?.execution_state] || 'Łączenie z zadaniem Pi…');
  const text = tool?.text || activity?.text || '';
  const updated = activity?.updated_at ? new Date(activity.updated_at * 1000).toLocaleTimeString() : null;

  return h('section', { style: box, 'aria-label': `Praca Pi ${taskId}` },
    h('div', { style: { display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', gap: 12 } },
      h('strong', null, `Pi · ${status}`),
      h('button', { type: 'button', onClick: () => setExpanded(!expanded), 'aria-expanded': expanded,
        style: { cursor: 'pointer', fontSize: 12, flexShrink: 0 } }, expanded ? 'Zwiń' : 'Pokaż przebieg')),
    h('div', { style: muted }, taskId,
      activity ? ` · ukończone wywołania narzędzi: ${activity.tools_completed}` : '',
      updated ? ` · ostatnia aktywność ${updated}` : ''),
    data?.active_tool ? h('div', { style: { fontSize: 13, marginTop: 6 } }, `Narzędzie: ${data.active_tool}`) : null,
    current?.error ? h('p', { role: 'status', style: muted }, current.error) : null,
    !paused && data && !activity ? h('p', { style: muted }, 'Brak zapisu strumienia dla tego zadania. Nowy worker utworzy podgląd po załadowaniu dodatku przez backend.') : null,
    !expanded && text ? h('p', { style: { fontSize: 13, whiteSpace: 'pre-wrap', overflowWrap: 'anywhere', marginTop: 8 } }, text.slice(-240)) : null,
    expanded && activity ? h('div', { style: { marginTop: 10 } },
      h('div', { style: muted }, 'Ostatnie fragmenty; długi wynik jest skracany.'),
      ...activity.entries.map((entry, index) => h('div', { key: `${index}:${entry.id || entry.kind}`, style: { borderTop: box.border, marginTop: 8 } },
        h('div', { style: muted }, entry.kind === 'tool' ? `${entry.name}${entry.error ? ' · błąd' : ' · zakończone'}` : 'Pi'),
        h('pre', { style: pre }, entry.text))),
      activity.text && activity.entries.at(-1)?.text !== activity.text
        ? h('pre', { style: pre }, activity.text) : null,
      tool ? h('div', null, h('div', { style: muted }, `${tool.name} · na żywo`), h('pre', { style: pre }, tool.text || 'Narzędzie jeszcze nie wysłało wyjścia.')) : null,
    ) : null,
    finished ? h('div', { style: { ...muted, marginTop: 8 } },
      data.verification_state === 'PENDING' ? 'Weryfikacja w toku…'
        : data.verification_state === 'NOT_RUN' ? 'Weryfikator: nie uruchamiano'
          : `Weryfikator: ${data.verification_state}`) : null,
  );
}

export default {
  id: 'pi-manager',
  name: 'Pi · przebieg pracy',
  description: 'Bieżący tekst i wyjście narzędzi Pi w karcie rozmowy.',
  register(ctx) {
    ctx.register({ id: 'live', area: TRANSCRIPT_DIRECTIVE_AREA, data: {
      name: 'pi-live',
      render: ({ attrs }) => h(PiCard, { ctx, taskId: attrs.task }),
    } });
  },
};
