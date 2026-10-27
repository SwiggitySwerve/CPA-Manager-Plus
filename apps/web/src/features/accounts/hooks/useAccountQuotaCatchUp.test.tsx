import { StrictMode } from 'react';
import { act, create, type ReactTestRenderer } from 'react-test-renderer';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { buildAccountRows, type AccountRow } from '@/features/accounts/model/accountRows';
import { useAccountQuotaCatchUp } from './useAccountQuotaCatchUp';

const NOW = 1_800_000_000_000;
const INTERVAL = 5 * 60_000;
type Options = Parameters<typeof useAccountQuotaCatchUp>[0];

const makeRow = (provider: string, authIndex: string, name = `${provider}.json`): AccountRow =>
  buildAccountRows([{ name, type: provider, auth_index: authIndex }], {
    antigravityQuota: {},
    claudeQuota: {},
    codexQuota: {},
    devinQuota: {},
    kimiQuota: {},
    metaQuota: {},
    xaiQuota: {},
  })[0];

const withFetchedAt = (row: AccountRow, fetchedAtMs: number): AccountRow => ({
  ...row,
  quota: { ...row.quota, fetchedAtMs },
});

function Harness(options: Options) {
  useAccountQuotaCatchUp(options);
  return null;
}

const renderers: ReactTestRenderer[] = [];
const mount = async (overrides: Partial<Options> = {}) => {
  const refresh = vi.fn<Options['refreshQuotaRows']>().mockResolvedValue(undefined);
  let options: Options = {
    enabled: true,
    connectionFingerprint: 'connection-a',
    active: true,
    quotaAutoRefreshMs: INTERVAL,
    quotaRefreshing: false,
    disableControls: false,
    pageRows: [makeRow('codex', 'first')],
    refreshQuotaRows: refresh,
    ...overrides,
  };
  let renderer!: ReactTestRenderer;
  const element = () => (
    <StrictMode>
      <Harness {...options} />
    </StrictMode>
  );
  await act(async () => {
    renderer = create(element());
  });
  renderers.push(renderer);
  return {
    refresh,
    update: async (next: Partial<Options>) => {
      options = { ...options, ...next };
      await act(async () => {
        renderer.update(element());
      });
    },
  };
};

beforeEach(() => {
  vi.spyOn(Date, 'now').mockReturnValue(NOW);
});

afterEach(async () => {
  await act(async () => {
    renderers.splice(0).forEach((renderer) => renderer.unmount());
  });
  vi.restoreAllMocks();
});

describe('useAccountQuotaCatchUp', () => {
  it('catches up each newly visited provider and pagination page once', async () => {
    const codexPageOne = makeRow('codex', 'codex-1');
    const codexPageTwo = makeRow('codex', 'codex-2');
    const claudePageOne = makeRow('claude', 'claude-1');
    const claudePageTwo = makeRow('claude', 'claude-2');
    const { refresh, update } = await mount({ pageRows: [codexPageOne] });

    await update({ pageRows: [claudePageOne] });
    await update({ pageRows: [claudePageTwo] });
    await update({ pageRows: [codexPageTwo] });
    await update({ pageRows: [codexPageOne] });
    await update({ pageRows: [claudePageOne, claudePageTwo] });

    expect(refresh.mock.calls).toEqual(
      [codexPageOne, claudePageOne, claudePageTwo, codexPageTwo].map((row) => [
        [row],
        { silent: true },
      ])
    );
  });

  it('waits for initially empty rows to load', async () => {
    const { refresh, update } = await mount({ pageRows: [] });
    expect(refresh).not.toHaveBeenCalled();
    const row = makeRow('claude', 'loaded');
    await update({ pageRows: [row] });
    expect(refresh).toHaveBeenCalledExactlyOnceWith([row], { silent: true });
  });

  it('uses the existing missing/stale threshold and excludes unsupported or runtime rows', async () => {
    const missing = makeRow('codex', 'missing');
    const stale = withFetchedAt(makeRow('claude', 'stale'), NOW - INTERVAL);
    const fresh = withFetchedAt(makeRow('claude', 'fresh'), NOW - INTERVAL + 1);
    const unsupported = makeRow('openai', 'unsupported');
    const runtime = { ...makeRow('codex', 'runtime'), runtimeOnly: true };
    const { refresh } = await mount({ pageRows: [missing, stale, fresh, unsupported, runtime] });

    expect(refresh).toHaveBeenCalledExactlyOnceWith([missing, stale], { silent: true });
  });

  it('does not consume a fresh credential attempt before it becomes stale', async () => {
    const row = withFetchedAt(makeRow('claude', 'fresh'), NOW);
    const { refresh, update } = await mount({ pageRows: [row] });
    expect(refresh).not.toHaveBeenCalled();

    vi.mocked(Date.now).mockReturnValue(NOW + INTERVAL);
    await update({ pageRows: [{ ...row }] });
    expect(refresh).toHaveBeenCalledExactlyOnceWith([row], { silent: true });
  });

  it('deduplicates credentials while allowing same-name siblings, providers, and replacements', async () => {
    const first = makeRow('codex', 'first', 'shared.json');
    const sibling = makeRow('codex', 'sibling', 'shared.json');
    const claude = makeRow('claude', 'first', 'shared.json');
    const replacement = makeRow('codex', 'replacement', 'shared.json');
    const { refresh, update } = await mount({ pageRows: [first, { ...first }, sibling, claude] });

    expect(refresh).toHaveBeenNthCalledWith(1, [first, sibling, claude], { silent: true });
    await update({ pageRows: [replacement, sibling, claude] });
    expect(refresh).toHaveBeenNthCalledWith(2, [replacement], { silent: true });
    expect(refresh).toHaveBeenCalledTimes(2);
  });

  it.each([500, 429])(
    'does not retry a %s error on store renders or page revisits',
    async (status) => {
      const row = makeRow('claude', 'failed');
      const { refresh, update } = await mount({ pageRows: [row] });
      const failed: AccountRow = {
        ...row,
        quota: { ...row.quota, status: 'error', error: 'quota unavailable', errorStatus: status },
      };

      await update({ quotaRefreshing: true });
      await update({ quotaRefreshing: false, pageRows: [failed] });
      const nextRefresh = vi.fn<Options['refreshQuotaRows']>().mockResolvedValue(undefined);
      await update({ pageRows: [{ ...failed }], refreshQuotaRows: nextRefresh });
      await update({ pageRows: [] });
      vi.mocked(Date.now).mockReturnValue(NOW + INTERVAL * 2);
      await update({ pageRows: [failed] });

      expect(refresh).toHaveBeenCalledTimes(1);
      expect(nextRefresh).not.toHaveBeenCalled();
    }
  );

  it('resets attempts on connection changes, including a return to a previous connection', async () => {
    const row = makeRow('codex', 'shared');
    const { refresh, update } = await mount({ pageRows: [row] });

    await update({ connectionFingerprint: 'connection-b', quotaRefreshing: true });
    expect(refresh).toHaveBeenCalledTimes(1);
    await update({ quotaRefreshing: false });
    expect(refresh).toHaveBeenCalledTimes(2);
    await update({ connectionFingerprint: 'connection-a' });
    expect(refresh).toHaveBeenCalledTimes(3);
    await update({ pageRows: [{ ...row }] });
    expect(refresh).toHaveBeenCalledTimes(3);
  });

  it('resets attempts after the document or accounts workspace becomes inactive', async () => {
    const row = makeRow('claude', 'visible');
    const { refresh, update } = await mount({ pageRows: [row] });

    await update({ active: false });
    expect(refresh).toHaveBeenCalledTimes(1);
    await update({ active: true });
    expect(refresh).toHaveBeenCalledTimes(2);
    await update({ active: false });
    await update({ active: true, pageRows: [withFetchedAt(row, NOW)] });
    expect(refresh).toHaveBeenCalledTimes(2);
  });

  it.each([
    { label: 'build flag', guard: { enabled: false }, resume: { enabled: true } },
    {
      label: 'auto-refresh opt-out',
      guard: { quotaAutoRefreshMs: 0 },
      resume: { quotaAutoRefreshMs: INTERVAL },
    },
    {
      label: 'disconnected controls',
      guard: { disableControls: true },
      resume: { disableControls: false },
    },
    { label: 'inactive view', guard: { active: false }, resume: { active: true } },
  ])('honors $label without consuming attempts', async ({ guard, resume }) => {
    const row = makeRow('codex', 'guarded');
    const { refresh, update } = await mount({ pageRows: [row], ...guard });
    await update({ pageRows: [{ ...row }] });
    expect(refresh).not.toHaveBeenCalled();

    await update(resume);
    expect(refresh).toHaveBeenCalledExactlyOnceWith([row], { silent: true });
  });

  it('waits for a running batch and then catches up the latest visible page', async () => {
    const first = makeRow('codex', 'first');
    const next = makeRow('claude', 'next');
    const { refresh, update } = await mount({ pageRows: [first], quotaRefreshing: true });
    await update({ pageRows: [next] });
    expect(refresh).not.toHaveBeenCalled();

    await update({ quotaRefreshing: false });
    expect(refresh).toHaveBeenNthCalledWith(1, [next], { silent: true });
    await update({ pageRows: [first], quotaRefreshing: true });
    expect(refresh).toHaveBeenCalledTimes(1);
    await update({ quotaRefreshing: false });
    expect(refresh).toHaveBeenNthCalledWith(2, [first], { silent: true });
    expect(refresh).toHaveBeenCalledTimes(2);
  });
});
