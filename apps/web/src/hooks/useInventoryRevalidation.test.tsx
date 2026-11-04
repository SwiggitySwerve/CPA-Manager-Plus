import { act, create, type ReactTestRenderer } from 'react-test-renderer';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { INVENTORY_POLL_INTERVAL_MS, useInventoryRevalidation } from './useInventoryRevalidation';

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const mocks = vi.hoisted(() => ({ getCredentialInventory: vi.fn() }));

vi.mock('@/services/api/usageService', () => ({
  usageServiceApi: { getCredentialInventory: mocks.getCredentialInventory },
}));

const documentStub = {
  visibilityState: 'visible' as 'visible' | 'hidden',
  listeners: new Map<string, () => void>(),
  addEventListener(type: string, listener: () => void) {
    this.listeners.set(type, listener);
  },
  removeEventListener(type: string) {
    this.listeners.delete(type);
  },
};

function Probe({
  onChange,
  enabled = true,
  managerBase = 'http://manager.local:18317',
}: {
  onChange: () => void;
  enabled?: boolean;
  managerBase?: string;
}) {
  useInventoryRevalidation({ managerBase, managementKey: 'admin-key', enabled, onChange });
  return null;
}

let renderer: ReactTestRenderer | null = null;

const settle = async (ms = 0) => {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });
};

const mount = async (props: Parameters<typeof Probe>[0]) => {
  await act(async () => {
    renderer = create(<Probe {...props} />);
  });
  await settle();
};

beforeEach(() => {
  vi.useFakeTimers();
  mocks.getCredentialInventory.mockReset();
  documentStub.visibilityState = 'visible';
  documentStub.listeners.clear();
  vi.stubGlobal('document', documentStub);
});

afterEach(() => {
  act(() => renderer?.unmount());
  renderer = null;
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('useInventoryRevalidation', () => {
  it('treats the first read as a baseline and revalidates only when the version moves', async () => {
    mocks.getCredentialInventory.mockResolvedValue({ version: 'v1', syncedAtMs: 1, count: 2 });
    const onChange = vi.fn();
    await mount({ onChange });
    expect(mocks.getCredentialInventory).toHaveBeenCalledWith(
      'http://manager.local:18317',
      'admin-key'
    );
    expect(onChange).not.toHaveBeenCalled();

    await settle(INVENTORY_POLL_INTERVAL_MS);
    expect(onChange).not.toHaveBeenCalled();

    mocks.getCredentialInventory.mockResolvedValue({ version: 'v2', syncedAtMs: 2, count: 1 });
    await settle(INVENTORY_POLL_INTERVAL_MS);
    expect(onChange).toHaveBeenCalledTimes(1);

    await settle(INVENTORY_POLL_INTERVAL_MS);
    expect(onChange).toHaveBeenCalledTimes(1);
  });

  it('keeps its baseline across a failed poll and still sees a later change', async () => {
    mocks.getCredentialInventory.mockResolvedValue({ version: 'v1', syncedAtMs: 1, count: 2 });
    const onChange = vi.fn();
    await mount({ onChange });

    mocks.getCredentialInventory.mockRejectedValue(new Error('503'));
    await settle(INVENTORY_POLL_INTERVAL_MS);
    expect(onChange).not.toHaveBeenCalled();

    mocks.getCredentialInventory.mockResolvedValue({ version: 'v1', syncedAtMs: 3, count: 2 });
    await settle(INVENTORY_POLL_INTERVAL_MS);
    expect(onChange).not.toHaveBeenCalled();

    mocks.getCredentialInventory.mockResolvedValue({ version: 'v2', syncedAtMs: 4, count: 3 });
    await settle(INVENTORY_POLL_INTERVAL_MS);
    expect(onChange).toHaveBeenCalledTimes(1);
  });

  it('does not poll while the tab is hidden and catches up when it is visible again', async () => {
    mocks.getCredentialInventory.mockResolvedValue({ version: 'v1', syncedAtMs: 1, count: 2 });
    const onChange = vi.fn();
    await mount({ onChange });
    expect(mocks.getCredentialInventory).toHaveBeenCalledTimes(1);

    documentStub.visibilityState = 'hidden';
    mocks.getCredentialInventory.mockResolvedValue({ version: 'v2', syncedAtMs: 2, count: 1 });
    await settle(INVENTORY_POLL_INTERVAL_MS * 3);
    expect(mocks.getCredentialInventory).toHaveBeenCalledTimes(1);
    expect(onChange).not.toHaveBeenCalled();

    documentStub.visibilityState = 'visible';
    await act(async () => {
      documentStub.listeners.get('visibilitychange')?.();
    });
    await settle();
    expect(onChange).toHaveBeenCalledTimes(1);
  });

  it('does nothing while disabled or without a manager service', async () => {
    mocks.getCredentialInventory.mockResolvedValue({ version: 'v1', syncedAtMs: 1, count: 2 });
    const onChange = vi.fn();
    await mount({ onChange, enabled: false });
    await settle(INVENTORY_POLL_INTERVAL_MS * 2);
    expect(mocks.getCredentialInventory).not.toHaveBeenCalled();

    act(() => renderer?.unmount());
    await mount({ onChange, managerBase: '' });
    await settle(INVENTORY_POLL_INTERVAL_MS * 2);
    expect(mocks.getCredentialInventory).not.toHaveBeenCalled();
  });
});
