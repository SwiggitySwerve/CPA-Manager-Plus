import { useEffect, useRef } from 'react';
import { usageServiceApi } from '@/services/api/usageService';

export const INVENTORY_POLL_INTERVAL_MS = 5_000;

interface InventoryRevalidationOptions {
  managerBase: string;
  managementKey: string;
  enabled?: boolean;
  /** Runs when the CLI-Proxy credential inventory changed. Revalidate in the background. */
  onChange: () => void | Promise<void>;
}

/**
 * Polls the manager's credential inventory version while the tab is visible and
 * calls onChange when it moves, so a screen listing accounts or credentials
 * follows an add or remove made elsewhere within one poll. The first read is the
 * baseline and never triggers onChange; a failed poll keeps the baseline.
 */
export function useInventoryRevalidation({
  managerBase,
  managementKey,
  enabled = true,
  onChange,
}: InventoryRevalidationOptions) {
  const onChangeRef = useRef(onChange);
  useEffect(() => {
    onChangeRef.current = onChange;
  }, [onChange]);

  useEffect(() => {
    if (!enabled || !managerBase || !managementKey) return;
    let cancelled = false;
    let inFlight = false;
    let baseline: string | null = null;

    const hidden = () => typeof document !== 'undefined' && document.visibilityState === 'hidden';
    const poll = async () => {
      if (inFlight || hidden()) return;
      inFlight = true;
      try {
        const snapshot = await usageServiceApi.getCredentialInventory(managerBase, managementKey);
        if (cancelled || !snapshot.version) return;
        const previous = baseline;
        baseline = snapshot.version;
        if (previous !== null && previous !== snapshot.version) await onChangeRef.current();
      } catch {
        // Keep the baseline; the next poll retries.
      } finally {
        inFlight = false;
      }
    };

    void poll();
    const timer = setInterval(() => void poll(), INVENTORY_POLL_INTERVAL_MS);
    const onVisible = () => void poll();
    if (typeof document !== 'undefined') document.addEventListener('visibilitychange', onVisible);
    return () => {
      cancelled = true;
      clearInterval(timer);
      if (typeof document !== 'undefined') {
        document.removeEventListener('visibilitychange', onVisible);
      }
    };
  }, [enabled, managerBase, managementKey]);
}
