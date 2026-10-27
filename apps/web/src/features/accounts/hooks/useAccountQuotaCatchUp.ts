import { useEffect, useRef } from 'react';
import { isQuotaRefreshSupportedProvider } from '@/features/authFiles/constants';
import type { AccountRow } from '@/features/accounts/model/accountRows';
import { getQuotaCredentialStoreKey } from '@/utils/quota/credentialScope';

export const getAccountQuotaRefreshKey = (row: AccountRow): string =>
  `${row.provider}:${getQuotaCredentialStoreKey(row.raw)}`;

interface AccountQuotaCatchUpOptions {
  enabled: boolean;
  connectionFingerprint: string;
  active: boolean;
  quotaAutoRefreshMs: number;
  quotaRefreshing: boolean;
  disableControls: boolean;
  pageRows: AccountRow[];
  refreshQuotaRows: (rows: AccountRow[], options: { silent: boolean }) => Promise<void>;
}

export function useAccountQuotaCatchUp({
  enabled,
  connectionFingerprint,
  active,
  quotaAutoRefreshMs,
  quotaRefreshing,
  disableControls,
  pageRows,
  refreshQuotaRows,
}: AccountQuotaCatchUpOptions) {
  const attemptsRef = useRef({ connectionFingerprint, keys: new Set<string>() });

  useEffect(() => {
    if (attemptsRef.current.connectionFingerprint !== connectionFingerprint || !active) {
      attemptsRef.current = { connectionFingerprint, keys: new Set() };
    }
    if (!enabled || !active || quotaAutoRefreshMs <= 0 || quotaRefreshing || disableControls) {
      return;
    }

    const now = Date.now();
    const targets = pageRows.filter((row) => {
      if (
        row.runtimeOnly ||
        !isQuotaRefreshSupportedProvider(row.provider) ||
        !(row.quota.fetchedAtMs == null || now - row.quota.fetchedAtMs >= quotaAutoRefreshMs)
      ) {
        return false;
      }
      const key = getAccountQuotaRefreshKey(row);
      if (attemptsRef.current.keys.has(key)) return false;
      attemptsRef.current.keys.add(key);
      return true;
    });
    if (targets.length > 0) void refreshQuotaRows(targets, { silent: true });
  }, [
    enabled,
    connectionFingerprint,
    active,
    quotaAutoRefreshMs,
    quotaRefreshing,
    disableControls,
    pageRows,
    refreshQuotaRows,
  ]);
}
