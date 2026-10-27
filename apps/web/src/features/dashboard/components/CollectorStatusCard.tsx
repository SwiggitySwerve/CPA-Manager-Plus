import { useTranslation } from 'react-i18next';
import type { UsageServiceCollectorStatus, UsageServiceStatus } from '@/services/api/usageService';
import styles from './CollectorStatusCard.module.scss';

interface CollectorStatusCardProps {
  enabled: boolean;
  serviceBase: string;
  managementKey?: string;
  refreshSignal?: number;
  status: UsageServiceStatus | null;
  loading: boolean;
  error: string;
}

const formatCount = (value: number | undefined) =>
  Number.isFinite(value) ? Number(value).toLocaleString() : '-';

const formatTimestamp = (value: number | undefined, locale: string) => {
  if (!value || !Number.isFinite(value)) return '-';
  return new Date(value).toLocaleTimeString(locale, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
};

function CollectorRow({
  label,
  value,
  loading,
  tone,
}: {
  label: string;
  value: string;
  loading: boolean;
  tone?: 'normal' | 'warning';
}) {
  return (
    <div className={styles.statusItem}>
      <span className={styles.label}>{label}</span>
      <span
        className={`${styles.value} ${tone ? `${styles.statusText} ${tone === 'warning' ? styles.statusWarn : styles.statusOk}` : ''}`}
      >
        {loading ? '...' : value}
        {tone && <span className={styles.statusDot} />}
      </span>
    </div>
  );
}

function CollectorDetails({
  enabled,
  status,
  loading,
  error,
  collector,
  multiple,
}: Pick<CollectorStatusCardProps, 'enabled' | 'status' | 'loading' | 'error'> & {
  collector?: UsageServiceCollectorStatus;
  multiple: boolean;
}) {
  const { t, i18n } = useTranslation();
  const lastError = collector?.lastError || error;
  const disabled = !enabled || collector?.collector === 'stopped';
  const queueStatus = disabled
    ? t('dashboard.health_status_disabled')
    : error
      ? t('dashboard.collector_unavailable')
      : lastError
        ? t('dashboard.health_status_warning')
        : status
          ? t('dashboard.health_status_normal')
          : '-';

  const rows = [
    { label: t('dashboard.collector_mode'), value: collector?.mode || collector?.collector || '-' },
    { label: t('dashboard.health_queue_status'), value: queueStatus, isStatus: true },
    {
      label: t('dashboard.collector_last_consumed'),
      value: formatTimestamp(collector?.lastConsumedAt, i18n.language),
    },
    {
      label: t('dashboard.collector_last_inserted'),
      value: formatTimestamp(collector?.lastInsertedAt, i18n.language),
    },
    {
      label: t('dashboard.collector_total_inserted'),
      value: formatCount(collector?.totalInserted),
    },
    { label: t('dashboard.collector_total_skipped'), value: formatCount(collector?.totalSkipped) },
  ];

  return (
    <div className={styles.collectorGroup}>
      {multiple && <h4 className={styles.upstream}>{collector?.upstream || '-'}</h4>}
      <div className={styles.statusList}>
        {rows.map((row) => (
          <CollectorRow
            key={row.label}
            label={row.label}
            value={row.value}
            loading={loading && !status}
            tone={
              row.isStatus ? (disabled || error || lastError ? 'warning' : 'normal') : undefined
            }
          />
        ))}
      </div>
      {lastError ? (
        <div className={styles.errorLine}>
          <span>{t('dashboard.collector_last_error')}</span>
          <strong>{lastError}</strong>
        </div>
      ) : null}
    </div>
  );
}

export function CollectorStatusCard(props: CollectorStatusCardProps) {
  const { t } = useTranslation();
  const { status, loading } = props;
  const collectors = status?.collectors?.length ? status.collectors : [status?.collector];
  const totals = [
    { label: t('dashboard.collector_events'), value: status?.events },
    {
      label: t('dashboard.collector_dead_letters'),
      value: status?.deadLetters ?? status?.collector?.deadLetters,
    },
  ];

  return (
    <section className={styles.dataCard}>
      <div className={styles.cardHeader}>
        <h3>{t('dashboard.collector_status_title')}</h3>
      </div>
      <div className={styles.statusList}>
        {totals.map((row) => (
          <CollectorRow
            key={row.label}
            label={row.label}
            value={formatCount(row.value)}
            loading={loading && !status}
          />
        ))}
      </div>
      {collectors.map((collector) => (
        <CollectorDetails
          key={collector?.upstream || 'primary'}
          {...props}
          collector={collector}
          multiple={collectors.length > 1}
        />
      ))}
    </section>
  );
}
