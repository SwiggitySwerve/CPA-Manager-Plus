import { useTranslation } from 'react-i18next';
import {
  formatTimestamp,
  formatTimestampTitle,
} from '@/features/accounts/model/accountsPagePresentation';
import styles from '@/features/accounts/AccountsPage.module.scss';

interface AccountStatusMessageProps {
  message: string;
  variant?: 'compact' | 'detail';
  retryAtMs?: number | null;
  quotaError?: string | null;
  className?: string;
}

export function AccountStatusMessage({
  message,
  variant = 'compact',
  retryAtMs,
  quotaError = null,
  className,
}: AccountStatusMessageProps) {
  const { t, i18n } = useTranslation();
  if (!message && quotaError === null) return null;
  const compact = variant === 'compact';

  return (
    <div
      className={[
        compact ? styles.accountGridCardRecentStatusMessage : styles.overviewRecentStatusMessage,
        className,
      ]
        .filter(Boolean)
        .join(' ')}
      data-overview-recent-status-message="true"
      title={compact ? message : undefined}
    >
      <span className={compact ? styles.accountGridCardRecentStatusMessageLabel : undefined}>
        {t('accounts.detail_overview_recent_status_message')}
      </span>
      {message ? (
        <p
          className={compact ? styles.accountGridCardRecentStatusMessageText : undefined}
          data-account-provider-status-message="true"
        >
          {message}
        </p>
      ) : null}
      {retryAtMs ? (
        <time
          dateTime={new Date(retryAtMs).toISOString()}
          title={formatTimestampTitle(retryAtMs, i18n.language)}
        >
          {t('accounts.detail_overview_retry_at', {
            time: formatTimestamp(retryAtMs, i18n.language, true),
          })}
        </time>
      ) : null}
      {quotaError !== null ? (
        <p
          className={compact ? styles.accountGridCardRecentStatusMessageText : undefined}
          data-account-quota-refresh-error="true"
        >
          {quotaError
            ? t('accounts.quota_subtext_error', { message: quotaError })
            : t('accounts.quota_subtext_error_unknown')}
        </p>
      ) : null}
    </div>
  );
}
