import { useTranslation } from 'react-i18next';
import styles from '@/features/accounts/AccountsPage.module.scss';

interface AccountStatusMessageProps {
  message: string;
  variant?: 'compact' | 'detail';
}

export function AccountStatusMessage({ message, variant = 'compact' }: AccountStatusMessageProps) {
  const { t } = useTranslation();
  if (!message) return null;
  const compact = variant === 'compact';

  return (
    <div
      className={compact ? styles.accountGridCardRecentStatusMessage : styles.overviewRecentStatusMessage}
      data-overview-recent-status-message="true"
      title={compact ? message : undefined}
    >
      <span className={compact ? styles.accountGridCardRecentStatusMessageLabel : undefined}>
        {t('accounts.detail_overview_recent_status_message')}
      </span>
      <p className={compact ? styles.accountGridCardRecentStatusMessageText : undefined}>
        {message}
      </p>
    </div>
  );
}
