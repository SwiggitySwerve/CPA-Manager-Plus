import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it, vi } from 'vitest';
import type { UsageServiceStatus } from '@/services/api/usageService';
import { CollectorStatusCard } from './CollectorStatusCard';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key, i18n: { language: 'en-US' } }),
}));

const renderStatus = (status: UsageServiceStatus | null, loading = false) =>
  renderToStaticMarkup(
    <CollectorStatusCard
      enabled
      serviceBase="http://manager.test"
      status={status}
      loading={loading}
      error=""
    />
  );

describe('CollectorStatusCard multi-instance status', () => {
  it('shows an extra collector failure alongside a healthy primary', () => {
    const primary = { upstream: 'http://codex.test:8317', mode: 'http', totalInserted: 123 };
    const html = renderStatus({
      events: 456,
      deadLetters: 0,
      collector: primary,
      collectors: [
        primary,
        {
          upstream: 'http://claude.test:8319',
          mode: 'http',
          lastError: 'Claude collector unreachable',
        },
      ],
    });
    expect(html).toContain('http://codex.test:8317');
    expect(html).toContain('http://claude.test:8319');
    expect(html).toContain('Claude collector unreachable');
    expect(html).toContain('dashboard.health_status_warning');
    expect(html).toContain('dashboard.health_status_normal');
    expect(html.match(/dashboard.collector_events/g)).toHaveLength(1);
    expect(html.match(/dashboard.collector_dead_letters/g)).toHaveLength(1);
  });

  it('does not treat an idle extra collector as a failure', () => {
    const html = renderStatus({
      collectors: [
        { upstream: 'http://codex.test', lastConsumedAt: 0, lastInsertedAt: 0 },
        { upstream: 'http://claude.test', lastConsumedAt: 0, lastInsertedAt: 0 },
      ],
    });
    expect(html.match(/dashboard.health_status_normal/g)).toHaveLength(2);
    expect(html).not.toContain('dashboard.health_status_warning');
  });

  it('preserves the legacy single-collector response', () => {
    const html = renderStatus({
      collector: { mode: 'http', lastError: 'Legacy collector failed' },
    });
    expect(html).toContain('Legacy collector failed');
    expect(html.match(/dashboard.health_queue_status/g)).toHaveLength(1);
  });

  it('does not report a stopped collector as healthy', () => {
    const html = renderStatus({ collector: { collector: 'stopped', mode: 'http' } });
    expect(html).toContain('dashboard.health_status_disabled');
    expect(html).not.toContain('dashboard.health_status_normal');
  });

  it('does not claim health before the first status response', () => {
    const html = renderStatus(null, true);
    expect(html).toContain('...');
    expect(html).not.toContain('dashboard.health_status_normal');
  });
});
