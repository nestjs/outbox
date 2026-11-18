import type { StoreMigration } from '@nestjs/store-kit/mysql';

/**
 * Messages by topic, as dead letters are: what an app's own view of its backlog per topic (how many wait, which to
 * settle) reads, without scanning every message.
 */
export const topicIndexMigration: StoreMigration = {
  version: 2,
  name: 'topic_index',
  up: (t) => [`CREATE INDEX messages_topic_seq ON ${t('messages')} (topic, seq)`],
};
