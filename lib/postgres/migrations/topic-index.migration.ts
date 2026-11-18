import type { StoreMigration } from '@nestjs/store-kit/postgres';

/**
 * Messages by topic, as dead letters are: what an app's own view of its backlog per topic (how many wait, which to
 * settle) reads, without scanning every message.
 */
export const topicIndexMigration: StoreMigration = {
  version: 2,
  name: 'topic_index',
  up: (s) => [`CREATE INDEX messages_topic_seq ON ${s}.messages (topic, seq)`],
};
