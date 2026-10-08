<p align="center">
  <a href="http://nestjs.com/" target="blank"><img src="https://nestjs.com/img/logo-small.svg" width="120" alt="Nest Logo" /></a>
</p>

[travis-image]: https://api.travis-ci.org/nestjs/nest.svg?branch=master
[travis-url]: https://travis-ci.org/nestjs/nest
[linux-image]: https://img.shields.io/travis/nestjs/nest/master.svg?label=linux
[linux-url]: https://travis-ci.org/nestjs/nest

  <p align="center">A progressive <a href="http://nodejs.org" target="blank">Node.js</a> framework for building efficient and scalable server-side applications.</p>
    <p align="center">
<a href="https://www.npmjs.com/~nestjscore"><img src="https://img.shields.io/npm/v/@nestjs/core.svg" alt="NPM Version" /></a>
<a href="https://www.npmjs.com/~nestjscore"><img src="https://img.shields.io/npm/l/@nestjs/core.svg" alt="Package License" /></a>
<a href="https://www.npmjs.com/~nestjscore"><img src="https://img.shields.io/npm/dm/@nestjs/core.svg" alt="NPM Downloads" /></a>
<a href="https://discord.gg/G7Qnnhy" target="_blank"><img src="https://img.shields.io/badge/discord-online-brightgreen.svg" alt="Discord"/></a>
<a href="https://opencollective.com/nest#backer"><img src="https://opencollective.com/nest/backers/badge.svg" alt="Backers on Open Collective" /></a>
<a href="https://opencollective.com/nest#sponsor"><img src="https://opencollective.com/nest/sponsors/badge.svg" alt="Sponsors on Open Collective" /></a>
  <a href="https://paypal.me/kamilmysliwiec"><img src="https://img.shields.io/badge/Donate-PayPal-dc3d53.svg"/></a>
  <a href="https://twitter.com/nestframework"><img src="https://img.shields.io/twitter/follow/nestframework.svg?style=social&label=Follow"></a>
</p>
  <!--[![Backers on Open Collective](https://opencollective.com/nest/backers/badge.svg)](https://opencollective.com/nest#backer)
  [![Sponsors on Open Collective](https://opencollective.com/nest/sponsors/badge.svg)](https://opencollective.com/nest#sponsor)-->

## Description

Transactional outbox module for [Nest](https://github.com/nestjs/nest): messages written in the same database transaction as your business data, a relay that publishes them after commit with leases, retries and a dead-letter table, and an inbox that deduplicates redeliveries, with no third-party dependencies.

## Installation

```bash
$ npm i --save @nestjs/outbox
```

## Quick Start

[Overview & Tutorial](https://docs.nestjs.com/reliability/outbox)

## PostgreSQL store

`@nestjs/outbox/postgres` ships `PostgresOutboxStore`, which keeps the messages, the dead letters and the consumers' inbox in a schema of its own (`nest_outbox`) and writes through the client your application already uses: `fromPg(pool)`, `fromDrizzle(db)`, `fromTypeOrm(dataSource)`, `fromPrisma(prisma)` or `fromKysely(db)`. Register it with a factory provider:

```ts
import { OutboxStorage } from '@nestjs/outbox';
import { fromDrizzle, PostgresOutboxStore } from '@nestjs/outbox/postgres';

@Module({
  imports: [DrizzleModule.forRoot({ drizzle, connection: process.env.DATABASE_URL! }), OutboxModule.forRoot()],
  providers: [
    {
      provide: PostgresOutboxStore,
      inject: [getDrizzleToken(), OutboxStorage],
      useFactory: (db: Database, storage: OutboxStorage) => new PostgresOutboxStore({ executor: fromDrizzle(db) }, storage),
    },
  ],
})
export class AppModule {}
```

`outbox.add(tx, message)` then takes your ORM's own transaction object (Drizzle's `tx`, a TypeORM `EntityManager`, a Prisma transaction client, a Kysely `Transaction`, a `pg` client after `BEGIN`). The store applies its migrations at startup, except when `NODE_ENV` is `production`; there, apply them on deploy with `npx nest-outbox migrate --url <database url>` (`status` checks, `sql` prints them for your own migration tool).

## MySQL store

`@nestjs/outbox/mysql` ships `MySqlOutboxStore` (MySQL 8.4 LTS and 9.x), registered the same way: import it and the executor from `@nestjs/outbox/mysql` instead (`fromMysql2(pool)`, `fromDrizzle(db)`, `fromTypeOrm(dataSource)`, `fromPrisma(prisma)` with `@prisma/adapter-mariadb`, or `fromKysely(db)`). Its tables live in your connection's database, named after the `schema` option (`nest_outbox_messages`, `nest_outbox_dead_letters`, `nest_outbox_inbox`); ids, topics, keys and consumer names are compared byte for byte and hold at most 255 characters. `npx nest-outbox migrate --url mysql://...` applies its migrations, and `sql --dialect mysql` prints them (`MySqlOutboxStore.migrationStatements()` lists them one per string, for TypeORM's `queryRunner.query()`).

## Retention

The consumers' inbox records (one per consumer per message) and the dead letters are kept until your application deletes them: nothing in the module does it for you. On the PostgreSQL and MySQL stores the tables grow without limit; on the default in-memory store they grow on the heap (about 470 bytes per inbox record, so 200k records take roughly 94 MB, and each dead letter keeps its full payload and attempt history), which adds up in a long-running dev server or with `allowInMemoryStorage: true`. Schedule `OutboxInbox.prune(olderThan)`, and purge the dead letters you've handled (requeued or given up on) with `OutboxDeadLetters.purge(target)`, e.g. with `@nestjs/schedule`:

```ts
import { Cron, CronExpression } from '@nestjs/schedule';
import { OutboxDeadLetters, OutboxInbox } from '@nestjs/outbox';

@Injectable()
export class OutboxRetention {
  constructor(private readonly inbox: OutboxInbox, private readonly deadLetters: OutboxDeadLetters) {}

  @Cron(CronExpression.EVERY_DAY_AT_3AM)
  async prune() {
    await this.deadLetters.purge({ failedBefore: new Date(Date.now() - 14 * 86_400_000) });
    await this.inbox.prune('30d');
  }
}
```

The inbox record is what recognizes a redelivery, so keep it longer than a message can take to come back: the whole retry schedule, your broker's redelivery window for remote consumers, and, above all, the longest a dead letter may wait before someone requeues it, since a requeued message keeps its id. Purging dead letters after 14 days bounds that wait, and 30 days of inbox covers it. Without `@nestjs/schedule`, a `setInterval()` started in `onModuleInit()` and cleared in `onModuleDestroy()` does the same.

## Support

Nest is an MIT-licensed open source project. It can grow thanks to the sponsors and support by the amazing backers. If you'd like to join them, please [read more here](https://docs.nestjs.com/support).

## Stay in touch

- Author - [Kamil Myśliwiec](https://twitter.com/kammysliwiec)
- Website - [https://nestjs.com](https://nestjs.com/)
- Twitter - [@nestframework](https://twitter.com/nestframework)

## License

Nest is [MIT licensed](LICENSE).
