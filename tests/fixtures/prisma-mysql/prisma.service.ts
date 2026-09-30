import { Injectable, type OnApplicationShutdown } from '@nestjs/common';
import { PrismaMariaDb } from '@prisma/adapter-mariadb';
import { PrismaClient, type Prisma } from './generated/client.js';

/** The `tx` that `prisma.$transaction(async (tx) => ...)` passes its callback. */
export type Transaction = Prisma.TransactionClient;

/**
 * An application's Prisma client on MySQL: Prisma 7 reaches MySQL through its MariaDB driver adapter. A pool of two
 * connections, as each application instance of the integration suites has (the MySQL server is shared).
 */
@Injectable()
export class PrismaService extends PrismaClient implements OnApplicationShutdown {
  constructor() {
    const url = new URL(process.env.DATABASE_URL!);
    super({
      adapter: new PrismaMariaDb({
        host: url.hostname,
        port: Number(url.port || 3306),
        user: decodeURIComponent(url.username),
        password: decodeURIComponent(url.password),
        database: url.pathname.slice(1),
        connectionLimit: 2,
        // caching_sha2_password without TLS needs the server's RSA key until it has cached the password (a fresh server).
        allowPublicKeyRetrieval: true,
      }),
    });
  }

  // Runs after onModuleDestroy(), where the outbox relay finishes its in-flight work.
  async onApplicationShutdown() {
    await this.$disconnect();
  }
}
