import { MigrationInterface, QueryRunner } from "typeorm";

export class OutboxTopicSeq1795007494788 implements MigrationInterface {
    name = 'OutboxTopicSeq1795007494788'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`
            CREATE INDEX "outbox_messages_topic_seq" ON "outbox_messages" ("topic", "seq")
        `);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`
            DROP INDEX "public"."outbox_messages_topic_seq"
        `);
    }

}
