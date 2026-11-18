-- CreateIndex
CREATE INDEX "outbox_messages_topic_seq" ON "outbox_messages"("topic", "seq");
