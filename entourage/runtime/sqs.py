"""
AWS SQS backend for the ReadyQueue interface.

Kept in its own module so importing the runtime never requires boto3;
this only loads when an SQSReadyQueue is actually constructed.
"""

import json
import logging
from typing import Any, Dict, List

from .interfaces import QueueMessage, ReadyQueue

logger = logging.getLogger(__name__)


class _SQSMessage(QueueMessage):
    def __init__(self, message):
        self._message = message
        self.payload = json.loads(message.body)

    def ack(self):
        self._message.delete()

    def nack(self):
        # Nothing to do — the message reappears after the visibility timeout.
        pass


class SQSReadyQueue(ReadyQueue):
    def __init__(self, queue_name: str = "entourage_tasks", region: str = "us-east-1"):
        import boto3
        from botocore.exceptions import ClientError

        self.queue_name = queue_name
        sqs = boto3.resource("sqs", region_name=region)
        try:
            self._queue = sqs.get_queue_by_name(QueueName=queue_name)
            logger.info("Connected to queue %s", queue_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
                self._queue = sqs.create_queue(QueueName=queue_name, Attributes={})
                logger.info("Created queue %s", queue_name)
            else:
                raise

    def send(self, payload: Dict[str, Any]):
        self._queue.send_message(MessageBody=json.dumps(payload))

    def receive(
        self, max_messages: int = 10, wait_seconds: float = 10
    ) -> List[QueueMessage]:
        messages = self._queue.receive_messages(
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=int(wait_seconds),
        )
        return [_SQSMessage(m) for m in messages]
