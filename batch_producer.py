import asyncio
import json
import logging
import random
import string
from datetime import datetime
from aiokafka import AIOKafkaProducer
import sys
import uuid
import ssl

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
topic_name = "test"

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def generate_random_blob(kb_size):
    """Generate a random string blob of specified KB size."""
    # Calculate number of characters needed (1 KB = 1024 bytes)
    num_chars = kb_size * 1024
    # Generate random string using letters and digits
    return ''.join(random.choices(string.ascii_letters + string.digits, k=num_chars))

async def produce_messages_batch():
    producer = AIOKafkaProducer(
        bootstrap_servers='127.0.0.1:9092',
        security_protocol='SASL_SSL',
        ssl_context=ctx,
        sasl_mechanism='PLAIN',
        sasl_plain_username='admin',
        sasl_plain_password='admin-password',
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        # Batching configuration for better performance
        linger_ms=100,  # Wait up to 100ms to batch messages together
        max_batch_size=16384,  # Batch size in bytes (16KB default)
        compression_type='gzip',  # Compress batches for better throughput
        acks='all'  # Wait for all replicas to acknowledge (durability)
    )
    start = int(sys.argv[1])
    end = int(sys.argv[2])
    batch_size = int(sys.argv[3]) if len(sys.argv) > 3 else 100
    blob_kb_size = int(sys.argv[4]) if len(sys.argv) > 4 else 0

    # Generate blob once if needed (reuse for all messages)
    blob_data = generate_random_blob(blob_kb_size) if blob_kb_size > 0 else None

    try:
        await producer.start()
        logger.info("Batch producer started successfully")
        logger.info(f"Producing messages {start} to {end-1} with batch size {batch_size}")
        if blob_data:
            logger.info(f"Each message includes a {blob_kb_size} KB random blob")

        message_count = 0
        batch_count = 0
        futures = []
        total_bytes = 0
        start_time = datetime.now()

        for i in range(start, end):
            message = {
                'id': i,
                'message': f'Hello from batch producer - message {i}',
                'timestamp': datetime.now().isoformat(),
                'message_uuid': str(uuid.uuid4())
            }

            # Add blob data if specified
            if blob_data:
                message['blob'] = blob_data

            # Calculate message size in bytes
            message_bytes = json.dumps(message).encode('utf-8')
            total_bytes += len(message_bytes)

            # Send without waiting - allows batching
            future = await producer.send(topic_name, message)
            futures.append((i, future))
            message_count += 1

            # Flush every batch_size messages
            if message_count % batch_size == 0:
                await producer.flush()
                batch_count += 1
                logger.info(f"Flushed batch {batch_count} ({message_count} messages sent so far)")

                # Optionally verify some futures
                for msg_id, fut in futures[-batch_size:]:
                    try:
                        metadata = await fut
                        logger.debug(f"Message {msg_id} sent to partition {metadata.partition} at offset {metadata.offset}")
                    except Exception as e:
                        logger.error(f"Failed to send message {msg_id}: {e}")

        # Final flush for remaining messages
        await producer.flush()
        end_time = datetime.now()
        elapsed_time = (end_time - start_time).total_seconds()

        logger.info(f"Final flush completed. Total messages sent: {message_count}")

        # Verify all futures
        logger.info("Verifying all messages were sent successfully...")
        failed = 0
        for msg_id, future in futures:
            try:
                await future
            except Exception as e:
                logger.error(f"Message {msg_id} failed: {e}")
                failed += 1

        if failed == 0:
            logger.info(f"All {message_count} messages sent successfully!")
        else:
            logger.warning(f"{failed} messages failed out of {message_count}")

        # Print statistics
        logger.info("=" * 60)
        logger.info("PERFORMANCE STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Total Messages:        {message_count:,}")
        logger.info(f"Total Bytes:           {total_bytes:,} bytes ({total_bytes / 1024 / 1024:.2f} MB)")
        logger.info(f"Time Elapsed:          {elapsed_time:.2f} seconds")
        logger.info(f"Messages/second:       {message_count / elapsed_time:.2f}")
        logger.info(f"Throughput (MB/s):     {(total_bytes / 1024 / 1024) / elapsed_time:.2f}")
        logger.info(f"Avg bytes/message:     {total_bytes / message_count:.2f}")
        logger.info(f"Failed messages:       {failed}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error in batch producer: {e}")
    finally:
        await producer.stop()
        logger.info("Batch producer stopped")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python batch_producer.py <start> <end> [batch_size] [blob_kb_size]")
        print("Example: python batch_producer.py 0 1000 100 5")
        print("  - Sends messages 0-999")
        print("  - Flushes every 100 messages")
        print("  - Each message includes a 5KB random blob")
        sys.exit(1)

    asyncio.run(produce_messages_batch())

