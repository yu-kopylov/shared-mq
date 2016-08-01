# SharedMQ

The SharedMQ is a persistent file-based message queue for IPC written in Java.

It uses memory-mapped files as a storage.

The queue can process up to 140000 messages per second.
The performance of the queue depends on the way it is used.
Big messages can slower it considerably.

## Inter-process Synchronisation

The queue uses locks based on atomic memory operations and memory barriers for inter-process synchronisation.

## Testing Utility

The project contains the "queue-tester" utility that can be run from a command line to test the queue performance and inter-process synchronisation.

To check that locks are working correctly it sends random messages that has equal left and right parts. If it receives messages that do not follow this rule, it will display a non-zero number of corrupted messages. 

## Error Recovery

All operations use a rollback journal that allows to recover the state of the queue in case of process crash.

Changes are not forced to the disk at the end of operation, so queue can become corrupted in case of operating system crash. 

## Limitations

The size of the queue is limited to 2GB.

The message size is limited by 256KB.

## Memory Usage

The queue itself does not consume much memory, since all structures are maintained within memory-mapped files.

However, the queue consumes address space of the application.
It is not a problem for a 64-bit JVM.
But in a 32-bit JVM the address space is limited.
Exceeding it will result in "IOException: Map failed".

