using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Voron;
using Sparrow.Collections;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public class LockFreeRingBufferTests(ITestOutputHelper output) : StorageTest(output)
    {
        [RavenFact(RavenTestCategory.Core)]
        public unsafe void EnsureStructuralProperties()
        {
            LockFreeRingBuffer<long>.Cell[] cells = new LockFreeRingBuffer<long>.Cell[2];
            long seqMemoryLocation = (long)Unsafe.AsPointer(ref cells[0].Sequence);
            long valueMemoryLocation = (long)Unsafe.AsPointer(ref cells[0].Value);
            long nextSeqMemoryLocation = (long)Unsafe.AsPointer(ref cells[1].Sequence);

            Assert.Equal(64, valueMemoryLocation - seqMemoryLocation);
            Assert.Equal(0, Unsafe.SizeOf<LockFreeRingBuffer<long>.Cell>() % 64);
            Assert.Equal(0, (nextSeqMemoryLocation - seqMemoryLocation) % 64);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Enqueue_Dequeue_SingleItem()
        {
            var ringBuffer = new LockFreeRingBuffer<int>(8);

            int item = 42;
            bool enqueueResult = ringBuffer.TryEnqueue(item);
            bool dequeueResult = ringBuffer.TryDequeue(out int dequeuedItem);

            Assert.True(enqueueResult);
            Assert.True(dequeueResult);
            Assert.Equal(item, dequeuedItem);
            Assert.True(ringBuffer.IsEmpty);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Enqueue_Dequeue_MultipleItems()
        {
            var ringBuffer = new LockFreeRingBuffer<int>(8);

            int[] items = { 1, 2, 3, 4, 5 };
            foreach (var item in items)
            {
                bool enqueueResult = ringBuffer.TryEnqueue(item);
                Assert.True(enqueueResult, $"Enqueue should succeed for item {item}.");
            }

            foreach (var expectedItem in items)
            {
                bool dequeueResult = ringBuffer.TryDequeue(out int dequeuedItem);
                Assert.True(dequeueResult);
                Assert.Equal(expectedItem, dequeuedItem);
            }

            Assert.True(ringBuffer.IsEmpty);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Enqueue_BufferFull()
        {
            var ringBuffer = new LockFreeRingBuffer<int>(8);

            int capacity = 8;

            // Enqueue 'capacity' items
            for (int i = 0; i < capacity; i++)
            {
                bool enqueueResult = ringBuffer.TryEnqueue(i);
                Assert.True(enqueueResult, $"Enqueue should succeed for item {i}.");
            }

            // Attempt to enqueue one more item, which should fail
            bool finalEnqueueResult = ringBuffer.TryEnqueue(999);

            Assert.False(finalEnqueueResult);
            Assert.True(ringBuffer.IsFull);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Dequeue_BufferEmpty()
        {
            var ringBuffer = new LockFreeRingBuffer<int>(8);

            // Act
            bool dequeueResult = ringBuffer.TryDequeue(out int dequeuedItem);

            // Assert
            Assert.False(dequeueResult);
            Assert.Equal(default(int), dequeuedItem);
            Assert.True(ringBuffer.IsEmpty);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Enqueue_Dequeue_WrapAround()
        {
            var ringBuffer = new LockFreeRingBuffer<int>(8);

            int capacity = 8;
            int halfCapacity = capacity / 2 + 1;
            int[] firstBatch = new int[halfCapacity];
            int[] secondBatch = new int[halfCapacity];

            for (int i = 0; i < halfCapacity; i++)
            {
                firstBatch[i] = i;
                bool enqueueResult = ringBuffer.TryEnqueue(firstBatch[i]);
                Assert.True(enqueueResult, $"Enqueue should succeed for item {firstBatch[i]}.");
            }

            for (int i = 0; i < halfCapacity; i++)
            {
                bool dequeueResult = ringBuffer.TryDequeue(out int dequeuedItem);
                Assert.True(dequeueResult, $"Dequeue should succeed for each enqueued item {i}.");
                Assert.Equal(firstBatch[i], dequeuedItem);
            }

            for (int i = 0; i < halfCapacity; i++)
            {
                secondBatch[i] = i + 100;
                bool enqueueResult = ringBuffer.TryEnqueue(secondBatch[i]);
                Assert.True(enqueueResult, $"Enqueue should succeed for item {secondBatch[i]}.");
            }

            for (int i = 0; i < halfCapacity; i++)
            {
                bool dequeueResult = ringBuffer.TryDequeue(out int dequeuedItem);
                Assert.True(dequeueResult, $"Dequeue should succeed for each enqueued item {i}.");
                Assert.Equal(secondBatch[i], dequeuedItem);
            }

            Assert.True(ringBuffer.IsEmpty);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Enqueue_Dequeue_MixedOperations()
        {
            var ringBuffer = new LockFreeRingBuffer<int>(8);

            int[] items = [10, 20, 30, 40, 50, 60, 70, 80];

            // Act & Assert
            foreach (var item in items)
            {
                bool enqueueResult = ringBuffer.TryEnqueue(item);
                Assert.True(enqueueResult, $"Enqueue should succeed for item {item}.");
            }

            // Buffer should be full now
            Assert.True(ringBuffer.IsFull);

            // Dequeue first four items
            for (int i = 0; i < 4; i++)
            {
                bool dequeueResult = ringBuffer.TryDequeue(out int dequeuedItem);
                Assert.True(dequeueResult, $"Dequeue should succeed for each enqueued item {i}.");
                Assert.Equal(items[i], dequeuedItem);
            }

            // Buffer should not be full yet
            Assert.False(ringBuffer.IsFull);

            // Enqueue four more items
            int[] additionalItems = { 90, 100, 110, 120 };
            foreach (var item in additionalItems)
            {
                bool enqueueResult = ringBuffer.TryEnqueue(item);
                Assert.True(enqueueResult, $"Enqueue should succeed for item {item}.");
            }

            // Buffer should be full again
            Assert.True(ringBuffer.IsFull);

            // Dequeue all remaining items
            for (int i = 4; i < items.Length; i++)
            {
                bool dequeueResult = ringBuffer.TryDequeue(out int dequeuedItem);
                Assert.True(dequeueResult, $"Dequeue should succeed for each enqueued item {i}.");
                Assert.Equal(items[i], dequeuedItem);
            }

            foreach (var item in additionalItems)
            {
                bool dequeueResult = ringBuffer.TryDequeue(out int dequeuedItem);
                Assert.True(dequeueResult, "Dequeue should succeed for each enqueued item.");
                Assert.Equal(item, dequeuedItem);
            }

            // Assert buffer is empty after all dequeues
            Assert.True(ringBuffer.IsEmpty, "Ring buffer should be empty after all dequeues.");
        }

        public static IEnumerable<object[]> Configuration =>
            new List<object[]>
            {
                // With this number we ensure hitting full and empty conditions, bigger than that probability
                // will ensure or the other are more common. 
                new object[] { 10000, Random.Shared.Next(), 8 },
                new object[] { 1000, Random.Shared.Next(), 8 },
                new object[] { 100, Random.Shared.Next(), 8 },
            };

        [RavenTheory(RavenTestCategory.Core)]
        [MemberData(nameof(Configuration))]
        public void Enqueue_Dequeue_RandomSequences_FuzzingTest(int totalOperations = 1000, int seed = 1337, int bufferCapacity = 8)
        {
            const double enqueueProbability = 0.5; // 50% chance to enqueue

            // Initialize the lock-free ring buffer with the specified capacity
            var ringBuffer = new LockFreeRingBuffer<int>(bufferCapacity);
            // Initialize a reference queue to track expected behavior
            var referenceQueue = new Queue<int>();
            // Initialize a random number generator
            var random = new Random(seed);

            for (int operation = 0; operation < totalOperations; operation++)
            {
                bool shouldEnqueue;

                // Determine whether to enqueue or dequeue based on current state
                if (referenceQueue.Count == 0)
                {
                    // If the reference queue is empty, we must enqueue
                    shouldEnqueue = true;
                }
                else if (referenceQueue.Count >= bufferCapacity)
                {
                    // If the reference queue is full, we must dequeue
                    shouldEnqueue = false;
                }
                else
                {
                    // Otherwise, decide randomly
                    shouldEnqueue = random.NextDouble() < enqueueProbability;
                }

                if (shouldEnqueue)
                {
                    // Generate a unique item to enqueue
                    int item = operation;

                    // Attempt to enqueue the item into the ring buffer
                    bool enqueueResult = ringBuffer.TryEnqueue(item);
                    // Determine the expected result based on the reference queue's state
                    bool expectedEnqueueResult = referenceQueue.Count < bufferCapacity;

                    // Assert that the enqueue result matches the expectation
                    Assert.Equal(expectedEnqueueResult, enqueueResult);

                    if (enqueueResult)
                    {
                        // If enqueue succeeded, enqueue the item into the reference queue
                        referenceQueue.Enqueue(item);
                    }
                }
                else
                {
                    // Attempt to dequeue an item from the ring buffer
                    bool dequeueResult = ringBuffer.TryDequeue(out int dequeuedItem);
                    // Determine the expected result based on the reference queue's state
                    bool expectedDequeueResult = referenceQueue.Count > 0;

                    // Assert that the dequeue result matches the expectation
                    Assert.Equal(expectedDequeueResult, dequeueResult);

                    if (dequeueResult)
                    {
                        // If dequeue succeeded, dequeue the item from the reference queue
                        int expectedItem = referenceQueue.Dequeue();
                        // Assert that the dequeued item matches the expected item
                        Assert.Equal(expectedItem, dequeuedItem);
                    }
                }
            }

            // After all operations, ensure that the ring buffer and reference queue are consistent
            while (referenceQueue.Count > 0)
            {
                bool dequeueResult = ringBuffer.TryDequeue(out int dequeuedItem);
                Assert.True(dequeueResult, "Should be able to dequeue remaining items.");
                int expectedItem = referenceQueue.Dequeue();
                Assert.Equal(expectedItem, dequeuedItem);
            }

            // Finally, assert that the ring buffer is empty
            Assert.True(ringBuffer.IsEmpty, "Ring buffer should be empty after all dequeues.");
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Enqueue_Dequeue_MultithreadedOperations()
        {
            var ringBuffer = new LockFreeRingBuffer<int>(2048);

            const int producerCount = 64; // Number of producer tasks
            const int consumerCount = 64; // Number of consumer tasks
            const int itemsPerProducer = 250_00; // Number of items each producer will enqueue

            const int totalItems = producerCount * itemsPerProducer;

            // Thread-safe collections to track enqueued and dequeued items
            var enqueuedItems = new ConcurrentBag<int>();
            var dequeuedItems = new ConcurrentBag<int>();

            // Atomic counter for unique item generation
            int itemCounter = -1;

            // CancellationTokenSource to handle potential timeouts
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120)); // 30-second timeout

            // Parallel Options
            var parallelOptions = new ParallelOptions
            {
                MaxDegreeOfParallelism = producerCount + consumerCount,
                CancellationToken = cts.Token
            };


            Parallel.For(0, producerCount + consumerCount, parallelOptions, (i, state) =>
            {
                if (i < producerCount)
                {
                    // Producer Task
                    for (int j = 0; j < itemsPerProducer; j++)
                    {
                        // Generate a unique item
                        int item = Interlocked.Increment(ref itemCounter);

                        // Attempt to enqueue the item with retry logic
                        while (ringBuffer.TryEnqueue(item) == false)
                        {
                            // Buffer is full, wait and retry
                            Thread.Sleep(1);

                            // Check for cancellation
                            if (cts.Token.IsCancellationRequested)
                            {
                                state.Stop();
                                return;
                            }
                        }

                        // Track the enqueued item
                        enqueuedItems.Add(item);
                    }
                }
                else
                {
                    // Consumer Task
                    while (dequeuedItems.Count < totalItems)
                    {
                        if (ringBuffer.TryDequeue(out int item))
                        {
                            // Track the dequeued item
                            dequeuedItems.Add(item);
                        }
                        else
                        {
                            // Buffer is empty, wait and retry
                            Thread.Sleep(1);
                        }

                        // Check for cancellation
                        if (cts.Token.IsCancellationRequested)
                        {
                            state.Stop();
                            return;
                        }
                    }
                }
            });


            // Total items enqueued should match total items dequeued
            Assert.Equal(totalItems, enqueuedItems.Count);
            Assert.Equal(totalItems, dequeuedItems.Count);

            // All enqueued items are present in dequeued items
            var enqueuedSet = new HashSet<int>(enqueuedItems);
            var dequeuedSet = new HashSet<int>(dequeuedItems);

            Assert.True(enqueuedSet.SetEquals(dequeuedSet), "Enqueued and dequeued items do not match.");

            // No duplicates in dequeued items
            Assert.Equal(dequeuedItems.Count, dequeuedSet.Count);

            // Ring buffer should be empty after all operations
            Assert.True(ringBuffer.IsEmpty, "Ring buffer has to be empty after all dequeues.");
        }
    }
}
