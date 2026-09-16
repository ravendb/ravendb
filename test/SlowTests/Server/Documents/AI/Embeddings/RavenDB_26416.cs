using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class RavenDB_26416(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenMultiplatformFact(RavenTestCategory.Vector | RavenTestCategory.Querying, RavenArchitecture.AllX64)]
    public async Task QueryEmbeddings_WhenWorkerIsShutDownByRecordChangeWhileEnqueuing_ShouldNotHangForever()
    {
        using var store = GetDocumentStore();

        var (configuration, _) = AddEmbeddingsGenerationTask(store,
            embeddingsPaths: [new EmbeddingPathConfiguration { Path = "TextualValue", ChunkingOptions = DefaultChunkingOptions }]);

        var (queriesWorkerRegistered, _) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
        Assert.True(queriesWorkerRegistered);

        var database = await GetDatabase(store.Database);
        var generator = database.EmbeddingsGeneratorQueries;
        var taskId = new EmbeddingsGenerationTaskIdentifier(configuration.Identifier);

        var recordWithoutTask = database.ReadDatabaseRecord();
        recordWithoutTask.EmbeddingsGenerations.Clear();

        var alreadyFired = false;
        generator.ForTestingPurposesOnly().AfterWorkerResolvedForQuery = () =>
        {
            if (alreadyFired)
                return;
            alreadyFired = true;

            generator.HandleDatabaseRecordChange(recordWithoutTask);
        };

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var queryTask = generator
                .GetEmbeddingsForQueryAsync(context, taskId, "some text that is not in the embeddings cache")
                .AsTask();

            try
            {
                await queryTask.WaitAsync(TimeSpan.FromSeconds(15));
                Assert.Fail("Expected OperationCanceledException: the worker was shut down before the query enqueued, " +
                            "so the enqueue is rejected and the query must surface cancellation (RavenDB-26416).");
            }
            catch (OperationCanceledException)
            {
                // expected: the rejected enqueue cancels the query's TaskCompletionSource
            }
            catch (TimeoutException)
            {
                Assert.Fail("Querying for embeddings hung forever: HandleDatabaseRecordChange shut the worker down " +
                            "while a query thread was still adding work to it, leaving its TaskCompletionSource never " +
                            "completed (RavenDB-26416).");
            }
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Vector | RavenTestCategory.Etl, RavenArchitecture.AllX64)]
    public async Task StoreDocumentEmbeddings_WhenGeneratorShutsDownWithPendingStore_ShouldNotHangForever()
    {
        using var store = GetDocumentStore();

        var (configuration, _) = AddEmbeddingsGenerationTask(store,
            embeddingsPaths: [new EmbeddingPathConfiguration { Path = "TextualValue", ChunkingOptions = DefaultChunkingOptions }]);

        var database = await GetDatabase(store.Database);
        var taskId = new EmbeddingsGenerationTaskIdentifier(configuration.Identifier);

        using var generator = new EmbeddingsGenerator(database, database.Loggers.GetLogger<EmbeddingsGenerator>(),
            database.DatabaseShutdown, EmbeddingsGenerator.Mode.Etl);

        var storeTaskReady = new TaskCompletionSource<Task>(TaskCreationOptions.RunContinuationsAsynchronously);
        generator.ForTestingPurposesOnly().BeforeShutdownDrain = () =>
        {
            var batch = generator.BatchFor(taskId);
            storeTaskReady.TrySetResult(batch.StoreDocumentEmbeddingsAsync());
        };

        generator.Start(); // InitializeWork -> HandleDatabaseRecordChange -> creates the worker for the task
        Assert.True(await WaitForValueAsync(() => generator.EmbeddingTaskExists(taskId), true),
            "the embeddings worker did not register on the standalone generator");

        generator.Stop(); // cancels the generator; DoWork exits and its finally fires BeforeShutdownDrain

        var storeTask = await storeTaskReady.Task.WaitAsync(TimeSpan.FromSeconds(30));

        try
        {
            await storeTask.WaitAsync(TimeSpan.FromSeconds(30));
        }
        catch (OperationCanceledException)
        {
            // expected: shutting down cancels the pending store. The point is it did not hang.
        }
        catch (TimeoutException)
        {
            Assert.Fail("StoreDocumentEmbeddingsAsync hung forever on shutdown: the generator's pending store " +
                        "request (DocumentEmbeddingsStorageTcs) was never completed (RavenDB-26416, parent store-phase).");
        }
    }

    [RavenFact(RavenTestCategory.Vector)]
    public void CompletableQueue_RejectsEnqueueAfterComplete_ButStillDrainsExistingItems()
    {
        var queue = new CompletableQueue<int>();

        Assert.True(queue.TryEnqueue(1));

        queue.Complete();

        // rejected after Complete() - this is what lets a producer cancel its own awaiter instead of hanging (RavenDB-26416)
        Assert.False(queue.TryEnqueue(2));

        // an item enqueued before Complete() is still drainable; the rejected one never entered
        Assert.True(queue.TryDequeue(out var drained));
        Assert.Equal(1, drained);
        Assert.False(queue.TryDequeue(out _));

        // completed and drained -> WaitToReadAsync returns false, which is what makes the consumer loops exit
        Assert.False(queue.WaitToReadAsync().AsTask().GetAwaiter().GetResult());
    }

    [RavenFact(RavenTestCategory.Vector)]
    public async Task CompletableQueue_CompleteWhileWaiting_ReturnsQueueState_WithoutLeakingCancellation()
    {
        // empty queue, a reader is waiting, then Complete() -> returns false (not an OperationCanceledException)
        var empty = new CompletableQueue<int>();
        var waitOnEmpty = empty.WaitToReadAsync().AsTask();
        Assert.False(waitOnEmpty.IsCompleted); // genuinely parked on the event

        empty.Complete();
        Assert.False(await waitOnEmpty.WaitAsync(TimeSpan.FromSeconds(5)));

        // items still queued at Complete() -> the reader returns true and can drain them, then the next wait returns false
        var withItem = new CompletableQueue<int>();
        Assert.True(withItem.TryEnqueue(42));
        withItem.Complete();

        Assert.True(await withItem.WaitToReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.True(withItem.TryDequeue(out var drained));
        Assert.Equal(42, drained);
        Assert.False(await withItem.WaitToReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [RavenFact(RavenTestCategory.Vector)]
    public async Task CompletableQueue_ExternalCancellation_WhenNotCompleted_Propagates()
    {
        var queue = new CompletableQueue<int>();
        using var cts = new CancellationTokenSource();

        var wait = queue.WaitToReadAsync(cts.Token).AsTask();
        Assert.False(wait.IsCompleted);

        // the queue is NOT completed - a caller's own cancellation must surface as OperationCanceledException,
        // not be swallowed by the completion path
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [RavenFact(RavenTestCategory.Vector)]
    public void CompletableQueue_StaleCallsAfterCompleteAndDispose_DoNotThrow()
    {
        // a stale reference to a removed/shut-down worker may still call TryEnqueue/Wake even after Dispose();
        // those must be safe (reject / no-op), never ObjectDisposedException
        var queue = new CompletableQueue<int>();
        queue.Complete();
        queue.Dispose();

        Assert.False(queue.TryEnqueue(1));
        queue.Wake();
    }

    [RavenMultiplatformFact(RavenTestCategory.Vector | RavenTestCategory.Etl, RavenArchitecture.AllX64)]
    public async Task StoreDocumentEmbeddings_WhenSubmittedAfterGeneratorShutdown_IsCancelledNotHung()
    {
        using var store = GetDocumentStore();

        var (configuration, _) = AddEmbeddingsGenerationTask(store,
            embeddingsPaths: [new EmbeddingPathConfiguration { Path = "TextualValue", ChunkingOptions = DefaultChunkingOptions }]);

        var database = await GetDatabase(store.Database);
        var taskId = new EmbeddingsGenerationTaskIdentifier(configuration.Identifier);

        using var generator = new EmbeddingsGenerator(database, database.Loggers.GetLogger<EmbeddingsGenerator>(),
            database.DatabaseShutdown, EmbeddingsGenerator.Mode.Etl);

        generator.Start();
        Assert.True(await WaitForValueAsync(() => generator.EmbeddingTaskExists(taskId), true),
            "the embeddings worker did not register on the standalone generator");

        generator.Stop(); // DoWork's finally Complete()s the parent queue and drains it

        var storeTask = generator.BatchFor(taskId).StoreDocumentEmbeddingsAsync();

        try
        {
            await storeTask.WaitAsync(TimeSpan.FromSeconds(15));
            Assert.Fail("Expected OperationCanceledException: a store request submitted after shutdown must be cancelled (RavenDB-26416).");
        }
        catch (OperationCanceledException)
        {
            // expected
        }
        catch (TimeoutException)
        {
            Assert.Fail("StoreDocumentEmbeddingsAsync hung after the generator was stopped (RavenDB-26416, parent store-phase).");
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Vector | RavenTestCategory.Etl, RavenArchitecture.AllX64)]
    public async Task EtlBatch_WithMultipleNewEmbeddings_IsGeneratedInOneModelCall()
    {
        using var store = GetDocumentStore();

        var database = await GetDatabase(store.Database);
        var batchSizes = new ConcurrentBag<int>();
        database.EmbeddingsGeneratorEtl.ForTestingPurposesOnly().OnGenerateBatch = count => batchSizes.Add(count);

        const int docs = 3;
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < docs; i++)
                session.Store(new EmbeddingsBatchDoc { TextualValue = $"value-{i}" });
            session.SaveChanges();
        }

        AddEmbeddingsGenerationTask(store,
            embeddingsPaths: [new EmbeddingPathConfiguration { Path = "TextualValue", ChunkingOptions = DefaultChunkingOptions }],
            collectionName: "EmbeddingsBatchDocs");

        // the whole ETL batch is one work unit -> a single GenerateAsync call (RavenDB-26416)
        Assert.Equal(docs, await WaitForValueAsync(() => batchSizes.Sum(), docs, timeout: (int)DefaultEtlTimeout.TotalMilliseconds));
        Assert.Equal(1, batchSizes.Count);
    }

    [RavenMultiplatformFact(RavenTestCategory.Vector | RavenTestCategory.Etl, RavenArchitecture.AllX64)]
    public async Task EtlBatch_LargerThanMaxBatchSize_IsSplitIntoCeilingNumberOfCalls()
    {
        using var store = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record => record.Settings["Ai.Embeddings.MaxBatchSize"] = "2"
        });

        var database = await GetDatabase(store.Database);
        var batchSizes = new ConcurrentBag<int>();
        database.EmbeddingsGeneratorEtl.ForTestingPurposesOnly().OnGenerateBatch = count => batchSizes.Add(count);

        const int docs = 5;
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < docs; i++)
                session.Store(new EmbeddingsBatchDoc { TextualValue = $"value-{i}" });
            session.SaveChanges();
        }

        AddEmbeddingsGenerationTask(store,
            embeddingsPaths: [new EmbeddingPathConfiguration { Path = "TextualValue", ChunkingOptions = DefaultChunkingOptions }],
            collectionName: "EmbeddingsBatchDocs");

        // each doc is a single chunk, so the cap (a per-work-item threshold) splits the 5 into ceil(5/2) = 3 calls
        Assert.Equal(docs, await WaitForValueAsync(() => batchSizes.Sum(), docs, timeout: (int)DefaultEtlTimeout.TotalMilliseconds));
        Assert.Equal(3, batchSizes.Count);
        Assert.True(batchSizes.All(size => size <= 2), "with single-chunk work items every call stays within maxBatchSize");
    }

    [RavenMultiplatformFact(RavenTestCategory.Vector | RavenTestCategory.Etl, RavenArchitecture.AllX64)]
    public async Task SingleEmbeddingWithMoreChunksThanMaxBatchSize_IsSentAtomicallyInOneCall()
    {
        // maxBatchSize is a per-work-item threshold (existing v7.2 behavior), not a strict cap on input values:
        // one source value's chunks are always sent in a single GenerateAsync call, even when they exceed the cap.
        var chunking = new ChunkingOptions { ChunkingMethod = ChunkingMethod.PlainTextSplitLines, MaxTokensPerChunk = 1 };
        var manyLines = string.Join("\n", Enumerable.Range(0, 12).Select(i => $"distinctword{i:D3}value"));
        var expectedChunks = TextChunker.Chunk(manyLines, chunking).Count(c => string.IsNullOrWhiteSpace(c) == false);
        Assert.True(expectedChunks > 2, $"test setup must produce more chunks than maxBatchSize; got {expectedChunks}");

        using var store = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record => record.Settings["Ai.Embeddings.MaxBatchSize"] = "2"
        });

        var database = await GetDatabase(store.Database);
        var batchSizes = new ConcurrentBag<int>();
        database.EmbeddingsGeneratorEtl.ForTestingPurposesOnly().OnGenerateBatch = count => batchSizes.Add(count);

        using (var session = store.OpenSession())
        {
            session.Store(new EmbeddingsBatchDoc { TextualValue = manyLines });
            session.SaveChanges();
        }

        AddEmbeddingsGenerationTask(store,
            embeddingsPaths: [new EmbeddingPathConfiguration { Path = "TextualValue", ChunkingOptions = chunking }],
            collectionName: "EmbeddingsBatchDocs");

        // one field = one GenerateEmbeddings of all its chunks -> a single atomic call that exceeds the cap, never split
        Assert.Equal(expectedChunks, await WaitForValueAsync(() => batchSizes.Sum(), expectedChunks, timeout: (int)DefaultEtlTimeout.TotalMilliseconds));
        Assert.Equal(1, batchSizes.Count);
        Assert.True(batchSizes.Single() > 2, $"a single work item must not be split by maxBatchSize; expected one call of {expectedChunks}");
    }

    [RavenMultiplatformFact(RavenTestCategory.Vector | RavenTestCategory.Querying, RavenArchitecture.AllX64)]
    public async Task Query_IsGeneratedEagerlyAsAUnitOfOne()
    {
        using var store = GetDocumentStore();

        var (configuration, _) = AddEmbeddingsGenerationTask(store,
            embeddingsPaths: [new EmbeddingPathConfiguration { Path = "TextualValue", ChunkingOptions = DefaultChunkingOptions }]);

        var (queriesWorkerRegistered, _) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
        Assert.True(queriesWorkerRegistered);

        var database = await GetDatabase(store.Database);
        var taskId = new EmbeddingsGenerationTaskIdentifier(configuration.Identifier);

        var batchSizes = new ConcurrentBag<int>();
        database.EmbeddingsGeneratorQueries.ForTestingPurposesOnly().OnGenerateBatch = count => batchSizes.Add(count);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            await database.EmbeddingsGeneratorQueries
                .GetEmbeddingsForQueryAsync(context, taskId, "a query value that is not in the cache")
                .AsTask();
        }

        Assert.Equal(1, batchSizes.Count);
        Assert.Equal(1, batchSizes.Single());
    }
    private sealed class EmbeddingsBatchDoc
    {
        public string Id { get; set; }
        public string TextualValue { get; set; }
    }
}
