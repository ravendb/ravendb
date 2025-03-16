using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Embeddings;
using Nito.AsyncEx;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.ServerWide;
using Raven.Server.Background;
using Raven.Server.Config;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json.Parsing;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;

namespace Raven.Server.Documents.AI.Embeddings;

#pragma warning disable SKEXP0001

[SuppressMessage("CancellationToken", "RDB0010:Async method should have a CancellationToken in its argument list")]
[SuppressMessage("ConfigureAwait", "RDB0002:Awaited operations must have ConfigureAwait(false)")]
public class EmbeddingsGenerator(DocumentDatabase database, RavenLogger logger, CancellationToken shutdown) : BackgroundWorkBase(database.Name, logger, shutdown)
{
    private readonly ConcurrentDictionary<EmbeddingsGenerationTaskIdentifier, AiWorker> _workers = [];
    private readonly ConcurrentQueue<List<Work>> _toCache = new();
    private readonly AsyncManualResetEvent _hasWork = new();
    private class Work
    {
        public TaskCompletionSource<ReadOnlyMemory<byte>> TaskCompletionSource;
        public string Value;
        public TimeSpan CacheDuration;
        public string CacheDocumentId;
        public string EmbeddingHash;
        public string ValueHash;
        public int TokenCount;
        public bool CompleteTaskAfterStorage;
        public ReadOnlyMemory<byte> EmbeddingValue;
    }

    private class AiWorker
    {
        private readonly DocumentsStorage _documentsStorage;
        private readonly EmbeddingsGenerationConfiguration _configuration;
        private readonly AiConnectionString _connectionString;
        private readonly ITextEmbeddingGenerationService _embeddingGenerationService;
        private readonly CancellationToken _cancellationToken;
        private readonly ConcurrentDictionary<string, Task<ReadOnlyMemory<byte>>> _inMemoryCache = new(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentQueue<Work> _work = new();
        private readonly AsyncManualResetEvent _hasWork = new();
        private readonly Task[] _tasks;
        private readonly EmbeddingsGenerator _parent;
        private readonly CancellationTokenSource _shutdown;
        private int _taskIsRunning;
        private AiConnectionStringIdentifier _connectionStringIdentifier;

        public AiWorker(EmbeddingsGenerator parent,DocumentsStorage documentsStorage, EmbeddingsGenerationConfiguration configuration,
            AiConnectionString connectionString, int maxConcurrentBatches, CancellationToken cancellationToken) 
        {
            if(maxConcurrentBatches < 1)
                throw new InvalidDataException($"QueryEmbeddingsMaxConcurrentBatches for {configuration.Name} must be at least 1");
            _documentsStorage = documentsStorage;
            _parent = parent;
            _configuration = configuration;
            _connectionString = connectionString;
            _connectionStringIdentifier = new AiConnectionStringIdentifier(_connectionString.Identifier);
            _embeddingGenerationService = AiHelper.CreateService(connectionString);
            _shutdown = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _cancellationToken = _shutdown.Token;
            var shutdownTask = new TaskCompletionSource();
            _cancellationToken.Register(_ => shutdownTask.TrySetCanceled(), null);
            _tasks = new Task[maxConcurrentBatches+1];
            Array.Fill(_tasks, Task.CompletedTask);
            _tasks[^1] = shutdownTask.Task;
        }

        public int MaxTokensPerChunkForQueries => _configuration.ChunkingOptionsForQuerying.MaxTokensPerChunk;

        public ValueTask<ReadOnlyMemory<byte>> GetEmbeddingsForQuery(DocumentsOperationContext documentsContext, string chunkedValue, int tokenCount)
        {
            var valueHash = EmbeddingsHelper.CalculateInputValueHash(chunkedValue);
            var docId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(_connectionStringIdentifier, valueHash, _configuration.Quantization);
            var attachment = _documentsStorage.AttachmentsStorage.GetAttachment(documentsContext, docId, valueHash, AttachmentType.Document, null);
            if (attachment != null)
            {
                //TODO: increment expiration
                
                var stream = attachment.Stream;
                //TODO: Need to find a way to avoid this allocation in favor of pooling, etc.
                byte[] buffer = new byte[attachment.Size];
                stream.ReadExactly(buffer);
                return new ValueTask<ReadOnlyMemory<byte>>(buffer);
            }

            var work = new Work
            {
                TaskCompletionSource = new (TaskCreationOptions.RunContinuationsAsynchronously),
                Value = chunkedValue,
                ValueHash = valueHash,
                CacheDocumentId = docId,
                TokenCount = tokenCount,
                CacheDuration = _configuration.EmbeddingsCacheForQueryingExpiration
            };
            var localTask = work.TaskCompletionSource.Task;
            var inCacheTask = _inMemoryCache.GetOrAdd(chunkedValue, localTask);
            if (inCacheTask == localTask)
            {
                _work.Enqueue(work);
                _hasWork.Set();
            }

            return new(inCacheTask);
        }

        private Task GenerateEmbeddingsFor(string chunkedValue, string cacheDocId, string valueHash, int tokenCount)
        {
            var work = new Work
            {
                TaskCompletionSource = new (TaskCreationOptions.RunContinuationsAsynchronously),
                Value = chunkedValue,
                ValueHash = valueHash,
                CacheDocumentId = cacheDocId,
                TokenCount = tokenCount,
                CacheDuration = _configuration.EmbeddingsCacheExpiration,
                CompleteTaskAfterStorage = true
            };
            var localTask = work.TaskCompletionSource.Task;
            var inCacheTask = _inMemoryCache.GetOrAdd(chunkedValue, localTask);
            if (inCacheTask == localTask)
            {
                _work.Enqueue(work);
                _hasWork.Set();
            }

            return inCacheTask;
        }
        
        public bool GenerateEmbeddingsToCache(DocumentsOperationContext documentsContext, string chunkedValue, int tokensCount,
            ref List<Task> tasks)
        {
            var valueHash = EmbeddingsHelper.CalculateInputValueHash(chunkedValue);
            var docId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(_connectionStringIdentifier, valueHash, _configuration.Quantization);
            if (_documentsStorage.AttachmentsStorage.AttachmentExists(documentsContext, docId, valueHash))
                return true;
            tasks ??= [];
            tasks.Add(GenerateEmbeddingsFor(chunkedValue, docId, valueHash, tokensCount));
            return false;
        }

        private ValueTask<int> GetAvailableTaskIndexAsync()
        {
            for (int i = 0; i < _tasks.Length; i++)
            {
                if (_tasks[i].IsCompleted)
                    return ValueTask.FromResult(i);
            }

            return new (WithAsyncWait());

            async Task<int> WithAsyncWait()
            {
                var task = await Task.WhenAny(_tasks);
                return Array.IndexOf(_tasks, task);
            } 
        }

        public async Task ShutdownAsync()
        {
            _shutdown.Cancel(false);
            try
            {
                await Task.WhenAll(_tasks);
            }
            catch (OperationCanceledException)
            {
                // this is fine, the last task is always the shutdown task
                // that we just cancelled
            }
            finally
            {
                    _taskIsRunning = 0; // only needed for debugging                
            }
        }
        
        public async Task RunAsync()
        {
            if (Interlocked.Increment(ref _taskIsRunning) != 1)
                return; // we may race to start it, so we skip the next one
            
            int maxTokens = _configuration.ChunkingOptionsForQuerying.MaxTokensPerChunk;
            // This gives us 7/8 of the max (448/512, 1792/2048, 3584/4096, 7168/8192)
            // The idea is that we don't want to _rely_ on the estimated token count to 
            // be accurate, so we leave ourselves a little cushion. 
            maxTokens -= maxTokens / 8; 
            List<string> batch = [];
            List<Work> works = [];
            int currentBatchTokens = 0;
            while (_cancellationToken.IsCancellationRequested == false)
            {
                await _hasWork.WaitAsync(_cancellationToken);
                _hasWork.Reset();

                while (_work.TryDequeue(out var work))
                {
                    if (currentBatchTokens + work.TokenCount > maxTokens)
                    {
                        RegisterBatch(await GetAvailableTaskIndexAsync());
                    }
                    batch.Add(work.Value);
                    works.Add(work);
                    currentBatchTokens += work.TokenCount;
                }

                if (batch.Count > 0)
                {
                    // GetAvailableTaskIndexAsync is ensuring that we aren't running
                    // too many concurrent tasks, while the actual batch itself
                    // is running in the background
                    RegisterBatch(await GetAvailableTaskIndexAsync());
                }
            }

            void RegisterBatch(int index)
            {
                if (_cancellationToken.IsCancellationRequested)
                {
                    foreach (Work work in works)
                    {
                        work.TaskCompletionSource.TrySetCanceled();
                    }

                    return;
                }

                _tasks[index] = FlushBatchAsync(batch, works);
                batch = [];
                works = [];
                currentBatchTokens = 0;
            }
        }

        private async Task FlushBatchAsync(List<string> batch, List<Work> works)
        {
            try
            {
                PortableExceptions.ThrowIf<IOException>(works.Count != batch.Count, "Unexpected number of batches, works & batches must have the same count!");

                IList<ReadOnlyMemory<float>> allEmbeddings;

                try
                {
                    allEmbeddings = await AiHelper.GenerateEmbeddingsAsync(_embeddingGenerationService, batch, _cancellationToken);
                }
                catch (HttpOperationException httpOperationException) when (httpOperationException.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    throw new EmbeddingGenerationException(
                        $"Failed to generate embeddings due to rate limits. Consider decreasing the number of elements processed in a single batch " +
                        $"('{RavenConfiguration.GetKey(x => x.Ai.QueryEmbeddingsMaxBatchSize)}') or increasing the " +
                        $"limits on your model deployment.", httpOperationException);
                }


                PortableExceptions.ThrowIf<IOException>(allEmbeddings.Count != batch.Count, "Model returned a different count of embeddings than expected");

                // we iterate twice - we want to free the waiting threads for this ASAP
                for (int i = 0; i < allEmbeddings.Count; i++)
                {
                  
                    var embeddingValue = EmbeddingsHelper.CreateEmbeddingValue(allEmbeddings[i], _configuration.Quantization);
                    works[i].EmbeddingValue = embeddingValue;
                    
                    if (works[i].CompleteTaskAfterStorage)
                        continue;
                    // means it is from queries, and we want to release the thread ASAP and send it the embeddings
                    works[i].TaskCompletionSource.TrySetResult(embeddingValue);
                }

                for (int i = 0; i < allEmbeddings.Count; i++)
                {
                    works[i].EmbeddingHash = AttachmentsStorageHelper.CalculateHash(works[i].EmbeddingValue.Span);
                }

                _parent.WriteToCache(works);
            }
            catch (Exception e)
            {
                foreach (Work work in works)
                {
                    work.TaskCompletionSource.TrySetException(e);
                    _inMemoryCache.TryRemove(work.Value, out _);
                }
            }
        }

        public bool ModifiedFrom(EmbeddingsGenerationConfiguration updated, AiConnectionString updateConnectionString)
        {
            return _configuration.Compare(updated) != EtlConfigurationCompareDifferences.None || 
                   _connectionString.Compare(updateConnectionString) != AiSettingsCompareDifferences.None;
        }
    }

    private void WriteToCache(List<Work> works)
    {
        _toCache.Enqueue(works);
        _hasWork.Set();
    }

    public bool GenerateEmbeddingsToCache(DocumentsOperationContext documentsContext,
        EmbeddingsGenerationTaskIdentifier embeddingTaskId, string value,
        ref List<Task> tasks)
    {
        var worker = _workers[embeddingTaskId];
        bool allInCache = true;
        foreach (var (text, tokenCount) in TextChunker.ChunkPlainText(value, worker.MaxTokensPerChunkForQueries))
        {
            allInCache &= worker.GenerateEmbeddingsToCache(documentsContext, text, tokenCount, ref tasks);
        }

        return allInCache;
    }

    public ValueTask<ReadOnlyMemory<ReadOnlyMemory<byte>>> GetEmbeddingsForQueryAsync(DocumentsOperationContext documentsContext,
        EmbeddingsGenerationTaskIdentifier embeddingTaskId, params ReadOnlySpan<string> values)
    {
        var worker = _workers[embeddingTaskId];
        var results = new List<ReadOnlyMemory<byte>>();
        List<Task<ReadOnlyMemory<byte>>> tasks = null;
        // we explicitly do *not* care about the order of vectors compared to the text, including with chunking or 
        // with multiple values. Logically, we send text, and get a set of vectors back, in some arbitrary order
        foreach (string value in values)
        {
            foreach (var (text, tokenCount) in TextChunker.ChunkPlainText(value, worker.MaxTokensPerChunkForQueries))
            {
                var task = worker.GetEmbeddingsForQuery(documentsContext, text, tokenCount);
                if (task.IsCompleted)
                {
                    results.Add(task.Result);
                    continue;
                }

                tasks ??= [];
                tasks.Add(task.AsTask());
            }
        }
        if(tasks == null)
            return ValueTask.FromResult(new ReadOnlyMemory<ReadOnlyMemory<byte>>(results.ToArray()));

        return new(CompleteAsync());
        
        async Task<ReadOnlyMemory<ReadOnlyMemory<byte>>> CompleteAsync()
        {
            results.AddRange(await Task.WhenAll(tasks));
            return new ReadOnlyMemory<ReadOnlyMemory<byte>>(results.ToArray());
        } 
    }
    
    private AiWorker CreateAiWorker(EmbeddingsGenerationTaskIdentifier id)
    {
        var record = database.ReadDatabaseRecord();
        foreach (var task in record.EmbeddingsGenerations)
        {
            if (task.Disabled)
                throw new InvalidOperationException($"The task {id.Value} has been disabled and cannot be used");
            
            if (string.Equals(id.Value, task.Identifier, StringComparison.OrdinalIgnoreCase))
            {
                var connectionString = GetConnectionString(record, task);
                int maxConcurrentBatches = connectionString.GetQueryEmbeddingsMaxConcurrentBatches(database.Configuration.Ai.QueryEmbeddingsMaxConcurrentBatches);
                return new AiWorker(this, database.DocumentsStorage, task, connectionString, maxConcurrentBatches, CancellationToken);
            }
        }
        
        throw new InvalidOperationException($"Could not find an embedding task named: {id.Value}");
    }

    private static AiConnectionString GetConnectionString(DatabaseRecord record, EmbeddingsGenerationConfiguration task)
    {
        foreach (var (name, conStr) in record.AiConnectionStrings)
        {
            if (string.Equals(task.ConnectionStringName, name, StringComparison.OrdinalIgnoreCase))
            {
                return conStr;
            }
        }

        throw new InvalidOperationException($"Could not find connection string '{task.ConnectionStringName}' for '{task.ConnectionStringName}'");
    }

    protected override void InitializeWork()
    {
        var record = database.ReadDatabaseRecord();
        HandleDatabaseRecordChange(record);
        foreach (var (name, state) in _workers)
        {
            _ = state.RunAsync();
        }
    }

    protected override async Task DoWork()
    {
        try
        {
            List<Task> tasks = [];
            List<Work> pending = [];
            while (CancellationToken.IsCancellationRequested == false)
            {
                tasks.Clear();
                pending.Clear();
                await _hasWork.WaitAsync(CancellationToken);
                _hasWork.Reset();
                try
                {
                    while (_toCache.TryDequeue(out List<Work> works))
                    {
                        pending.AddRange(works);
                        tasks.Add(database.TxMerger.Enqueue(new PutEmbeddingsIntoCacheCommand(works)));
                    }
                    await Task.WhenAll(tasks);
                    foreach (var work in pending)
                    {
                        work.TaskCompletionSource.TrySetResult(default);
                    }
                }
                catch (Exception e)
                {
                    foreach (var work in pending)
                    {
                        work.TaskCompletionSource.TrySetException(e);
                    }
                }
            }
        }
        finally
        {
            //TODO: wait and then fail
            foreach (var (_, state)  in _workers)
            {
                _ = state.ShutdownAsync();
            }
            while (_toCache.TryDequeue(out List<Work> works))
            {
                foreach (var work in works)
                {
                    work.TaskCompletionSource.TrySetCanceled();
                }
            }

        }
    }
    
    private sealed class PutEmbeddingsIntoCacheCommand(List<Work> work)
        : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
    {
        private const string EmbeddingAttachmentContentType = "application/octet-stream";
        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            var documentsStorage = context.DocumentDatabase.DocumentsStorage;
            var attachmentsStorage = documentsStorage.AttachmentsStorage;
            foreach (var item in work)
            {
                var docJson = CreateEmbeddingCacheDocumentJson(DateTime.UtcNow.Add(item.CacheDuration));
                using (var json = context.ReadObject(docJson, item.CacheDocumentId))
                {
                    documentsStorage.Put(context, item.CacheDocumentId, null, json);
                }

                attachmentsStorage.PutAttachment(context, item.CacheDocumentId, item.ValueHash, EmbeddingAttachmentContentType, 
                    item.EmbeddingHash, null, new ReadOnlyMemoryStream<byte>(item.EmbeddingValue));
            }

            return work.Count;
        }
        
        private DynamicJsonValue CreateEmbeddingCacheDocumentJson(DateTime expireAt)
        {
            return new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = Constants.Documents.Collections.EmbeddingsCacheCollection,
                    [Constants.Documents.Metadata.Expires] = expireAt
                }
            };
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            throw new NotSupportedException();
        }
    }

    public void HandleDatabaseRecordChange(DatabaseRecord record)
    {
        if (record is null)
            return;

        foreach (EmbeddingsGenerationConfiguration configuration in record.EmbeddingsGenerations)
        {
            var identifier = new EmbeddingsGenerationTaskIdentifier(configuration.Identifier);
            if (configuration.Disabled)
            {
                if (_workers.TryRemove(identifier, out var toDispose))
                {
                    _ = toDispose.ShutdownAsync();
                }

                continue;
            }
            
            if (_workers.TryGetValue(identifier, out var existing) is false)
            {
                _ = _workers.GetOrAdd(identifier, CreateAiWorker).RunAsync();
                continue;
            }

            var newConStr = GetConnectionString(record, configuration);

            if (existing.ModifiedFrom(configuration, newConStr))
            {
                if (_workers.TryRemove(identifier, out var toDispose))
                {
                    _ = toDispose.ShutdownAsync();
                }
                _ = _workers.GetOrAdd(identifier, CreateAiWorker).RunAsync();
            }
        }
    }

    public bool EmbeddingTaskExists(EmbeddingsGenerationTaskIdentifier id)
    {
        return _workers.ContainsKey(id);
    }
}
