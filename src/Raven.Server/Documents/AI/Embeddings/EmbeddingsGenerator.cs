using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.IdentityModel.Protocols.Configuration;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Embeddings;
using Nito.AsyncEx;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Background;
using Raven.Server.Config;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;

namespace Raven.Server.Documents.AI.Embeddings;

#pragma warning disable SKEXP0001

public class EmbeddingsGenerator(DocumentDatabase database, RavenLogger logger, CancellationToken shutdown, EmbeddingsGenerator.Mode mode) : BackgroundWorkBase(database.Name, logger, shutdown)
{
    private const string EmbeddingAttachmentContentType = "application/octet-stream";
    private readonly Mode _mode = mode;
    private readonly ConcurrentDictionary<EmbeddingsGenerationTaskIdentifier, AiWorker> _workers = [];
    private readonly ConcurrentQueue<IEmbeddingsCommand> _work = new();
    private readonly AsyncManualResetEvent _hasWork = new();
    private readonly RavenLogger _logger = logger;
    private readonly DocumentDatabase _database = database;

    public enum Mode
    {
        Query,
        Etl,
    }

    private interface IEmbeddingsCommand;

    private record RefreshCache(List<string> DocumentIds, TimeSpan CacheDuration) : IEmbeddingsCommand;
    
    private record StoreEmbeddings(List<GenerateEmbeddings> GeneratedEmbeddings, VectorEmbeddingType Quantization): IEmbeddingsCommand;

    private record PutDocumentEmbeddings(
        string TaskId,
        Dictionary<string, HashSet<GenerateEmbeddings>> Data,
        string DocumentId,
        string Collection,
        VectorEmbeddingType Quantization
    );

    private record GenerateEmbeddings(
        AiConnectionStringIdentifier ConnectionStringId,
        TimeSpan CacheDuration, 
        List<string> Values,
        List<ReadOnlyMemory<byte>> Embeddings,
        string CacheKey,
        AiWorker Owner
        )
    {
        public readonly TaskCompletionSource TaskCompletionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private class AiWorker
    {
        private readonly DocumentsStorage _documentsStorage;
        public readonly EmbeddingsGenerationConfiguration Configuration;
        private readonly AiConnectionString _connectionString;
        private readonly ITextEmbeddingGenerationService _embeddingGenerationService;
        private readonly CancellationToken _cancellationToken;
        private readonly AsyncManualResetEvent _hasWork = new();
        private readonly ConcurrentQueue<GenerateEmbeddings> _work = new();
        private readonly ConcurrentDictionary<string, GenerateEmbeddings> _inFlightCache = new(StringComparer.OrdinalIgnoreCase);
        private readonly Task[] _tasks;
        private readonly EmbeddingsGenerator _parent;
        private readonly CancellationTokenSource _shutdown;
        private int _taskIsRunning;
        private readonly AiConnectionStringIdentifier _connectionStringIdentifier;
        private readonly int? _maxBatchSize;

        public AiWorker(EmbeddingsGenerator parent, DocumentsStorage documentsStorage, EmbeddingsGenerationConfiguration configuration,
            AiConnectionString connectionString, int maxConcurrentBatches, CancellationToken cancellationToken)
        {
            _documentsStorage = documentsStorage;
            _maxBatchSize = documentsStorage.DocumentDatabase.Configuration.Ai.EmbeddingsGenerationMaxBatchSize;
            _parent = parent;
            Configuration = configuration;
            _connectionString = connectionString;
            _connectionStringIdentifier = new AiConnectionStringIdentifier(_connectionString.Identifier);
            _embeddingGenerationService = AiHelper.CreateService(connectionString);
            _shutdown = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _cancellationToken = _shutdown.Token;
            var shutdownTask = new TaskCompletionSource();
            _cancellationToken.Register(_ => shutdownTask.TrySetCanceled(), null);
            _tasks = new Task[maxConcurrentBatches + 1];
            Array.Fill(_tasks, Task.CompletedTask);
            _tasks[^1] = shutdownTask.Task;
        }
        
         public PutDocumentEmbeddings GenerateEmbeddingAsync(DocumentsOperationContext documentsContext,
             string sourceDocumentId, string sourceCollectionName,
             HashSet<Task> tasks,
             Reference<int> cachedEmbeddings,
             Dictionary<string, List<(string,ChunkingOptions)>> props)
         {
             Dictionary<string, HashSet<GenerateEmbeddings>> embeddingsByName = new();
             var cacheDuration = Configuration.EmbeddingsCacheExpiration;
             List<string> expirationRefresh = null;
             foreach (var (name, field) in props)
             {
                 HashSet<GenerateEmbeddings> hashes = [];
                 foreach (var  (value,chunking) in field)
                 {
                     List<string> pending = [];
                     List<ReadOnlyMemory<byte>> cachedEmbeddingsBuffers = [];
                     foreach (var chunkedValue in TextChunker.Chunk(value, chunking))
                     {
                         if (TryGetFromCache(documentsContext, chunkedValue, cacheDuration, ref expirationRefresh, out var cachedEntry))
                         {
                             cachedEmbeddingsBuffers.Add(cachedEntry);
                             cachedEmbeddings.Value++;
                         }
                         else
                         {
                             pending.Add(chunkedValue);
                         }
                     }
                     var generateEmbeddings = RegisterPendingEmbeddings(pending, cachedEmbeddingsBuffers, value, cacheDuration);
                     tasks.Add(generateEmbeddings.TaskCompletionSource.Task);
                     hashes.Add(generateEmbeddings);
                 }
                 embeddingsByName[name] = hashes;
             }
             
             if (expirationRefresh is not null)
             {
                 _parent.ProcessInBackground(new RefreshCache(expirationRefresh, cacheDuration));
             }

             return new PutDocumentEmbeddings(
                 Configuration.Identifier,
                 embeddingsByName,
                 sourceDocumentId,
                 sourceCollectionName,
                  Configuration.Quantization
             );
         }


        public ValueTask<ReadOnlyMemory<ReadOnlyMemory<byte>>> GetEmbeddingsForQueryAsync(
            DocumentsOperationContext documentsContext, 
            string value)
        {
            List<ReadOnlyMemory<byte>> results = [];
            List<string> expirationRefresh = null;
            List<string> pending = null;
            // we explicitly do *not* care about the order of vectors compared to the text, including with chunking or 
            // with multiple values. Logically, we send text, and get a set of vectors back, in some arbitrary order
            foreach (var text in TextChunker.Chunk(value, Configuration.ChunkingOptionsForQuerying))
            {
                if (TryGetFromCache(documentsContext, text, Configuration.EmbeddingsCacheForQueryingExpiration, ref expirationRefresh,
                        out ReadOnlyMemory<byte> cached))
                {
                    results.Add(cached);
                }
                else
                {
                    pending ??= [];
                    pending.Add(text);
                }
            }

            var cacheDuration = Configuration.EmbeddingsCacheForQueryingExpiration;
            if (expirationRefresh is not null)
            {
                _parent.ProcessInBackground(new RefreshCache(expirationRefresh, cacheDuration));
            }

            if (pending is null)
                return ValueTask.FromResult(new ReadOnlyMemory<ReadOnlyMemory<byte>>(results.ToArray()));

            var generateEmbeddings = RegisterPendingEmbeddings(pending, results, value, cacheDuration);
            return new (ReturnAsync());

            async Task<ReadOnlyMemory<ReadOnlyMemory<byte>>> ReturnAsync()
            {
                Wake();
                await generateEmbeddings.TaskCompletionSource.Task;
                return new ReadOnlyMemory<ReadOnlyMemory<byte>>(generateEmbeddings.Embeddings.ToArray());
            }
        }
        
        GenerateEmbeddings RegisterPendingEmbeddings(List<string> pending, List<ReadOnlyMemory<byte>> cachedEmbeddings, string value, TimeSpan cacheDuration)
        {
            var newGen = new GenerateEmbeddings(_connectionStringIdentifier, cacheDuration, pending,  cachedEmbeddings,value, this);
            if (pending.Count == 0) // all from the cache
            {
                newGen.TaskCompletionSource.TrySetResult();
                return newGen;
            }

            var inCacheGen = _inFlightCache.GetOrAdd(value, newGen);
            if (newGen == inCacheGen)
            {
                _work.Enqueue(newGen);
            }
            return inCacheGen;
        }

        public void RemoveFromCache(string value)
        {
            _inFlightCache.TryRemove(value, out _);   
        }
        
        private bool TryGetFromCache(DocumentsOperationContext documentsContext, string text,
            TimeSpan cacheDuration, ref List<string> expirationRefresh, out ReadOnlyMemory<byte> result)
        {
            var valueHash = EmbeddingsHelper.CalculateInputValueHash(text);
            var docId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(_connectionStringIdentifier, valueHash, Configuration.Quantization);
            
            Attachment attachment = _documentsStorage.AttachmentsStorage.GetAttachment(documentsContext, docId, valueHash, AttachmentType.Document, null);
            if (attachment == null)
            {
                result = default;
                return false;
            }

            using var document = _documentsStorage.Get(documentsContext, docId);
            if (document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
            {
                if (metadata.TryGet(Constants.Documents.Metadata.Expires, out DateTime expires))
                {
                    var timeToExpireSeconds = (expires - _parent._database.Time.GetUtcNow()).TotalSeconds;
                    var halfTime = cacheDuration.TotalSeconds / 2;
                    if (halfTime > timeToExpireSeconds)
                    {
                        expirationRefresh ??= [];
                        expirationRefresh.Add(docId);
                    }
                }
            }

            var stream = attachment.Stream;
            //TODO: Need to find a way to avoid this allocation in favor of pooling, etc.
            byte[] buffer = new byte[attachment.Size];
            stream.ReadExactly(buffer);
            result = buffer;

            return true;
        }

        private ValueTask<int> GetAvailableTaskIndexAsync()
        {
            for (int i = 0; i < _tasks.Length; i++)
            {
                if (_tasks[i].IsCompleted)
                    return ValueTask.FromResult(i);
            }

            return new(WithAsyncWait());

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
                while (_work.TryDequeue(out var work))
                {
                    work.TaskCompletionSource.SetCanceled(_shutdown.Token);
                }

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

            while (_cancellationToken.IsCancellationRequested == false)
            {
                await _hasWork.WaitAsync(_cancellationToken);
                _hasWork.Reset();

                List<string> batch = [];
                List<List<ReadOnlyMemory<byte>>> embeddings = [];
                List<GenerateEmbeddings> works = [];

                while (_work.TryDequeue(out var work))
                {
                    for (int i = 0; i < work.Values.Count; i++)
                    {
                        batch.Add(work.Values[i]);
                        // we add the embedding list multiple times, to make it
                        // easier to track which results list each value belongs to
                        embeddings.Add(work.Embeddings);
                    }

                    works.Add(work);
                    if (batch.Count >= _maxBatchSize)
                    {
                        Wake();
                        break;
                    }
                }

                if (works.Count is 0)
                    continue;

                // GetAvailableTaskIndexAsync is ensuring that we aren't running
                // too many concurrent tasks, while the actual batch itself
                // is running in the background
                int index = await GetAvailableTaskIndexAsync();
                _tasks[index] = FlushBatchAsync(batch, embeddings, works);
            }
        } 
        private async Task FlushBatchAsync(List<string> batch, List<List<ReadOnlyMemory<byte>>> embeddings, List<GenerateEmbeddings> works)
        {
            try
            {
                IList<ReadOnlyMemory<float>> allEmbeddings;

                try
                {
                    allEmbeddings = await AiHelper.GenerateEmbeddingsAsync(_embeddingGenerationService, batch, _cancellationToken);
                }
                catch (HttpOperationException httpOperationException) when (httpOperationException.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    throw new EmbeddingGenerationException(
                        $"Failed to generate embeddings due to rate limits. Consider decreasing the number of elements processed in a single batch " +
                        $"('{RavenConfiguration.GetKey(x => x.Ai.EmbeddingsGenerationMaxBatchSize)}') or increasing the " +
                        $"limits on your model deployment.", httpOperationException);
                }


                PortableExceptions.ThrowIf<IOException>(allEmbeddings.Count != batch.Count, "Model returned a different count of embeddings than expected");

                for (int i = 0; i < allEmbeddings.Count; i++)
                {
                    embeddings[i].Add(EmbeddingsHelper.CreateEmbeddingValue(allEmbeddings[i], Configuration.Quantization));
                }

                if (_parent._mode is Mode.Query)
                {
                    // for queries we want to release the thread ASAP and send it the embeddings
                    foreach (var work in works)
                    {
                        work.TaskCompletionSource.TrySetResult();
                        // explicitly *not* calling this here, we have a value
                        // but we haven't persisted that yet
                        // work.Owner.RemoveFromCache(ge.CacheKey);
                    }
                }
                _parent.ProcessInBackground(new StoreEmbeddings(works, Configuration.Quantization));
            }
            catch (Exception e)
            {
                foreach (var work in works)
                {
                    work.TaskCompletionSource.TrySetException(e);
                    work.Owner.RemoveFromCache(work.CacheKey);
                }
            }
        }

        public bool ModifiedFrom(EmbeddingsGenerationConfiguration updated, AiConnectionString updateConnectionString)
        {
            return Configuration.Compare(updated) != EtlConfigurationCompareDifferences.None ||
                   _connectionString.Compare(updateConnectionString) != AiSettingsCompareDifferences.None;
        }

        public void Wake()
        {
            _hasWork.Set();
        }
    }
    
    private void ProcessInBackground(IEmbeddingsCommand works)
    {
        _work.Enqueue(works);
        _hasWork.Set();
    }
    private AiWorker CreateAiWorker(EmbeddingsGenerationTaskIdentifier id)
    {
        var record = _database.ReadDatabaseRecord();
        foreach (var task in record.EmbeddingsGenerations)
        {
            if (task.Disabled)
                throw new InvalidOperationException($"The task {id.Value} has been disabled and cannot be used");
            
            if (string.Equals(id.Value, task.Identifier, StringComparison.OrdinalIgnoreCase))
            {
                var connectionString = GetConnectionString(record, task);
                int maxConcurrentBatches = connectionString.GetQueryEmbeddingsMaxConcurrentBatches(_database.Configuration.Ai.EmbeddingsMaxConcurrentBatches);
                if (maxConcurrentBatches > 0) 
                    return new AiWorker(this, _database.DocumentsStorage, task, connectionString, maxConcurrentBatches, CancellationToken);
                
                string message = $"{RavenConfiguration.GetKey(x => x.Ai.EmbeddingsMaxConcurrentBatches)} must be a positive value: {connectionString.Identifier}";
                throw new InvalidConfigurationException(message);
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
        var record = _database.ReadDatabaseRecord();
        HandleDatabaseRecordChange(record);
    }

    protected override async Task DoWork()
    {
        try
        {
            while (CancellationToken.IsCancellationRequested == false)
            {
                await _hasWork.WaitAsync(CancellationToken);
                _hasWork.Reset();
                await SubmitAndWaitForWorkAsync(GetBatch());
            }
        }
        finally
        {
            List<Task> tasks = [];
            foreach (var (_, state)  in _workers)
            {
                tasks.Add(state.ShutdownAsync());
            }

            try
            {
                // we wait for a bit for all the tasks to complete before
                // we are done
                Task.WaitAll(tasks.ToArray(), 15_000);
            }
            catch
            {
                // we don't care about an error here
            }
            
            while (_work.TryDequeue(out IEmbeddingsCommand o))
            {
                if (o is not StoreEmbeddings se) 
                    continue;
                
                foreach (GenerateEmbeddings ge in se.GeneratedEmbeddings)
                {
                    ge.TaskCompletionSource.TrySetCanceled();
                }
            }
        }
    }

    private async Task SubmitAndWaitForWorkAsync(List<IEmbeddingsCommand> batch)
    {
        try
        {
            await _database.TxMerger.Enqueue(new PutEmbeddingsIntoCacheCommand(batch));

            foreach (var work in batch)
            {
                switch (work)
                {
                    case BatchGenerator pde:
                    {
                        pde.TaskCompletionSource.TrySetResult();
                        break;
                    }
                    case StoreEmbeddings se:
                    {
                        foreach (var ge in se.GeneratedEmbeddings)
                        {
                            ge.TaskCompletionSource.TrySetResult();
                            ge.Owner.RemoveFromCache(ge.CacheKey);
                        }

                        break;
                    }
                    case RefreshCache:
                        // nothing to do here
                        break;
                    
                    // here we have invalid states, we are throwing here 
                    case null:
                        throw new ArgumentNullException();
                    default:
                        throw new ArgumentOutOfRangeException(work.GetType().FullName);
                }
            }
        }
        catch (Exception e)
        {
            if (_logger.IsWarnEnabled)
            {
                _logger.Warn($"Failed to submit embeddings to cache", e);
            }
            foreach (var work in batch)
            {
                switch (work)
                {
                    case BatchGenerator pde:
                    {
                        pde.TaskCompletionSource.TrySetException(e);
                        break;
                    }
                    case StoreEmbeddings se:
                    {
                        foreach (var ge in se.GeneratedEmbeddings)
                        {
                            ge.TaskCompletionSource.TrySetException(e);
                            ge.Owner.RemoveFromCache(ge.CacheKey);
                        }

                        break;
                    }
                }
            }
        }
    }

    private List<IEmbeddingsCommand> GetBatch()
    {
        List<IEmbeddingsCommand> results = [];
        while (_work.TryDequeue(out IEmbeddingsCommand o))
        {
            results.Add(o);
            if (results.Count > 128)
            {
                _hasWork.Set();
                break;
            }
        }

        return results;
    }

    private sealed class PutEmbeddingsIntoCacheCommand(List<IEmbeddingsCommand> batch)
        : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
    {
        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            int operations = 0;
            var documentsStorage = context.DocumentDatabase.DocumentsStorage;
            var attachmentsStorage = documentsStorage.AttachmentsStorage;
            DateTime now = context.DocumentDatabase.Time.GetUtcNow();
            foreach (var cur in batch)
            {
                switch (cur)
                {
                    case RefreshCache rc:
                    {
                        DateTime expireAt = now.Add(rc.CacheDuration);
                        foreach (string docIdToRefresh in rc.DocumentIds)
                        {
                            var docJson = CreateEmbeddingCacheDocumentJson(expireAt);
                            using (var json = context.ReadObject(docJson, docIdToRefresh))
                            {
                                operations++;
                                documentsStorage.Put(context, docIdToRefresh, null, json);
                            }
                        }

                        break;
                    }
                    case BatchGenerator bg:
                    {
                        operations += bg.ApplyInTransaction(context);
                        break;
                    }
                    case StoreEmbeddings se:
                    {
                        foreach (var ge in se.GeneratedEmbeddings)
                        {
                            DateTime expireAt = now.Add(ge.CacheDuration);
                            for (int i = 0; i < ge.Values.Count; i++)
                            {
                                var val = ge.Values[i];
                                var embedding = ge.Embeddings[i];
                            
                                var docJson = CreateEmbeddingCacheDocumentJson(expireAt);
                                var valueHash = EmbeddingsHelper.CalculateInputValueHash(val);
                                var docId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(ge.ConnectionStringId, valueHash, se.Quantization);

                                using (var json = context.ReadObject(docJson, docId))
                                {
                                    documentsStorage.Put(context, docId, null, json);
                                }

                                string embeddingHash = AttachmentsStorageHelper.CalculateHash(embedding.Span);
                                attachmentsStorage.PutAttachment(context, docId, valueHash, EmbeddingAttachmentContentType,
                                    embeddingHash, null, new ReadOnlyMemoryStream<byte>(embedding));
                                operations++;
                            }
                        }
                        break;
                    }
                    case null:
                        throw new ArgumentNullException();
                    default:
                        throw new ArgumentOutOfRangeException(cur.GetType().FullName);
                }
            }
            return operations; 
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
            return new Replay(batch);
        }

        public class Replay(List<IEmbeddingsCommand> work) : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>>
        {
            public MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction> ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                return new PutEmbeddingsIntoCacheCommand(work);
            }
        }
    }
    
    public void HandleDatabaseRecordChange(DatabaseRecord record)
    {
        if (record is null)
            return;

        HashSet<EmbeddingsGenerationTaskIdentifier> taskIdsToRetain = [];

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

            taskIdsToRetain.Add(identifier);

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

        foreach (var taskIdToRemove in _workers.Keys.Except(taskIdsToRetain))
        {
            if (_workers.TryRemove(taskIdToRemove, out var toDispose))
            {
                _ = toDispose.ShutdownAsync();
            }
        }
    }

    public bool EmbeddingTaskExists(EmbeddingsGenerationTaskIdentifier id)
    {
        return _workers.ContainsKey(id);
    }

    public VectorEmbeddingType GetQuantizationOf(EmbeddingsGenerationTaskIdentifier taskId)
    {
        if(_workers.TryGetValue(taskId, out var worker)is false)
            return VectorEmbeddingType.Single;
        return worker.Configuration.Quantization;
    }
    
    public ReadOnlyMemory<ReadOnlyMemory<byte>> GetEmbeddingsForQuery(
        DocumentsOperationContext documentsContext,
        EmbeddingsGenerationTaskIdentifier taskId,
        params ReadOnlySpan<string> values)
    {
        var valueTask = GetEmbeddingsForQueryAsync(documentsContext, taskId, values);
        if(valueTask.IsCompletedSuccessfully)
            return valueTask.Result;
        
        return valueTask.AsTask().GetAwaiter().GetResult();
    }

    public ValueTask<ReadOnlyMemory<ReadOnlyMemory<byte>>> GetEmbeddingsForQueryAsync(
        DocumentsOperationContext documentsContext,
        EmbeddingsGenerationTaskIdentifier taskId,
        params ReadOnlySpan<string> values)
    {
        if(_workers.TryGetValue(taskId, out var worker) is false)
        {
            throw new InvalidQueryException($"Couldn't find Embeddings Generation task with '{taskId.Value}' identifier");
        }

        if (values.Length == 1)
        {
            return worker.GetEmbeddingsForQueryAsync(documentsContext, values[0]);
        }

        return GetEmbeddingsArrayAsync(values);

        ValueTask<ReadOnlyMemory<ReadOnlyMemory<byte>>> GetEmbeddingsArrayAsync(ReadOnlySpan<string> multi)
        {
            List<ReadOnlyMemory<byte>> results = [];
            List<Task<ReadOnlyMemory<ReadOnlyMemory<byte>>>> tasks = null;
            for (int i = 0; i < multi.Length; i++)
            {
                var valueTask = worker.GetEmbeddingsForQueryAsync(documentsContext, multi[i]);
                if (valueTask.IsCompleted)
                {
                    results.AddRange(valueTask.Result.Span);
                }
                else
                {
                    tasks ??= [];
                    tasks.Add(valueTask.AsTask());
                }
            }

            if (tasks is null)
                return new(results.ToArray());

            return new(CompleteAsync());

            async Task<ReadOnlyMemory<ReadOnlyMemory<byte>>> CompleteAsync()
            {
                var asyncResults = await Task.WhenAll(tasks);
                foreach (var result in asyncResults)
                {
                    results.AddRange(result.Span);
                }

                return results.ToArray();
            }
        }
    }

    public class BatchGenerator(EmbeddingsGenerator parent, EmbeddingsGenerationTaskIdentifier taskId) : IEmbeddingsCommand
    {
        private readonly AiWorker _worker = parent._workers[taskId];
        private readonly HashSet<Task> _tasks = [];
        public readonly TaskCompletionSource TaskCompletionSource = new();
        private readonly List<PutDocumentEmbeddings> _results = [];
        private readonly Reference<int> _cachedEmbeddings = new();
        private readonly List<string> _toDelete = [];
        public int CachedEmbeddings => _cachedEmbeddings.Value;

        public void StartGenerateEmbeddingFor(
            DocumentsOperationContext documentsContext,
            string sourceDocumentId, string sourceCollectionName,
            Dictionary<string, List<(string,ChunkingOptions)>> props)
        {
            var putDocumentEmbeddings =
                _worker.GenerateEmbeddingAsync(documentsContext, sourceDocumentId, sourceCollectionName, _tasks, _cachedEmbeddings, props);
            _results.Add(putDocumentEmbeddings);
        }

        public async Task WaitForGenerationAsync()
        {
            _worker.Wake();
            await Task.WhenAll(_tasks);
        }

        public Task StoreResults()
        {
            parent.ProcessInBackground(this);
            return TaskCompletionSource.Task;
        }

        public int ApplyInTransaction(DocumentsOperationContext context)
        {
            int operations = _toDelete.Count;
            var documentsStorage = context.DocumentDatabase.DocumentsStorage;
            var attachmentsStorage = context.DocumentDatabase.DocumentsStorage.AttachmentsStorage;
            foreach (var del in _toDelete)
            {
                operations++;
                var embeddingDocId = EmbeddingsHelper.GetEmbeddingDocumentId(del);
                documentsStorage.Delete(context, embeddingDocId, null);
            }

            foreach (var pde in _results)
            {
                var embeddingDocId = EmbeddingsHelper.GetEmbeddingDocumentId(pde.DocumentId);
                Dictionary<string, HashSet<string>> hashesByName = [];
                Dictionary<string, ReadOnlyMemory<byte>> attachments = [];
                foreach (var (name, embeddings) in pde.Data)
                {
                    HashSet<string> hashes = [];
                    foreach (var generatedEmbeddings in embeddings)
                    {
                        foreach (var embedding in generatedEmbeddings.Embeddings)
                        {
                            string embeddingHash = AttachmentsStorageHelper.CalculateHash(embedding.Span);
                            hashes.Add(embeddingHash);
                            attachments[embeddingHash] = embedding;
                        }
                    }

                    hashesByName[name] = hashes;
                }

                using var updatedDoc = CreateOrUpdateDocumentEmbeddingDoc(embeddingDocId, pde, hashesByName, out var attachmentsToRemove);
                documentsStorage.Put(context, embeddingDocId, null, updatedDoc);
                foreach (var (embeddingHash, embedding) in attachments)
                {
                    operations++;
                    attachmentsStorage.PutAttachment(context, embeddingDocId, embeddingHash, EmbeddingAttachmentContentType,
                        embeddingHash, null, new ReadOnlyMemoryStream<byte>(embedding));
                }

                foreach (var toRemove in attachmentsToRemove)
                {
                    operations++;
                    attachmentsStorage.DeleteAttachment(context, embeddingDocId, toRemove, null, out _);
                }
            }

            return operations;

            BlittableJsonReaderObject CreateOrUpdateDocumentEmbeddingDoc(string embeddingDocId, PutDocumentEmbeddings pde, Dictionary<string, HashSet<string>> hashesByName, out HashSet<string> attachmentsToRemove)
            {
                using Document document = documentsStorage.Get(context, embeddingDocId);
                DynamicJsonValue modifications;
                attachmentsToRemove = [];
                if (document != null)
                {
                    ExtractAllAttachmentsForCurrentTask(document, pde, attachmentsToRemove);
                    modifications = new DynamicJsonValue(document.Data);
                }
                else
                {
                    modifications = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue()
                        {
                            [Constants.Documents.Metadata.Collection] = EmbeddingsHelper.GetEmbeddingDocumentCollectionName(pde.Collection),
                        }
                    };
                }

                var djv = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Quantization] = pde.Quantization
                };
                foreach (var (name, hashes) in hashesByName)
                {
                    attachmentsToRemove.ExceptWith(hashes);
                    djv[name] = new DynamicJsonArray(hashes);
                }
                if (document != null)
                {
                    RetainAttachmentsFromOtherTasks(document.Data, pde, attachmentsToRemove);
                }
                modifications[pde.TaskId] = djv;
                return document == null ?
                    context.ReadObject(modifications, embeddingDocId) :
                    context.ReadObject(document.Data, embeddingDocId);
            }

            void ExtractAllAttachmentsForCurrentTask(Document document, PutDocumentEmbeddings pde, HashSet<string> attachmentsToRemove)
            {
                if (!document.Data.TryGet(pde.TaskId, out BlittableJsonReaderObject taskDetails))
                    return;

                BlittableJsonReaderObject.PropertyDetails prop = default;
                for (int i = 0; i < taskDetails.Count; i++)
                {
                    taskDetails.GetPropertyByIndex(i, ref prop);
                    if (prop.Value is not BlittableJsonReaderArray arr)
                        continue;

                    for (int j = 0; j < arr.Length; j++)
                    {
                        var hash = arr.GetStringByIndex(j);
                        if (hash is null)
                            continue;

                        attachmentsToRemove.Add(hash);
                    }
                }
            }

            void RetainAttachmentsFromOtherTasks(BlittableJsonReaderObject doc, PutDocumentEmbeddings pde, HashSet<string> attachmentsToRemove)
            {
                BlittableJsonReaderObject.PropertyDetails prop = default;
                for (int i = 0; i < doc.Count; i++)
                {
                    doc.GetPropertyByIndex(i, ref prop);
                    // we skip the _current_ task, since we update all its attachemnts
                    if (string.Equals(prop.Name, pde.TaskId, StringComparison.OrdinalIgnoreCase))
                        continue;
                    // handles the case of not an object here, can just skip it
                    if (prop.Token.HasFlag(BlittableJsonToken.StartObject) is false)
                        continue;

                    var taskDetails = (BlittableJsonReaderObject)prop.Value;
                    for (int j = 0; j < taskDetails.Count; j++)
                    {
                        taskDetails.GetPropertyByIndex(j, ref prop);
                        if (prop.Value is not BlittableJsonReaderArray arr)
                            continue;

                        for (int k = 0; k < arr.Length; k++)
                        {
                            var (val, type) = arr.GetValueTokenTupleByIndex(k);
                            if (type != BlittableJsonToken.String)
                                continue;

                            attachmentsToRemove.Remove(val.ToString());
                        }
                    }

                }
            }
        }

    public void Delete(string documentId)
        {
            _toDelete.Add(documentId);
        }
    }

    public BatchGenerator BatchFor(EmbeddingsGenerationTaskIdentifier taskId) => new (this, taskId);
}
