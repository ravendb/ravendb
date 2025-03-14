using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Background;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;

namespace Raven.Server.Documents.AI.Embeddings;

public class QueryEmbeddingsCacher : BackgroundWorkBase
{
    private readonly DocumentDatabase _database;

    private readonly ConcurrentQueue<EmbeddingGenerationItem> _embeddingsQueue;
    private readonly AsyncManualResetEvent _mre;
    private int _approximateCount;
    public QueryEmbeddingsCacher(DocumentDatabase database, CancellationToken shutdown) : base(database.Name, database.Loggers.GetLogger<QueryEmbeddingsCacher>(), shutdown)
    {
        _database = database;
        _embeddingsQueue = new ConcurrentQueue<EmbeddingGenerationItem>();
        _mre = new AsyncManualResetEvent();
    }

    protected override async Task DoWork()
    {
        while (CancellationToken.IsCancellationRequested == false)
        {
            await _mre.WaitAsync(CancellationToken);

            _mre.Reset();

            var mightBeMore = CacheEnqueuedEmbeddings();
            
            if (mightBeMore)
                _mre.Set();
        }
    }

    internal bool CacheEnqueuedEmbeddings()
    {
        var mightBeMore = false;
        
        var maxBatchSize = _database.Configuration.Ai.QueryEmbeddingsGenerationMaxCacheBatchSize;

        var payload = new List<EmbeddingGenerationItem>(Math.Min(_approximateCount, maxBatchSize));

        while (_embeddingsQueue.TryDequeue(out var item))
        {
            CancellationToken.ThrowIfCancellationRequested();
                

            payload.Add(item);

            if (payload.Count >= maxBatchSize)
            {
                mightBeMore = true;
                break;
            }
        }

        if (payload.Count == 0)
            return mightBeMore;
        
        Interlocked.Add(ref _approximateCount, -payload.Count);

        var putEmbeddingsCommand = new PutQueryEmbeddingsCommand(payload, _database);

        _database.TxMerger.EnqueueSync(putEmbeddingsCommand);

        return mightBeMore;
    }

    public void EnqueueEmbeddingsToCache(List<EmbeddingGenerationItem> embeddings)
    {
        foreach (EmbeddingGenerationItem item in embeddings)
            _embeddingsQueue.Enqueue(item);

        Interlocked.Add(ref _approximateCount, embeddings.Count);

        _mre.Set();
    }

    private sealed class PutQueryEmbeddingsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
    {
        private readonly List<EmbeddingGenerationItem> _embeddingItems;
        private readonly EmbeddingsStorage _embeddingsStorage;

        public PutQueryEmbeddingsCommand(List<EmbeddingGenerationItem> embeddingItems, DocumentDatabase database)
        {
            _embeddingItems = embeddingItems;
            _embeddingsStorage = database.AiIntegrations.Embeddings.Storage;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            foreach (var item in _embeddingItems)
            {
                _embeddingsStorage.CacheEmbedding(context, item, item.ExpireAt!.Value);
            }

            return _embeddingItems.Count;
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            throw new NotSupportedException();
        }
    }
}
