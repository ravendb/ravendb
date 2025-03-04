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

public class EmbeddingsCacher : BackgroundWorkBase
{
    private readonly DocumentDatabase _database;

    private readonly ConcurrentQueue<EmbeddingGenerationItem> _embeddingsQueue;
    private readonly AsyncManualResetEvent _mre;

    public EmbeddingsCacher(DocumentDatabase database, CancellationToken shutdown) : base(database.Name, database.Loggers.GetLogger<EmbeddingsCacher>(), shutdown)
    {
        _database = database;
        _embeddingsQueue = new ConcurrentQueue<EmbeddingGenerationItem>();
        _mre = new AsyncManualResetEvent();
    }

    protected override async Task DoWork()
    {
        while (true)
        {
            await _mre.WaitAsync(CancellationToken);

            List<EmbeddingGenerationItem> payload = null;

            while (_embeddingsQueue.TryDequeue(out var item))
            {
                payload ??= new List<EmbeddingGenerationItem>();

                payload.Add(item);

                if (payload.Count >= _database.Configuration.Ai.MaxBatchSizeOfQueryGeneratedEmbeddingsToCache)
                    break;
            }

            if (payload == null || payload.Count == 0)
                return;

            var putEmbeddingsCommand = new PutEmbeddingsCommand(payload, _database);

            _database.TxMerger.EnqueueSync(putEmbeddingsCommand);
        }
    }

    public void EnqueueEmbeddingToCache(List<EmbeddingGenerationItem> embeddings)
    {
        foreach (EmbeddingGenerationItem item in embeddings)
        {
            _embeddingsQueue.Enqueue(item);
        }

        _mre.Set();
    }

    private sealed class PutEmbeddingsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>, IDisposable
    {
        private readonly List<EmbeddingGenerationItem> _embeddingItems;
        private readonly EmbeddingsStorage _embeddingsStorage;

        public PutEmbeddingsCommand(List<EmbeddingGenerationItem> embeddingItems, DocumentDatabase database)
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
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
