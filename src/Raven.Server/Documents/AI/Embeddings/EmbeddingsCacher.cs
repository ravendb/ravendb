using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Background;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.AI.Embeddings;

public class EmbeddingsCacher : BackgroundWorkBase
{
    private DocumentDatabase _database;

    private readonly ConcurrentQueue<EmbeddingCacheItem> _embeddingsQueue;
    private readonly SemaphoreSlim _semaphore;

    private int _approxQueueLength;

    public EmbeddingsCacher(DocumentDatabase database, CancellationToken shutdown) : base(database.Name, database.Loggers.GetLogger<EmbeddingsCacher>(), shutdown)
    {
        _database = database;
        _embeddingsQueue = new ConcurrentQueue<EmbeddingCacheItem>();
        _semaphore = new SemaphoreSlim(0, 1);
    }

    protected override async Task DoWork()
    {
        while (true)
        {
            await _semaphore.WaitAsync(CancellationToken);

            var payload = new List<EmbeddingCacheItem>(_approxQueueLength);

            while (_embeddingsQueue.TryDequeue(out var item))
            {
                payload.Add(item);
                _approxQueueLength--;
            }

            var putEmbeddingsCommand = new PutEmbeddingsCommand(payload, _database);

            _database.TxMerger.EnqueueSync(putEmbeddingsCommand);
        }
    }

    public void EnqueueEmbeddingToCache(List<EmbeddingCacheItem> embeddings)
    {
        foreach (EmbeddingCacheItem item in embeddings)
        {
            _embeddingsQueue.Enqueue(item);

            _approxQueueLength++;
        }

        _semaphore.Release();
    }

    private sealed class PutEmbeddingsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>, IDisposable
    {
        private readonly List<EmbeddingCacheItem> _embeddingItems;
        private readonly DocumentDatabase _database;
        private readonly DocumentsStorage _documentsStorage;

        public PutEmbeddingsCommand(List<EmbeddingCacheItem> embeddingItems, DocumentDatabase database)
        {
            _embeddingItems = embeddingItems;
            _database = database;
            _documentsStorage = database.DocumentsStorage;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            var operationStartDate = _database.Time.GetUtcNow();

            foreach (var item in _embeddingItems)
            {
                using (var stream = new MemoryStream(item.EmbeddingValue.Span.ToArray()))
                {
                    var hash = AttachmentsStorageHelper.CalculateHash(item.EmbeddingValue.Span);

                    var valueEmbeddingsDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(item.ConnectionStringIdentifier, hash, item.Quantization);

                    var valueEmbeddingsDocumentJsonDjv = EmbeddingsStorage.CreateEmbeddingCacheDocument(operationStartDate);

                    using (var json = context.ReadObject(valueEmbeddingsDocumentJsonDjv, valueEmbeddingsDocumentId))
                    {
                        _documentsStorage.Put(context, valueEmbeddingsDocumentId, null, json);
                    }

                    _documentsStorage.AttachmentsStorage.PutAttachment(context, valueEmbeddingsDocumentId, "TODO arek", "application/octet-stream", hash, null,
                        stream);
                }
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
