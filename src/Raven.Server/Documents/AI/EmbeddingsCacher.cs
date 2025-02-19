using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Background;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.AI;

public class EmbeddingsCacher : BackgroundWorkBase
{
    //private TimeSpan _cachingInterval = TimeSpan.FromMinutes(5);
    private TimeSpan _cachingInterval = TimeSpan.FromSeconds(15);
    
    private DocumentDatabase _database;
    
    private readonly ConcurrentQueue<EmbeddingCacheItem> _embeddingsQueue;
    private readonly SemaphoreSlim _semaphore;
    
    public EmbeddingsCacher(DocumentDatabase database, RavenLogger logger, CancellationToken shutdown) : base(database.Name, logger, shutdown)
    {
        _database = database;
        _embeddingsQueue = new ConcurrentQueue<EmbeddingCacheItem>();
        _semaphore = new SemaphoreSlim(0, 1);
        Start();
    }

    protected override async Task DoWork()
    {
        await WaitOrThrowOperationCanceled(_cachingInterval);
        
        await CacheEmbeddings();
    }
    
    public void EnqueueEmbeddingToCache(string connectionStringName, string textualValue, ReadOnlyMemory<float> embedding)
    {
        var newItem = new EmbeddingCacheItem() { EmbeddingValue = embedding, TextualValue = textualValue, ConnectionStringName = connectionStringName };
        
        _embeddingsQueue.Enqueue(newItem);
        _semaphore.Release();
    }
    
    private async Task CacheEmbeddings()
    {
        await _semaphore.WaitAsync(CancellationToken);
        
        var payload = new List<EmbeddingCacheItem>();
        
        while (_embeddingsQueue.TryDequeue(out var item))
            payload.Add(item);
        
        var putEmbeddingsCommand = new MergedCacheEmbeddingsCommand(payload, _database);
        
        _database.TxMerger.EnqueueSync(putEmbeddingsCommand);
    }

    private sealed class MergedCacheEmbeddingsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>, IDisposable
    {
        private List<EmbeddingCacheItem> _embeddingItems;
        private readonly DocumentDatabase _database;
        private readonly DocumentsStorage _documentsStorage;
        
        public MergedCacheEmbeddingsCommand(List<EmbeddingCacheItem> embeddingItems, DocumentDatabase database)
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
                string attachmentName = Guid.NewGuid().ToString();

                using (var stream = new MemoryStream(MemoryMarshal.Cast<float, byte>(item.EmbeddingValue.Span).ToArray()))
                {
                    var hash = AttachmentsStorageHelper.CalculateHash(context, stream);

                    var valueEmbeddingsDocumentId = AiHelper.GetValueEmbeddingsDocumentId(item.ConnectionStringName, hash);
                    
                    var valueEmbeddingsDocumentJsonDjv = AiStorage.CreateValueEmbeddingsDocument(item.TextualValue, attachmentName, operationStartDate);
                    
                    using (var json = context.ReadObject(valueEmbeddingsDocumentJsonDjv, valueEmbeddingsDocumentId))
                    {
                        _documentsStorage.Put(context, valueEmbeddingsDocumentId, null, json);
                    }
                    
                    _documentsStorage.AttachmentsStorage.PutAttachment(context, valueEmbeddingsDocumentId, attachmentName, "application/octet-stream", hash, null,
                        stream);
                }
            }
            
            return 1;
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
    
    public class EmbeddingCacheItem
    {
        public ReadOnlyMemory<float> EmbeddingValue;
        public string TextualValue;
        // Name of the connection string used for embedding generation 
        public string ConnectionStringName;
    }
}
