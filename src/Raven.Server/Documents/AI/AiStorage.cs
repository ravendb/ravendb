using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using JetBrains.Annotations;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Exceptions.Documents.Attachments;
using Raven.Client.ServerWide;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.AI;

public class AiStorage
{
    private const string EmbeddingAttachmentContentType = "application/octet-stream";

    private readonly DocumentsStorage _documentsStorage;
    private static DocumentDatabase _database;
    
#pragma warning disable SKEXP0001
    private Dictionary<string, ITextEmbeddingGenerationService> _servicesByTaskName;
    private Dictionary<string, ITextEmbeddingGenerationService> _servicesByConnectionStringName;
#pragma warning restore SKEXP0001
    private static Dictionary<string, string> _taskNameToNormalizedConnectionStringName;
    
    private static ConcurrentQueue<EmbeddingCacheItem> _embeddingsQueue;
    
    public AiStorage([NotNull] DocumentDatabase database)
    {
        _database = database;
        _documentsStorage = database.DocumentsStorage ?? throw new ArgumentNullException(nameof(_documentsStorage));
#pragma warning disable SKEXP0001
        _servicesByConnectionStringName = new Dictionary<string, ITextEmbeddingGenerationService>();
        _servicesByTaskName = new Dictionary<string, ITextEmbeddingGenerationService>();
#pragma warning restore SKEXP0001
        
        _embeddingsQueue = new ConcurrentQueue<EmbeddingCacheItem>();
        _taskNameToNormalizedConnectionStringName = new Dictionary<string, string>();
    }

#pragma warning disable SKEXP0001
    public bool TryGetServiceByTaskName(string taskName, out ITextEmbeddingGenerationService service)
#pragma warning restore SKEXP0001
    {
        return _servicesByTaskName.TryGetValue(taskName, out service);
    }

    public Document GetDocumentEmbeddings(DocumentsOperationContext context, string sourceDocumentId, out string documentEmbeddingsId)
    {
        documentEmbeddingsId = AiHelper.GetDocumentEmbeddingsId(sourceDocumentId);

        var document = _documentsStorage.Get(context, documentEmbeddingsId);
        
        return document;
    }

    public ValueEmbeddingsDocument GetValueEmbeddingsDocument(DocumentsOperationContext context, AiIntegrationConfiguration configuration, string value,
        out string valueEmbeddingsDocumentId)
    {
        valueEmbeddingsDocumentId = AiHelper.GetValueEmbeddingsDocumentId(configuration.NormalizedConnectionName, AiHelper.CalculateValueHash(value));
        
        var valueEmbeddingsDocument = GetValueEmbeddingsDocument(context, valueEmbeddingsDocumentId);

        return valueEmbeddingsDocument;
    }

    private ValueEmbeddingsDocument GetValueEmbeddingsDocument(DocumentsOperationContext context, string documentId)
    {
        var document = _documentsStorage.Get(context, documentId);
        if (document == null)
            return null;

        return new ValueEmbeddingsDocument(document);
    }

    public string AddOrUpdateValueEmbeddingsDocument(DocumentsOperationContext context, AiIntegrationEmbeddingItemValue item, DateTime lastModified)
    {
        Debug.Assert((item.EmbeddingValue.IsEmpty && item.ValueEmbeddingsSourceAttachmentName != null) ||
                     (item.EmbeddingValue.IsEmpty == false && item.ValueEmbeddingsSourceAttachmentName == null));

        var document = GetValueEmbeddingsDocument(context, item.ValueEmbeddingsDocumentId);
        string attachmentName = item.ValueEmbeddingsSourceAttachmentName ?? Guid.NewGuid().ToString();

        if (item.EmbeddingValue.IsEmpty == false)
        {
            if (document == null)
            {
                var djv = CreateValueEmbeddingsDocument(item.TextualValue, attachmentName, lastModified);

                using (var json = context.ReadObject(djv, item.ValueEmbeddingsDocumentId))
                    PutValueEmbeddingsDocumentFromEmbeddingValue(json, item.EmbeddingValue, attachmentName);

                return attachmentName;
            }

            document.Inner.Data.Modifications = new DynamicJsonValue(document.Inner.Data) { [item.TextualValue] = attachmentName };

            using (var json = context.ReadObject(document.Inner.Data, item.ValueEmbeddingsDocumentId))
                PutValueEmbeddingsDocumentFromEmbeddingValue(json, item.EmbeddingValue, attachmentName);

            return attachmentName;
        }

        var attachment = _documentsStorage.AttachmentsStorage.GetAttachment(context, item.ValueEmbeddingsDocumentId, attachmentName, AttachmentType.Document, null);
        if (attachment == null)
            AttachmentDoesNotExistException.ThrowFor(item.ValueEmbeddingsDocumentId, attachmentName);

        if (document == null)
        {
            var djv = CreateValueEmbeddingsDocument(item.TextualValue, attachmentName, lastModified);

            using (var json = context.ReadObject(djv, item.ValueEmbeddingsDocumentId))
                PutValueEmbeddingsDocumentFromAttachment(json, attachment, attachmentName);

            return attachmentName;
        }

        if (document.Inner.Data.TryGet(item.TextualValue, out attachmentName) == false || attachment.Name != attachmentName)
        {
            document.Inner.Data.Modifications = new DynamicJsonValue(document.Inner.Data) { [item.TextualValue] = attachment.Name };

            using (var json = context.ReadObject(document.Inner.Data, item.ValueEmbeddingsDocumentId))
                PutValueEmbeddingsDocumentFromEmbeddingValue(json, item.EmbeddingValue, attachmentName);
        }

        return attachmentName;

        void PutValueEmbeddingsDocumentFromEmbeddingValue(BlittableJsonReaderObject json, ReadOnlyMemory<float> embeddingValue, string attachmentName)
        {
            using (var stream = new MemoryStream(MemoryMarshal.Cast<float, byte>(embeddingValue.Span).ToArray()))
            {
                var hash = AttachmentsStorageHelper.CalculateHash(context, stream);

                _documentsStorage.Put(context, item.ValueEmbeddingsDocumentId, null, json);
                _documentsStorage.AttachmentsStorage.PutAttachment(context, item.ValueEmbeddingsDocumentId, attachmentName, EmbeddingAttachmentContentType, hash, null,
                    stream);
            }
        }

        void PutValueEmbeddingsDocumentFromAttachment(BlittableJsonReaderObject json, Attachment attachment, string attachmentName)
        {
            _documentsStorage.Put(context, item.ValueEmbeddingsDocumentId, null, json);
            _documentsStorage.AttachmentsStorage.PutAttachment(context, item.ValueEmbeddingsDocumentId, attachmentName, attachment.ContentType,
                attachment.Base64Hash.ToString(), null, attachment.Stream);
        }
    }
    
    public static DynamicJsonValue CreateValueEmbeddingsDocument(string textualValue, string attachmentName, DateTime lastModified)
    {
        return new DynamicJsonValue
        {
            [textualValue] = attachmentName,
            [Constants.Documents.Metadata.Key] = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Collection] = Constants.Documents.Collections.EmbeddingsCollection,
                [Constants.Documents.Metadata.Expires] = lastModified.AddMonths(3)
            }
        };
    }

    public static string GetConnectionStringNameByTaskName(string taskName)
    {
        return _taskNameToNormalizedConnectionStringName[taskName];
    }
    
    public void HandleDatabaseRecordChange(DatabaseRecord record)
    {
        if (record == null)
            return;

        foreach (var connectionStringKvp in record.AiConnectionStrings)
        {
            var connectionStringName = connectionStringKvp.Key;
            var connectionString = connectionStringKvp.Value;
            
            if (_servicesByConnectionStringName.ContainsKey(connectionStringName))
                continue;
            
            var kernelBuilder = Kernel.CreateBuilder();
            kernelBuilder.Configure(connectionString, isConnectionTest: false);
            var kernel = kernelBuilder.Build();
#pragma warning disable SKEXP0001
            var service = kernel.GetRequiredService<ITextEmbeddingGenerationService>();
#pragma warning restore SKEXP0001
            
            _servicesByConnectionStringName[connectionStringName] = service;
        }

        foreach (var aiIntegrationConfiguration in record.AiIntegrations)
        {
            var aiIntegrationName = aiIntegrationConfiguration.Name;
            var connectionStringName = aiIntegrationConfiguration.ConnectionStringName;

            var service = _servicesByConnectionStringName[connectionStringName];

            _servicesByTaskName[aiIntegrationName] = service;
            
            // todo use normalized name
            _taskNameToNormalizedConnectionStringName[aiIntegrationName] = connectionStringName;
        }
    }

    public static void CacheEmbeddings()
    {
        var payload = new List<EmbeddingCacheItem>();
        
        while (_embeddingsQueue.TryDequeue(out var item))
            payload.Add(item);
        
        var putEmbeddingsCommand = new AiIntegrationTask.MergedCacheEmbeddingsCommand(payload, _database);
        
        _database.TxMerger.EnqueueSync(putEmbeddingsCommand);
    }

    public static void EnqueueEmbeddingToCache(string connectionStringName, string textualValue, ReadOnlyMemory<float> embedding)
    {
        var newItem = new EmbeddingCacheItem() { EmbeddingValue = embedding, TextualValue = textualValue, ConnectionStringName = connectionStringName };
        
        _embeddingsQueue.Enqueue(newItem);
    }

    public class EmbeddingCacheItem
    {
        public ReadOnlyMemory<float> EmbeddingValue;
        public string TextualValue;
        // Name of the connection string used for embedding generation 
        public string ConnectionStringName;
    }
}
