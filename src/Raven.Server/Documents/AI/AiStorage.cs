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
    private const string AttachmentNameLiteral = "AttachmentName";

    private readonly DocumentsStorage _documentsStorage;
    
#pragma warning disable SKEXP0001
    private readonly Dictionary<AiIntegrationIdentifier, ITextEmbeddingGenerationService> _servicesByIntegrationTaskIdentifier;
    private readonly Dictionary<AiConnectionStringIdentifier, ITextEmbeddingGenerationService> _servicesByConnectionStringIdentifier;
#pragma warning restore SKEXP0001
    private static Dictionary<AiIntegrationIdentifier, AiConnectionStringIdentifier> _taskIdentifierToConnectionStringIdentifier;
    
    private readonly EmbeddingsCacher _embeddingsCacher;
    
    public AiStorage([NotNull] DocumentDatabase database)
    {
        _documentsStorage = database.DocumentsStorage ?? throw new ArgumentNullException(nameof(_documentsStorage));
#pragma warning disable SKEXP0001
        _servicesByConnectionStringIdentifier = new();
        _servicesByIntegrationTaskIdentifier = new ();
#pragma warning restore SKEXP0001
        
        _taskIdentifierToConnectionStringIdentifier = new Dictionary<AiIntegrationIdentifier, AiConnectionStringIdentifier>();

        //_embeddingsCacher = new EmbeddingsCacher(database, database.Loggers.GetLogger<EmbeddingsCacher>(), database.DatabaseShutdown);
    }

#pragma warning disable SKEXP0001
    public bool TryGetServiceByIntegrationIdentifier(AiIntegrationIdentifier taskIdentifier, out ITextEmbeddingGenerationService service)
#pragma warning restore SKEXP0001
    {
        return _servicesByIntegrationTaskIdentifier.TryGetValue(taskIdentifier, out service);
    }

    public Document GetDocumentEmbeddings(DocumentsOperationContext context, string sourceDocumentId, out string documentEmbeddingsId)
    {
        documentEmbeddingsId = AiHelper.GetDocumentEmbeddingsId(sourceDocumentId);

        var document = _documentsStorage.Get(context, documentEmbeddingsId);
        
        return document;
    }

    public ValueEmbeddingsDocument GetValueEmbeddingsDocument(DocumentsOperationContext context, AiConnectionStringIdentifier connectionStringIdentifier, string value,
        out string valueEmbeddingsDocumentId)
    {
        valueEmbeddingsDocumentId = AiHelper.GetValueEmbeddingsDocumentId(connectionStringIdentifier, AiHelper.CalculateValueHash(value));
        
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
                var djv = CreateValueEmbeddingsDocument(attachmentName, lastModified);

                using (var json = context.ReadObject(djv, item.ValueEmbeddingsDocumentId))
                    PutValueEmbeddingsDocumentFromEmbeddingValue(json, item.EmbeddingValue, item.UsedBytes, attachmentName);

                return attachmentName;
            }

            document.Inner.Data.Modifications = new DynamicJsonValue(document.Inner.Data) { [AttachmentNameLiteral] = attachmentName };

            using (var json = context.ReadObject(document.Inner.Data, item.ValueEmbeddingsDocumentId))
                PutValueEmbeddingsDocumentFromEmbeddingValue(json, item.EmbeddingValue, item.UsedBytes, attachmentName);

            return attachmentName;
        }

        var attachment = _documentsStorage.AttachmentsStorage.GetAttachment(context, item.ValueEmbeddingsDocumentId, attachmentName, AttachmentType.Document, null);
        if (attachment == null)
            AttachmentDoesNotExistException.ThrowFor(item.ValueEmbeddingsDocumentId, attachmentName);

        if (document == null)
        {
            var djv = CreateValueEmbeddingsDocument(attachmentName, lastModified);

            using (var json = context.ReadObject(djv, item.ValueEmbeddingsDocumentId))
                PutValueEmbeddingsDocumentFromAttachment(json, attachment, attachmentName);

            return attachmentName;
        }

        if (document.Inner.Data.TryGet(item.TextualValue, out attachmentName) == false || attachment.Name != attachmentName)
        {
            document.Inner.Data.Modifications = new DynamicJsonValue(document.Inner.Data) { [item.TextualValue] = attachment.Name };

            using (var json = context.ReadObject(document.Inner.Data, item.ValueEmbeddingsDocumentId))
                PutValueEmbeddingsDocumentFromEmbeddingValue(json, item.EmbeddingValue, item.UsedBytes, attachmentName);
        }

        return attachmentName;

        void PutValueEmbeddingsDocumentFromEmbeddingValue(BlittableJsonReaderObject json, ReadOnlyMemory<float> embeddingValue, int usedBytes, string attachmentName)
        {
            var embeddingSpan = MemoryMarshal.Cast<float, byte>(embeddingValue.Span)[..usedBytes];
            
            using (var stream = new MemoryStream(embeddingSpan.ToArray()))
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
    
    public static DynamicJsonValue CreateValueEmbeddingsDocument(string attachmentName, DateTime lastModified)
    {
        return new DynamicJsonValue
        {
            [AttachmentNameLiteral] = attachmentName,
            [Constants.Documents.Metadata.Key] = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Collection] = Constants.Documents.Collections.EmbeddingsCollection,
                [Constants.Documents.Metadata.Expires] = lastModified.AddMonths(3)
            }
        };
    }

    public static AiConnectionStringIdentifier GetConnectionStringIdentifierByIntegrationIdentifier(AiIntegrationIdentifier integrationIdentifier)
    {
        return _taskIdentifierToConnectionStringIdentifier[integrationIdentifier];
    }
    
    public void HandleDatabaseRecordChange(DatabaseRecord record)
    {
        if (record == null)
            return;

        foreach (var connectionStringKvp in record.AiConnectionStrings)
        {
            var connectionStringIdentifier = new AiConnectionStringIdentifier(connectionStringKvp.Value.Identifier);
            var connectionString = connectionStringKvp.Value;
            
            if (_servicesByConnectionStringIdentifier.ContainsKey(connectionStringIdentifier))
                continue;
            
            var kernelBuilder = Kernel.CreateBuilder();
            kernelBuilder.Configure(connectionString, isConnectionTest: false);
            var kernel = kernelBuilder.Build();
#pragma warning disable SKEXP0001
            var service = kernel.GetRequiredService<ITextEmbeddingGenerationService>();
#pragma warning restore SKEXP0001
            
            _servicesByConnectionStringIdentifier[connectionStringIdentifier] = service;
        }
        
        // todo skip disabled tasks?
        foreach (var aiIntegrationConfiguration in record.AiIntegrations)
        {
            var aiIntegrationIdentifier = new AiIntegrationIdentifier(aiIntegrationConfiguration.Identifier);
            var connectionStringIdentifier = new AiConnectionStringIdentifier(record.AiConnectionStrings[aiIntegrationConfiguration.ConnectionStringName].Identifier);

            var service = _servicesByConnectionStringIdentifier[connectionStringIdentifier];

            _servicesByIntegrationTaskIdentifier[aiIntegrationIdentifier] = service;
            
            _taskIdentifierToConnectionStringIdentifier[aiIntegrationIdentifier] = connectionStringIdentifier;
        }

        /*
        if (_embeddingsCacher.IsStarted)
        {
            if (record.AiIntegrations.Count == 0)
            {
                _embeddingsCacher.Stop();
                _embeddingsCacher.IsStarted = false;
            }
            return;
        }
        */
        
        //_embeddingsCacher.Start();
        //_embeddingsCacher.IsStarted = true;
    }

    public void EnqueueEmbeddingToCache(AiConnectionStringIdentifier connectionStringIdentifier, string textualValue, ReadOnlyMemory<float> embeddingValue)
    {
        _embeddingsCacher.EnqueueEmbeddingToCache(connectionStringIdentifier, textualValue, embeddingValue);
    }
}
