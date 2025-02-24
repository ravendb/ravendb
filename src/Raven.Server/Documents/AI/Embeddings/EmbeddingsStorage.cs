using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using JetBrains.Annotations;
using Microsoft.CodeAnalysis;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Exceptions.Documents.Attachments;
using Raven.Client.ServerWide;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.AI;

public class EmbeddingsStorage
{
    private const string EmbeddingAttachmentContentType = "application/octet-stream";

    private readonly DocumentsStorage _documentsStorage;
    
#pragma warning disable SKEXP0001
    private readonly Dictionary<EmbeddingsGenerationTaskIdentifier, ITextEmbeddingGenerationService> _servicesByIntegrationTaskIdentifier;
    private readonly Dictionary<AiConnectionStringIdentifier, ITextEmbeddingGenerationService> _servicesByConnectionStringIdentifier;
#pragma warning restore SKEXP0001
    private static Dictionary<EmbeddingsGenerationTaskIdentifier, AiConnectionStringIdentifier> _taskIdentifierToConnectionStringIdentifier;
    
    private readonly EmbeddingsCacher _embeddingsCacher;
    
    public EmbeddingsStorage([NotNull] DocumentDatabase database)
    {
        _documentsStorage = database.DocumentsStorage ?? throw new ArgumentNullException(nameof(_documentsStorage));
#pragma warning disable SKEXP0001
        _servicesByConnectionStringIdentifier = new();
        _servicesByIntegrationTaskIdentifier = new ();
#pragma warning restore SKEXP0001
        
        _taskIdentifierToConnectionStringIdentifier = new Dictionary<EmbeddingsGenerationTaskIdentifier, AiConnectionStringIdentifier>();

        //_embeddingsCacher = new EmbeddingsCacher(database, database.Loggers.GetLogger<EmbeddingsCacher>(), database.DatabaseShutdown);
    }

#pragma warning disable SKEXP0001
    public bool TryGetServiceByIntegrationIdentifier(EmbeddingsGenerationTaskIdentifier taskIdentifier, out ITextEmbeddingGenerationService service)
#pragma warning restore SKEXP0001
    {
        return _servicesByIntegrationTaskIdentifier.TryGetValue(taskIdentifier, out service);
    }

    public Document GetDocumentEmbeddings(DocumentsOperationContext context, string sourceDocumentId, out string documentEmbeddingsId)
    {
        documentEmbeddingsId = EmbeddingsHelper.GetEmbeddingDocumentId(sourceDocumentId);

        var document = _documentsStorage.Get(context, documentEmbeddingsId);
        
        return document;
    }

    public bool TryGetEmbeddingCacheDocument(DocumentsOperationContext context, AiConnectionStringIdentifier connectionStringIdentifier, string value,
        out string embeddingCacheDocumentId, out EmbeddingCacheDocument result)
    {
        embeddingCacheDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, "TODO arek");
        
        return TryGetEmbeddingCacheDocument(context, embeddingCacheDocumentId, out result);
    }

    private bool TryGetEmbeddingCacheDocument(DocumentsOperationContext context, string documentId, out EmbeddingCacheDocument result)
    {
        var document = _documentsStorage.Get(context, documentId);

        if (document == null)
        {
            result = null;
            return false;
        }

        result = new EmbeddingCacheDocument(document);
        return true;
    }

    public void AddOrUpdateEmbeddingDocument(DocumentsOperationContext context, EmbeddingGenerationItem item, DateTime currentDate)
    {
        if (item.OutputValue.IsEmpty)
        {
            // Cache contains embedding for our input value. Extend the expiration date.

            // todo logic
            // rozszerzenie eksipracji
            // validacja istnienia attachmentu


            return;
        }

        var newDoc = CreateEmbeddingCacheDocument(currentDate);
        using (var reader = context.ReadObject(newDoc, item.EmbeddingCacheDocumentId))
        {
            PutEmbeddingCacheDocument(reader, item.OutputValue, item.UsedBytes);
        }


        void PutEmbeddingCacheDocument(BlittableJsonReaderObject json, ReadOnlyMemory<float> embeddingValue, int usedBytes)
        {
            var embeddingSpan = MemoryMarshal.Cast<float, byte>(embeddingValue.Span)[..usedBytes];
            
            using (var stream = new MemoryStream(embeddingSpan.ToArray()))
            {
                var hash = AttachmentsStorageHelper.CalculateHash(MemoryMarshal.Cast<float, byte>(embeddingValue.Span));

                _documentsStorage.Put(context, item.EmbeddingCacheDocumentId, null, json);
                _documentsStorage.AttachmentsStorage.PutAttachment(context, item.EmbeddingCacheDocumentId, item.InputValueHash, EmbeddingAttachmentContentType, hash, null,
                    stream);
            }
        }
    }
    
    public static DynamicJsonValue CreateEmbeddingCacheDocument(DateTime lastModified)
    {
        return new DynamicJsonValue
        {
            [Constants.Documents.Metadata.Key] = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Collection] = Constants.Documents.Collections.EmbeddingsCacheCollection,
                [Constants.Documents.Metadata.Expires] = lastModified.AddMonths(3)// TODO arek
            }
        };
    }

    public static AiConnectionStringIdentifier GetConnectionStringIdentifierByIntegrationIdentifier(EmbeddingsGenerationTaskIdentifier integrationIdentifier)
    {
        return _taskIdentifierToConnectionStringIdentifier[integrationIdentifier];
    }
    
    

    public void EnqueueEmbeddingToCache(AiConnectionStringIdentifier connectionStringIdentifier, string textualValue, ReadOnlyMemory<float> embeddingValue)
    {
        _embeddingsCacher.EnqueueEmbeddingToCache(connectionStringIdentifier, textualValue, embeddingValue);
    }

    public bool ExistsEmbeddingCacheDocument(DocumentsOperationContext context, AiConnectionStringIdentifier connectionStringIdentifier, EmbeddingGenerationItem value)
    {
        value.EmbeddingCacheDocumentId = EmbeddingsHelper.GetEmbeddingCacheDocumentId(connectionStringIdentifier, value.InputValueHash);

        using var document = _documentsStorage.Get(context, value.EmbeddingCacheDocumentId);

        return document != null;
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
            var aiIntegrationIdentifier = new EmbeddingsGenerationTaskIdentifier(aiIntegrationConfiguration.Identifier);
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
}
