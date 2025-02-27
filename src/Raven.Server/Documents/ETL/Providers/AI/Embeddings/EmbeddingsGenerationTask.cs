using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Queries.Vector;
using Raven.Client.Util;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings.Test;
using Raven.Server.Documents.ETL.Providers.AI.Enumerators;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

#pragma warning disable SKEXP0001

namespace Raven.Server.Documents.ETL.Providers.AI.Embeddings;

public sealed class EmbeddingsGenerationTask : EtlProcess<AiIntegrationItem, EmbeddingGenerationScriptResult, EmbeddingsGenerationConfiguration, AiConnectionString, AiIntegrationStatsScope, EmbeddingsGenerationPerformanceOperation>
{
    private ITextEmbeddingGenerationService _service;

    private readonly MissingEmbeddingsHolder _missingEmbeddingsHolder = new();

    public const string EmbeddingsTaskTag = "AI/Embeddings Generation";

    public EmbeddingsGenerationTask(Transformation transformation, EmbeddingsGenerationConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
        : base(transformation, configuration, database, serverStore, EmbeddingsTaskTag)
    {
        Metrics = new EtlMetricsCountersManager();
    }

    private AiIntegrationStatsScope _statsScope;

    public override EtlType EtlType => EtlType.EmbeddingsGeneration;
    public override bool ShouldTrackCounters() => false;
    public override bool ShouldTrackTimeSeries() => false;

    protected override IEnumerator<AiIntegrationItem> ConvertDocsEnumerator(DocumentsOperationContext context, IEnumerator<Document> docs, string collection)
    {
        return new DocumentsToAiItems(docs, collection);
    }

    protected override IEnumerator<AiIntegrationItem> ConvertTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection, bool trackAttachments)
    {
        return new TombstonesToAiItems(context, tombstones, collection, trackAttachments);
    }

    protected override IEnumerator<AiIntegrationItem> ConvertAttachmentTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, List<string> collections)
    {
        throw new NotImplementedException();
    }

    protected override IEnumerator<AiIntegrationItem> ConvertCountersEnumerator(DocumentsOperationContext context, IEnumerator<CounterGroupDetail> counters, string collection)
    {
        throw new NotImplementedException();
    }

    protected override IEnumerator<AiIntegrationItem> ConvertTimeSeriesEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesSegmentEntry> timeSeries, string collection)
    {
        throw new NotImplementedException();
    }

    protected override IEnumerator<AiIntegrationItem> ConvertTimeSeriesDeletedRangeEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesDeletedRangeItem> timeSeries, string collection)
    {
        throw new NotImplementedException();
    }

    protected override bool ShouldTrackAttachmentTombstones()
    {
        return false;
    }

    protected override EtlTransformer<AiIntegrationItem, EmbeddingGenerationScriptResult, AiIntegrationStatsScope, EmbeddingsGenerationPerformanceOperation> GetTransformer(DocumentsOperationContext context)
    {
        return new EmbeddingsGenerationScriptTransformer(Database, context, Transformation, null, Configuration);
    }

    protected override int LoadInternal(IEnumerable<EmbeddingGenerationScriptResult> items, DocumentsOperationContext context, AiIntegrationStatsScope scope)
    {
        _service ??= AiHelper.CreateService(Configuration);
        if (items is not EmbeddingsGenerationScriptRun embeddingsScriptRun)
        {
            Debug.Assert(items != null && items!.GetType()!.FullName!.StartsWith("System.Linq.EmptyPartition")
                , $"items != null && items!.GetType()!.FullName!.StartsWith('System.Linq.EmptyPartition'): {items!.GetType()!.FullName!}");
            return 0;
        }
        
        int processed = 0;

        using (_missingEmbeddingsHolder)
        {
            foreach (var embeddingItem in embeddingsScriptRun.Additions)
            {
                foreach (var kvp in embeddingItem.Values)
                {
                    var values = kvp.Value;
                    
                    foreach (var value in values)
                    {
                        var connectionStringIdentifier = new AiConnectionStringIdentifier(Configuration.Connection.Identifier);

                        if (Database.AiIntegrations.Embeddings.Storage.ExistsEmbeddingCacheDocument(context, connectionStringIdentifier, value, Configuration.TargetQuantizationType) == false)
                            _missingEmbeddingsHolder.Add(value.InputValue, value);
                    }
                }

                processed++;
            }

            var embeddingsMap = _missingEmbeddingsHolder.GetEmbeddingsMap();
            var keys = embeddingsMap.Keys.ToList();
            if (embeddingsMap.Keys.Any())
            {
                var generatedValues = AsyncHelpers.RunSync(() => _service.GenerateEmbeddingsAsync(keys));

                if (generatedValues.Count != keys.Count)
                    throw new InvalidOperationException("Generated embeddings count does not match missing values count");
                
                for (var i = 0; i < keys.Count; ++i)
                {
                    var key = keys[i];
                    var embedding = generatedValues[i];
                    
                    switch (Configuration.TargetQuantizationType)
                    {
                        case VectorEmbeddingType.Single:
                            foreach (var embeddingItem in embeddingsMap[key])
                            {
                                Debug.Assert(embeddingItem.OutputValue is ReadOnlyMemory<float>, "embeddingItem.OutputValue is ReadOnlyMemory<float>");
                                embeddingItem.OutputValue = embedding;
                                embeddingItem.UsedBytes = embedding.Length * sizeof(float);
                            }
                            break;
                        case VectorEmbeddingType.Int8:
                            foreach (var embeddingItem in embeddingsMap[key])
                            {
                                var dest = MemoryMarshal.Cast<float, sbyte>(embedding.Span);
                                if (VectorQuantizer.TryToInt8(embedding.Span, dest, out int usedBytes) == false)
                                {
                                    var newMemory = new ReadOnlyMemory<float>(new float[embedding.Length + 1]);
                                    var span = MemoryMarshal.Cast<float, sbyte>(newMemory.Span);
                                    var result = VectorQuantizer.TryToInt8(embedding.Span, span, out usedBytes);
                                    Debug.Assert(result, "TryToInt8 should always return true");
                                    embeddingItem.OutputValue = newMemory;
                                    embeddingItem.UsedBytes = usedBytes;
                                    break;
                                }
                                
                                embeddingItem.OutputValue = embedding;
                                embeddingItem.UsedBytes = usedBytes;
                            }
                            break;
                        case VectorEmbeddingType.Binary:
                            foreach (var embeddingItem in embeddingsMap[key])
                            {
                                var dest = MemoryMarshal.Cast<float, byte>(embedding.Span);
                                VectorQuantizer.TryToInt1(embedding.Span, dest, out int usedBytes);
                                embeddingItem.OutputValue = embedding;
                                embeddingItem.UsedBytes = usedBytes;
                            }
                            break;
                        default:
                            throw new ArgumentOutOfRangeException($"Quantization type {Configuration.TargetQuantizationType} is not supported");
                    }
                }
            }
            
            var putEmbeddingsCommand = new MergedPutEmbeddingsCommand(embeddingsScriptRun, new EmbeddingsGenerationTaskIdentifier(Configuration.Identifier), Database);

            Database.TxMerger.EnqueueSync(putEmbeddingsCommand);
        }
        
        return processed;
    }

    protected override AiIntegrationStatsScope CreateScope(EtlRunStats stats)
    {
        return new AiIntegrationStatsScope(stats);
    }

    protected override bool ShouldFilterOutHiLoDocument()
    {
        return true;
    }

    private class MissingEmbeddingsHolder : IDisposable
    {
        // missing value -> embeddings
        private readonly Dictionary<string, List<EmbeddingGenerationItem>> _embeddingsMap = new();
        
        public void Add(string value, EmbeddingGenerationItem item)
        {
            if (_embeddingsMap.ContainsKey(value) == false)
                _embeddingsMap.Add(value, new List<EmbeddingGenerationItem>());
                
            _embeddingsMap[value].Add(item);
        }

        public IReadOnlyDictionary<string, List<EmbeddingGenerationItem>> GetEmbeddingsMap() => _embeddingsMap;

        public void Dispose()
        {
            _embeddingsMap.Clear();
        }
    }

    public EmbeddingsGenerationTestScriptResult RunTest(IEnumerable<EmbeddingGenerationScriptResult> records, DocumentsOperationContext context)
    {
        var services = AiHelper.CreateServicesForTest(
            new EmbeddingsGenerationConfiguration
            {
                Connection = new AiConnectionString { OnnxSettings = new OnnxSettings()}
            }, out string serviceId);

        var embeddingService = services.GetRequiredKeyedService<ITextEmbeddingGenerationService>(serviceId);
        var result = new EmbeddingsGenerationTestScriptResult();

        foreach (var record in records)
        {
            foreach (var embeddingItemValue in record.Values.SelectMany(x => x.Value))
            {
                embeddingItemValue.OutputValue = embeddingService.GenerateEmbeddingsAsync([embeddingItemValue.InputValue]).Result[0];

                result.EmbeddingItemValues.Add(embeddingItemValue);
            }
        }

        result.TransformationErrors = Statistics.TransformationErrorsInCurrentBatch.Errors.ToList();
        return result;
    }

    private sealed class MergedPutEmbeddingsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>, IDisposable
    {
        /// <summary>
        /// Contains ETL result
        /// </summary>
        private readonly EmbeddingsGenerationScriptRun _taskResults;
        private readonly EmbeddingsGenerationTaskIdentifier _embeddingsTaskIdentifier;
        private readonly DocumentDatabase _database;
        private readonly EmbeddingsGenerationConfiguration _configuration; 
        
        public MergedPutEmbeddingsCommand(EmbeddingsGenerationScriptRun taskResults, EmbeddingsGenerationTaskIdentifier embeddingsTaskIdentifier, DocumentDatabase database)
        {
            _taskResults = taskResults;
            _embeddingsTaskIdentifier = embeddingsTaskIdentifier;
            _database = database;
            database.AiIntegrations.TryGetEmbeddingsGenerationConfiguration(embeddingsTaskIdentifier, out _configuration);
            Debug.Assert(_configuration != null, "Configuration should not be null");
        }

        //We need to remove all attachments from this ETL task (but not all of them, as they may be loaded from other tasks.)
        //So, if the embeddingsDocument exists, let's gather the attachments currently used by it.
        private Dictionary<string, List<string>> LoadNamesOfExistingAttachmentsOfThisTransformer(Document embeddingsDocument)
        {
            var destination = new Dictionary<string, List<string>>();
        
            if (embeddingsDocument == null)
                return destination;
            
            if (BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, embeddingsDocument, _embeddingsTaskIdentifier.Value, out var etlEmbeddingsByPathObject)
                && etlEmbeddingsByPathObject is BlittableJsonReaderObject etlEmbeddingsByPath)
            {
                //For each property under ETL name
                foreach (var path in etlEmbeddingsByPath.GetPropertyNames())
                {
                    // We need to read the property at it is, since it may contain dots, etc and BlittableJsonTraverserHelper detects them as nested properties.
                    if (etlEmbeddingsByPath.TryGetMember(path, out var attachmentsArrayObject) == false)
                        continue;
        
                    if (attachmentsArrayObject is not BlittableJsonReaderArray array) 
                        continue; // this should never happen unless user manually modifies the embeddings document
                            
                    ref var currentRemoval = ref CollectionsMarshal.GetValueRefOrAddDefault(destination, path, out _);
                    currentRemoval ??= new(array.Length);
                            
                    foreach (var item in array.Items)
                        currentRemoval.Add(item.ToString());
                }
            }
            
            return destination;
        }

        private DynamicJsonValue CreateNewDocument(string collectionName, DynamicJsonValue embeddingsDocumentModification)
        {
            return new DynamicJsonValue
            {
                [_embeddingsTaskIdentifier.Value] = embeddingsDocumentModification,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue()
                {
                    // todo cache
                    [Constants.Documents.Metadata.Collection] = EmbeddingsHelper.GetEmbeddingDocumentCollectionName(collectionName),
                }
            };
        }

        private BlittableJsonReaderObject GetModifiedCurrentDocument(Document embeddingsDocument, DynamicJsonValue embeddingsDocumentModification)
        {
            embeddingsDocument.Data.Modifications = new DynamicJsonValue
            {
                [_embeddingsTaskIdentifier.Value] = embeddingsDocumentModification
            };

            return embeddingsDocument.Data;
        }

        private BlittableJsonReaderObject GetReader(DocumentsOperationContext context, object document, string documentId, out bool isNewDocument)
        {
            if (document is DynamicJsonValue newDocument)
            {
                isNewDocument = true;
                return context.ReadObject(newDocument, documentId);
            }
            
            PortableExceptions.ThrowIfNot<InvalidCastException>(document is BlittableJsonReaderObject, $"Unexpected document type: {document.GetType().FullName}");

            isNewDocument = false;
            return context.ReadObject((BlittableJsonReaderObject)document, documentId);
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            var operationStartDate = _database.Time.GetUtcNow();

            // For each of processed document
            foreach (var document in _taskResults.Additions)
            {
                // Modifications of embeddings document
                var embeddingsDocumentModification = new DynamicJsonValue();
                if (_configuration.TargetQuantizationType != VectorEmbeddingType.Single)
                    embeddingsDocumentModification[Constants.Documents.Metadata.Quantization] = _configuration.TargetQuantizationType.ToString();
                
                
                // Load the embeddings document (if it exists) to track which attachments need to be removed (on update)
                using var embeddingsDocument = _database.AiIntegrations.Embeddings.Storage.GetDocumentEmbeddings(context, document.DocumentId, out string embeddingsDocumentId);
                var currentAttachmentsOfEmbeddingsFromThisTransformer = LoadNamesOfExistingAttachmentsOfThisTransformer(embeddingsDocument);
                
                foreach (var embeddingsByPath in document.Values)
                {
                    var currentPath = embeddingsByPath.Key;
                    var generatedEmbeddings = embeddingsByPath.Value;
                    var prefix = EmbeddingsHelper.GetPrefixForAttachmentInEmbeddingsDocument(_embeddingsTaskIdentifier, currentPath);
                    var namesOfNewAttachments = new DynamicJsonArray();
                    
                    // todo better handling
                    var alreadyAddedAttachments = new HashSet<string>();
                    
                    foreach (var embedding in generatedEmbeddings)
                    {
                        _database.AiIntegrations.Embeddings.Storage.AddOrUpdateEmbeddingDocument(context, embedding, operationStartDate);
                        embedding.SetPrefixForDestinationAttachmentName(prefix);
                        
                        if (alreadyAddedAttachments.Add(embedding.DestinationAttachmentName) == false)
                            continue;
                        
                        namesOfNewAttachments.Add(embedding.DestinationAttachmentName);
                    }
                    
                    embeddingsDocumentModification[currentPath] = namesOfNewAttachments;
                }

                object documentToProcess = embeddingsDocument is null 
                    ? CreateNewDocument(document.DocumentCollectionName, embeddingsDocumentModification) 
                    : GetModifiedCurrentDocument(embeddingsDocument, embeddingsDocumentModification);
                
                using (var reader = GetReader(context, documentToProcess, embeddingsDocumentId, out var isNewDocument))
                {
                    // Update the document
                    _database.DocumentsStorage.Put(context, embeddingsDocumentId, null, reader);
                    
                    // Insert new embeddings
                    foreach (var embeddingsByPath in document.Values)
                    {
                        // todo better handling
                        var alreadyAddedAttachments = new HashSet<string>();
                        
                        var namesOfCurrentAttachments = currentAttachmentsOfEmbeddingsFromThisTransformer.GetValueOrDefault(embeddingsByPath.Key);
                        foreach (var embedding in embeddingsByPath.Value)
                        {
                            if (alreadyAddedAttachments.Add(embedding.DestinationAttachmentName) == false)
                                continue;
                            
                            // When true:
                            //  This embedding is already in the embeddings document. Therefore, we do not have to insert it again.
                            //  At the same time, we are removing it from the list of attachments to remove (essentially a no-op on attachment storage in case of an update).
                            if (namesOfCurrentAttachments?.Remove(embedding.DestinationAttachmentName) == true)
                                continue; 
                            
                            _database.DocumentsStorage.AttachmentsStorage.CopyAttachment(context, embedding.EmbeddingCacheDocumentId, embedding.InputValueHash
                                , embeddingsDocumentId, embedding.DestinationAttachmentName, null, AttachmentType.Document);
                        }
                    }
                    
                    // Remove old embeddings
                    foreach (var embeddingsByPath in currentAttachmentsOfEmbeddingsFromThisTransformer)
                    {
                        foreach (var attachmentToRemove in embeddingsByPath.Value)
                        {
                            _database.DocumentsStorage.AttachmentsStorage.DeleteAttachment(context, embeddingsDocumentId, attachmentToRemove, null, out _, extractCollectionName: false);
                        }
                    }
                }
            }
            
            foreach (var item in _taskResults.Removals)
            {
                var documentEmbeddingsToDeleteId = EmbeddingsHelper.GetEmbeddingDocumentId(item.DocumentId);
                _database.DocumentsStorage.Delete(context, documentEmbeddingsToDeleteId, DocumentFlags.None);
            }
            
            return _taskResults.Additions.Count + _taskResults.Removals.Count;
        }
        
        public void Dispose()
        {
            
        }

        // todo
        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            throw new NotImplementedException();
        }

        public sealed class MergedPutEmbeddingsCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedPutCommand>
        {
            public string Id { get; set; }
            public LazyStringValue ExpectedChangeVector { get; set; }
            public BlittableJsonReaderObject Document { get; set; }

            public MergedPutCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                throw new NotImplementedException();
            }
        }
    }
}
