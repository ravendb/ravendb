using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Providers.AI.Enumerators;
using Raven.Server.Documents.ETL.Providers.AI.Test;
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

namespace Raven.Server.Documents.ETL.Providers.AI;

public sealed class AiIntegrationTask : EtlProcess<AiIntegrationItem, AiIntegrationEmbeddingItem, AiIntegrationConfiguration, AiConnectionString, AiIntegrationStatsScope, AiIntegrationPerformanceOperation>
{
    private ITextEmbeddingGenerationService _service;

    private readonly MissingEmbeddingsHolder _missingEmbeddingsHolder = new();

    public const string AiTaskTag = "AI Integration";

    public AiIntegrationTask(Transformation transformation, AiIntegrationConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
        : base(transformation, configuration, database, serverStore, AiTaskTag)
    {
        Metrics = new EtlMetricsCountersManager();
    }

    private AiIntegrationStatsScope _statsScope;

    public override EtlType EtlType => EtlType.Ai;
    public override bool ShouldTrackCounters() => false;
    public override bool ShouldTrackTimeSeries() => false;

    protected override IEnumerator<AiIntegrationItem> ConvertDocsEnumerator(DocumentsOperationContext context, IEnumerator<Document> docs, string collection)
    {
        return new DocumentsToAiEmbeddingsItems(docs, collection);
    }

    protected override IEnumerator<AiIntegrationItem> ConvertTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection, bool trackAttachments)
    {
        return new TombstonesToAiEmbeddingsItems(context, tombstones, collection, trackAttachments);
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

    protected override EtlTransformer<AiIntegrationItem, AiIntegrationEmbeddingItem, AiIntegrationStatsScope, AiIntegrationPerformanceOperation> GetTransformer(DocumentsOperationContext context)
    {
        return new AiIntegrationTransformer(Database, context, Transformation, null, Configuration);
    }

    protected override int LoadInternal(IEnumerable<AiIntegrationEmbeddingItem> items, DocumentsOperationContext context, AiIntegrationStatsScope scope)
    {
        _service ??= AiHelper.CreateService(Configuration);

        if (items is not AiEmbeddingsScriptRun aiEtlScriptRun)
        {
            Debug.Assert(items != null && items!.GetType()!.FullName!.StartsWith("System.Linq.EmptyPartition")
                , $"items != null && items!.GetType()!.FullName!.StartsWith('System.Linq.EmptyPartition'): {items!.GetType()!.FullName!}");
            return 0;
        }
        
        int processed = 0;

        using (_missingEmbeddingsHolder)
        {
            foreach (var aiEtlEmbeddingItem in aiEtlScriptRun.Additions)
            {
                foreach (var kvp in aiEtlEmbeddingItem.Values)
                {
                    var values = kvp.Value;
                    
                    foreach (var value in values)
                    {
                        var valueEmbeddingsDocument = Database.AiStorage.GetValueEmbeddingsDocument(context, Configuration, value.TextualValue, out var valueEmbeddingsDocumentId);

                        value.ValueEmbeddingsDocumentId = valueEmbeddingsDocumentId;
                        value.ValueEmbeddingsSourceAttachmentName = valueEmbeddingsDocument?.GetAttachmentNameForValue(value.TextualValue);
                        
                        if (value.ValueEmbeddingsSourceAttachmentName == null)
                            _missingEmbeddingsHolder.Add(value.TextualValue, value);
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
                    
                    foreach (var embeddingItem in embeddingsMap[key])
                        embeddingItem.EmbeddingValue = embedding;
                }
            }
            
            var putEmbeddingsCommand = new MergedPutEmbeddingsCommand(aiEtlScriptRun, Configuration.Name, Database);

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
        private readonly Dictionary<string, List<AiIntegrationEmbeddingItemValue>> _embeddingsMap = new();
        
        public void Add(string value, AiIntegrationEmbeddingItemValue item)
        {
            if (_embeddingsMap.ContainsKey(value) == false)
                _embeddingsMap.Add(value, new List<AiIntegrationEmbeddingItemValue>());
                
            _embeddingsMap[value].Add(item);
        }

        public IReadOnlyDictionary<string, List<AiIntegrationEmbeddingItemValue>> GetEmbeddingsMap() => _embeddingsMap;

        public void Dispose()
        {
            _embeddingsMap.Clear();
        }
    }

    public AiIntegrationTestScriptResult RunTest(IEnumerable<AiIntegrationEmbeddingItem> records, DocumentsOperationContext context)
    {
        var services = AiHelper.CreateServicesForTest(
            new AiIntegrationConfiguration
            {
                Connection = new AiConnectionString { OnnxSettings = new OnnxSettings()}
            }, out string serviceId);

        var embeddingService = services.GetRequiredKeyedService<ITextEmbeddingGenerationService>(serviceId);
        var result = new AiIntegrationTestScriptResult();

        foreach (var record in records)
        {
            foreach (var aiEtlEmbeddingItemValue in record.Values.SelectMany(x => x.Value))
            {
                aiEtlEmbeddingItemValue.ValueEmbeddingsDocumentId = record.DocumentId;
                aiEtlEmbeddingItemValue.EmbeddingValue = embeddingService.GenerateEmbeddingsAsync([aiEtlEmbeddingItemValue.TextualValue]).Result[0];

                result.EmbeddingItemValues.Add(aiEtlEmbeddingItemValue);
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
        private readonly AiEmbeddingsScriptRun _taskResults;
        private readonly string _aiIntegrationTaskName;
        private readonly DocumentDatabase _database;
        public DocumentsStorage.PutOperationResults PutResult;
        
        public MergedPutEmbeddingsCommand(AiEmbeddingsScriptRun taskResults, string aiIntegrationTaskName, DocumentDatabase database)
        {
            _taskResults = taskResults;
            _aiIntegrationTaskName = aiIntegrationTaskName;
            _database = database;
        }

        //We need to remove all attachments from this ETL task (but not all of them, as they may be loaded from other tasks.)
        //So, if the embeddingsDocument exists, let's gather the attachments currently used by it.
        private Dictionary<string, List<string>> LoadNamesOfExistingAttachmentsOfThisTransformer(Document embeddingsDocument)
        {
            var destination = new Dictionary<string, List<string>>();
        
            if (embeddingsDocument == null)
                return destination;
            
            if (BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, embeddingsDocument, _aiIntegrationTaskName, out var etlEmbeddingsByPathObject)
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
                [_aiIntegrationTaskName] = embeddingsDocumentModification,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue()
                {
                    // todo cache
                    [Constants.Documents.Metadata.Collection] = AiHelper.GetDocumentEmbeddingsCollectionName(collectionName),
                }
            };
        }

        private BlittableJsonReaderObject GetModifiedCurrentDocument(Document embeddingsDocument, DynamicJsonValue embeddingsDocumentModification)
        {
            embeddingsDocument.Data.Modifications = new DynamicJsonValue
            {
                [_aiIntegrationTaskName] = embeddingsDocumentModification
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
            // Key: Transformer input
            // Value: Attachment name of embedding 
            var localEmbeddingCache = new Dictionary<string, string>();
            
            // For each of processed document
            foreach (var document in _taskResults.Additions)
            {
                // Modifications of embeddings document
                var embeddingsDocumentModification = new DynamicJsonValue();
                
                // Load the embeddings document (if it exists) to track which attachments need to be removed (on update)
                using var embeddingsDocument = _database.AiStorage.GetDocumentEmbeddings(context, document.DocumentId, out string embeddingsDocumentId);
                var currentAttachmentsOfEmbeddingsFromThisTransformer = LoadNamesOfExistingAttachmentsOfThisTransformer(embeddingsDocument);
                
                foreach (var embeddingsByPath in document.Values)
                {
                    var currentPath = embeddingsByPath.Key;
                    var generatedEmbeddings = embeddingsByPath.Value;
                    var prefix = AiHelper.GetPrefixForAttachmentInEmbeddingsDocument(_aiIntegrationTaskName, currentPath);
                    var namesOfNewAttachments = new DynamicJsonArray();
                    
                    foreach (var embedding in generatedEmbeddings)
                    {
                        ref var attachmentName = ref CollectionsMarshal.GetValueRefOrAddDefault(localEmbeddingCache, embedding.TextualValue, out _);
                        attachmentName ??= _database.AiStorage.AddOrUpdateValueEmbeddingsDocument(context, embedding, operationStartDate);
                        embedding.ValueEmbeddingsSourceAttachmentName = attachmentName;
                        embedding.SetPrefix(prefix);
                        namesOfNewAttachments.Add(embedding.ValueEmbeddingsDestinationAttachmentName);
                    }
                    
                    embeddingsDocumentModification[currentPath] = namesOfNewAttachments;
                }

                object documentToProcess = embeddingsDocument is null 
                    ? CreateNewDocument(document.DocumentCollectionName, embeddingsDocumentModification) 
                    : GetModifiedCurrentDocument(embeddingsDocument, embeddingsDocumentModification);
                
                using (var reader = GetReader(context, documentToProcess, embeddingsDocumentId, out var isNewDocument))
                {
                    //Update the document
                    _database.DocumentsStorage.Put(context, embeddingsDocumentId, null, reader);
                    
                    //Insert new embeddings
                    foreach (var embeddingsByPath in document.Values)
                    {
                        var namesOfCurrentAttachments = currentAttachmentsOfEmbeddingsFromThisTransformer.GetValueOrDefault(embeddingsByPath.Key);
                        foreach (var embedding in embeddingsByPath.Value)
                        {
                            //When true:
                            //  This embedding is already in the embeddings document. Therefore, we do not have to insert it again.
                            //  At the same time, we are removing it from the list of attachments to remove (essentially a no-op on attachment storage in case of an update).
                            if (namesOfCurrentAttachments?.Remove(embedding.ValueEmbeddingsDestinationAttachmentName) == true)
                                continue; 
                            
                            _database.DocumentsStorage.AttachmentsStorage.CopyAttachment(context, embedding.ValueEmbeddingsDocumentId, embedding.ValueEmbeddingsSourceAttachmentName, embeddingsDocumentId, embedding.ValueEmbeddingsDestinationAttachmentName, null, AttachmentType.Document);
                        }
                    }
                    
                    //Remove old embeddings
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
                var documentEmbeddingsToDeleteId = AiHelper.GetDocumentEmbeddingsId(item.DocumentId);
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
