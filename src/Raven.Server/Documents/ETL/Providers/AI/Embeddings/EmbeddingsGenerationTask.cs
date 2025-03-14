using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Config;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings.Stats;
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

public sealed class EmbeddingsGenerationTask : EtlProcess<EmbeddingsGenerationItem, EmbeddingGenerationScriptResult, EmbeddingsGenerationConfiguration, AiConnectionString,
    EmbeddingsGenerationStatsScope, EmbeddingsGenerationPerformanceOperation>
{
    private const string EmbeddingsTaskTag = "AI/Embeddings Generation";

    private readonly MissingEmbeddingsHolder _missingEmbeddingsHolder = new();
    private int _fallbackCounter = 0;


    public EmbeddingsGenerationTask(Transformation transformation, EmbeddingsGenerationConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
        : base(transformation, configuration, database, serverStore, EmbeddingsTaskTag)
    {
        Metrics = new EtlMetricsCountersManager();
    }

    public override EtlType EtlType => EtlType.EmbeddingsGeneration;
    public override bool ShouldTrackCounters() => false;
    public override bool ShouldTrackTimeSeries() => false;
    
    protected override bool ShouldTrackAttachmentTombstones() => false;

    protected override IEnumerator<EmbeddingsGenerationItem> ConvertDocsEnumerator(DocumentsOperationContext context, IEnumerator<Document> docs, string collection)
    {
        return new DocumentsToEmbeddingsGenerationItems(docs, collection);
    }

    protected override IEnumerator<EmbeddingsGenerationItem> ConvertTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection,
        bool trackAttachments)
    {
        return new TombstonesToEmbeddingsGenerationItems(tombstones, collection);
    }

    protected override IEnumerator<EmbeddingsGenerationItem> ConvertAttachmentTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones,
        List<string> collections)
    {
        throw new NotSupportedException($"{nameof(ConvertAttachmentTombstonesEnumerator)} is not supported for {nameof(EmbeddingsGenerationTask)}");
    }

    protected override IEnumerator<EmbeddingsGenerationItem> ConvertCountersEnumerator(DocumentsOperationContext context, IEnumerator<CounterGroupDetail> counters,
        string collection)
    {
        throw new NotSupportedException($"{nameof(ConvertCountersEnumerator)} is not supported for {nameof(EmbeddingsGenerationTask)}");
    }

    protected override IEnumerator<EmbeddingsGenerationItem> ConvertTimeSeriesEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesSegmentEntry> timeSeries,
        string collection)
    {
        throw new NotSupportedException($"{nameof(ConvertTimeSeriesEnumerator)} is not supported for {nameof(EmbeddingsGenerationTask)}");
    }

    protected override IEnumerator<EmbeddingsGenerationItem> ConvertTimeSeriesDeletedRangeEnumerator(DocumentsOperationContext context,
        IEnumerator<TimeSeriesDeletedRangeItem> timeSeries, string collection)
    {
        throw new NotSupportedException($"{nameof(ConvertTimeSeriesDeletedRangeEnumerator)} is not supported for {nameof(EmbeddingsGenerationTask)}");
    }

    protected override EtlTransformer<EmbeddingsGenerationItem, EmbeddingGenerationScriptResult, EmbeddingsGenerationStatsScope, EmbeddingsGenerationPerformanceOperation>
        GetTransformer(DocumentsOperationContext context)
    {
        return new EmbeddingsGenerationScriptTransformer(Database, context, Transformation, null, Configuration);
    }

    protected override string LoadFailureMessage => $"Failed to generate embeddings in '{Configuration.Name}' task. Going to do the retry using '{Database.Configuration.Ai.EmbeddingsGenerationTaskRetryStrategy}' backoff strategy.";

    protected override void EnterFallbackMode(Exception e, DateTime? lastErrorTime)
    {
        _fallbackCounter++;
        var taskRetryDelay = Database.Configuration.Ai.EmbeddingsGenerationTaskRetryDelay.AsTimeSpan;
        if (lastErrorTime == null)
        {
            FallbackTime = taskRetryDelay;
        }
        else
        {
            var secondsToWait = (Database.Configuration.Ai.EmbeddingsGenerationTaskRetryStrategy) switch
            {
                EmbeddingsGenerationRetryStrategy.Linear => _fallbackCounter * taskRetryDelay.TotalSeconds,
                EmbeddingsGenerationRetryStrategy.Exponential => taskRetryDelay.TotalSeconds * Math.Pow(2, _fallbackCounter),
                _ => throw new NotImplementedException($"Strategy: '{Database.Configuration.Ai.EmbeddingsGenerationTaskRetryStrategy}' is not implemented.")
                
            };
            FallbackTime = TimeSpan.FromSeconds(Math.Min(Database.Configuration.Ai.EmbeddingsGenerationTaskMaxFallbackTime.AsTimeSpan.TotalSeconds, Math.Max(taskRetryDelay.Seconds, secondsToWait)));
        }
    }

    protected override int LoadInternal(IEnumerable<EmbeddingGenerationScriptResult> items, DocumentsOperationContext context, EmbeddingsGenerationStatsScope scope)
    {
        if (items is not EmbeddingsGenerationScriptRun embeddingsScriptRun)
        {
            Debug.Assert(items != null && items!.GetType()!.FullName!.StartsWith("System.Linq.EmptyPartition"),
                $"items != null && items!.GetType()!.FullName!.StartsWith('System.Linq.EmptyPartition'): {items!.GetType()!.FullName!}");
            return 0;
        }

        var connectionStringId = new AiConnectionStringIdentifier(Configuration.Connection.Identifier);

        int processed = 0;

        using (_missingEmbeddingsHolder)
        {
            var embeddingsInCache = 0;
            
            foreach (var embeddingItem in embeddingsScriptRun.Additions)
            {
                foreach (var kvp in embeddingItem.Values)
                {
                    var embeddingsGenerationItems = kvp.Value;
                    
                    for (var i = 0; i < embeddingsGenerationItems.Count; i++)
                    {
                        var embeddingToGenerate = embeddingsGenerationItems[i];

                        if (Database.AiIntegrations.Embeddings.Storage.IsEmbeddingValueCached(context, connectionStringId, ref embeddingToGenerate, Configuration.Quantization) == false)
                            _missingEmbeddingsHolder.Add(embeddingToGenerate.TextualValue, embeddingToGenerate);
                        else
                            embeddingsInCache++;
                    }
                }

                processed++;
            }

            var embeddingsMap = _missingEmbeddingsHolder.GetEmbeddingsMap();
            if (embeddingsMap.Keys.Any())
            {
                var keys = embeddingsMap.Keys.ToList();

                if (Database.AiIntegrations.TryGetServiceByConnectionString(connectionStringId, out var service) == false)
                    throw new ArgumentException($"Couldn't find embedding generation service for connection string '{connectionStringId.Value}'");

                IList<ReadOnlyMemory<float>> generatedValues;
                try
                {
                    using var embeddingsGenerationScope = scope.For(EmbeddingsGenerationOperations.GenerateInAiService);
                    generatedValues = AiHelper.GenerateEmbeddingsAsync(service.Instance, keys).GetAwaiter().GetResult();
                    embeddingsGenerationScope.NumberOfGeneratedEmbeddings = generatedValues.Count;
                    embeddingsGenerationScope.NumberOfEmbeddingsInCache = embeddingsInCache;

                }
                catch (Exception ex)
                {
                    if (ex is HttpOperationException { StatusCode: HttpStatusCode.TooManyRequests })
                    {
                        throw new EmbeddingGenerationException(
                            $"Failed to generate embeddings due to rate limits. " +
                            $"The process will increase the delay between calls to the model using {Database.Configuration.Ai.EmbeddingsGenerationTaskRetryStrategy.ToString().ToLower()} backoff strategy. " +
                            $"However, decreasing the number of elements processed in a single batch (using '{RavenConfiguration.GetKey(x => x.Ai.EmbeddingsGenerationTaskMaxBatchSize)}' " +
                            $"configuration option) may help, or you can increase the limits on your model deployment.",
                            ex);
                    }

                    throw;
                }

                if (generatedValues.Count != keys.Count)
                    throw new InvalidOperationException($"Generated embeddings count ({generatedValues.Count}) does not match missing values count ({keys.Count})");

                for (var i = 0; i < keys.Count; ++i)
                {
                    var key = keys[i];
                    var embedding = EmbeddingsHelper.CreateEmbeddingValue(generatedValues[i], Configuration.Quantization);

                    foreach (var embeddingItem in embeddingsMap[key])
                    {
                        embeddingItem.SetEmbedding(embedding, Configuration.Quantization, connectionStringId);
                    }
                }
            }

            using (var storageScope = scope.For(EmbeddingsGenerationOperations.Storage))
            {
                var putEmbeddingsCommand = new MergedPutEmbeddingsCommand(embeddingsScriptRun, Configuration, Database);

                Database.TxMerger.EnqueueSync(putEmbeddingsCommand);

                storageScope.NumberOfPutEmbeddingDocuments = embeddingsScriptRun.Additions.Count;
                storageScope.NumberOfDeletedEmbeddingDocuments = embeddingsScriptRun.Removals.Count;
            }
        }

        return processed;
    }

    protected override EmbeddingsGenerationStatsScope CreateScope(EtlRunStats stats)
    {
        return new EmbeddingsGenerationStatsScope(stats);
    }

    protected override string StatsAggregatorTag => "Embeddings Generation";

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
        (ITextEmbeddingGenerationService embeddingService, _) = AiHelper.CreateServicesForTest(
            new EmbeddingsGenerationConfiguration { Connection = new AiConnectionString { EmbeddedSettings = new EmbeddedSettings() } });

        var result = new EmbeddingsGenerationTestScriptResult();

        foreach (var record in records)
        {
            foreach (var embeddingItemValue in record.Values.SelectMany(x => x.Value))
            {
                var embedding = AiHelper.GenerateEmbedding(embeddingService, embeddingItemValue.TextualValue);

                var embeddingValue = EmbeddingsHelper.CreateEmbeddingValue(embedding, Configuration.Quantization);

                embeddingItemValue.SetEmbedding(embeddingValue, Configuration.Quantization, new AiConnectionStringIdentifier("TODO")); //TODO

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

        public MergedPutEmbeddingsCommand(EmbeddingsGenerationScriptRun taskResults, EmbeddingsGenerationConfiguration configuration,
            DocumentDatabase database)
        {
            _taskResults = taskResults ?? throw new ArgumentException($"{nameof(taskResults)} must not be null");
            _configuration = configuration ?? throw new ArgumentException($"{nameof(configuration)} must not be null");
            _database = database ?? throw new ArgumentException($"{nameof(database)} must not be null");

            _embeddingsTaskIdentifier = new EmbeddingsGenerationTaskIdentifier(configuration.Identifier);
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
                    var prefix = EmbeddingsHelper.GetPrefixForAttachmentInEmbeddingsDocument(_embeddingsTaskIdentifier, path);

                    // We need to read the property at it is, since it may contain dots, etc and BlittableJsonTraverserHelper detects them as nested properties.
                    if (etlEmbeddingsByPath.TryGetMember(path, out var attachmentsArrayObject) == false)
                        continue;

                    if (attachmentsArrayObject is not BlittableJsonReaderArray array)
                        continue; // this should never happen unless user manually modifies the embeddings document

                    ref var currentRemoval = ref CollectionsMarshal.GetValueRefOrAddDefault(destination, path, out _);
                    currentRemoval ??= new(array.Length);

                    foreach (var item in array.Items)
                        currentRemoval.Add(EmbeddingsHelper.GenerateDestinationAttachmentName(prefix, item.ToString(), _configuration.Quantization));
                }
            }

            return destination;
        }

        private DynamicJsonValue CreateEmbeddingDocument(string collectionName, DynamicJsonValue embeddingsDocumentModification)
        {
            return new DynamicJsonValue
            {
                [_embeddingsTaskIdentifier.Value] = embeddingsDocumentModification,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue()
                {
                    [Constants.Documents.Metadata.Collection] = EmbeddingsHelper.GetEmbeddingDocumentCollectionName(collectionName),
                }
            };
        }

        private BlittableJsonReaderObject GetModifiedCurrentDocument(Document embeddingsDocument, DynamicJsonValue embeddingsDocumentModification)
        {
            embeddingsDocument.Data.Modifications = new DynamicJsonValue { [_embeddingsTaskIdentifier.Value] = embeddingsDocumentModification };

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
            var expireAt = _database.Time.GetUtcNow().Add(_configuration.EmbeddingsCacheExpiration);

            var quantization = _configuration.Quantization;
            var alreadyAddedAttachments = new HashSet<string>();

            // For each of processed document
            foreach (var document in _taskResults.Additions)
            {
                // Modifications of embeddings document
                var embeddingsDocumentModification = new DynamicJsonValue();
                if (quantization != VectorEmbeddingType.Single)
                    embeddingsDocumentModification[Constants.Documents.Metadata.Quantization] = quantization.ToString();


                // Load the embeddings document (if it exists) to track which attachments need to be removed (on update)
                using var embeddingsDocument =
                    _database.AiIntegrations.Embeddings.Storage.GetEmbeddingDocument(context, document.DocumentId, out string embeddingsDocumentId);
                var currentAttachmentsOfEmbeddingsFromThisTransformer = LoadNamesOfExistingAttachmentsOfThisTransformer(embeddingsDocument);

                foreach (var embeddingsByPath in document.Values)
                {
                    var currentPath = embeddingsByPath.Key;
                    var generatedEmbeddings = embeddingsByPath.Value;
                    var prefix = EmbeddingsHelper.GetPrefixForAttachmentInEmbeddingsDocument(_embeddingsTaskIdentifier, currentPath);
                    var namesOfNewAttachments = new DynamicJsonArray();
                    alreadyAddedAttachments.Clear();

                    foreach (var embedding in generatedEmbeddings)
                    {
                        _database.AiIntegrations.Embeddings.Storage.PutOrUpdateEmbeddingCacheDocument(context, embedding, expireAt,
                            _configuration.EmbeddingsCacheExpiration);
                        embedding.GenerateDestinationAttachmentName(prefix, quantization);

                        if (alreadyAddedAttachments.Add(embedding.DestinationAttachmentName) == false)
                            continue;

                        namesOfNewAttachments.Add(embedding.ValueHash);
                    }

                    embeddingsDocumentModification[currentPath] = namesOfNewAttachments;
                }

                object documentToProcess = embeddingsDocument is null
                    ? CreateEmbeddingDocument(document.DocumentCollectionName, embeddingsDocumentModification)
                    : GetModifiedCurrentDocument(embeddingsDocument, embeddingsDocumentModification);

                using (var reader = GetReader(context, documentToProcess, embeddingsDocumentId, out var isNewDocument))
                {
                    // Update the document
                    _database.DocumentsStorage.Put(context, embeddingsDocumentId, null, reader);

                    // Insert new embeddings
                    foreach (var embeddingsByPath in document.Values)
                    {
                        alreadyAddedAttachments.Clear();

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

                            _database.DocumentsStorage.AttachmentsStorage.CopyAttachment(context, embedding.CacheDocumentId, embedding.ValueHash
                                , embeddingsDocumentId, embedding.DestinationAttachmentName, null, AttachmentType.Document);
                        }
                    }

                    // Remove old embeddings
                    foreach (var embeddingsByPath in currentAttachmentsOfEmbeddingsFromThisTransformer)
                    {
                        foreach (var attachmentToRemove in embeddingsByPath.Value)
                        {
                            _database.DocumentsStorage.AttachmentsStorage.DeleteAttachment(context, embeddingsDocumentId, attachmentToRemove, null, out _,
                                extractCollectionName: false);
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
        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>>
            ToDto(DocumentsOperationContext context)
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
