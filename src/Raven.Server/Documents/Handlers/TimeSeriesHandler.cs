using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Handlers.Processors.Configuration;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Sparrow.Json;
using System.Diagnostics.CodeAnalysis;
using Raven.Client.Documents.Smuggler;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public sealed class TimeSeriesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/timeseries/stats", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Stats()
        {
            using (var processor = new TimeSeriesHandlerProcessorForGetTimeSeriesStats(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenAction("/databases/*/timeseries/ranges", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task ReadRanges()
        {
            using (var processor = new TimeSeriesHandlerProcessorForGetTimeSeriesRanges(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/timeseries/ranges", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task ReadRangesPost()
        {
            var start = GetStart();
            var pageSize = GetPageSize();

            var returnFullResults = GetBoolValueQueryString("full", required: false) ?? false;

            var result = new GetMultipleTimeSeriesRangesCommand.Response()
            {
                Results = new List<TimeSeriesDetails>()
            };

            using (var token = CreateHttpRequestBoundOperationToken())
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var timeSeriesBlittable = await context.ReadForMemoryAsync(RequestBodyStream(), "timeseries");

                var timeSeries = JsonDeserializationClient.TimeSeriesRangesRequestBody(timeSeriesBlittable);

                foreach (var (docId, ranges) in timeSeries.RangesPerDocumentId)
                {
                    var rangeResultDictionary = TimeSeriesHandlerProcessorForGetTimeSeriesRanges.GetTimeSeriesRangeResults(context, docId, ranges, start, pageSize, null, returnFullResults);

                    result.Results.Add(new TimeSeriesDetails()
                    {
                        Id = docId,
                        Values = rangeResultDictionary
                    });
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    {
                        writer.WritePropertyName(nameof(GetMultipleTimeSeriesRangesCommand.Response.Results));
                        writer.WriteStartArray();
                        {
                            var first = true;

                            foreach (var ranges in result.Results)
                            {
                                if (first == false)
                                    writer.WriteComma();
                                first = false;

                                writer.WriteStartObject();

                                writer.WritePropertyName(nameof(TimeSeriesDetails.Id));
                                writer.WriteString(ranges.Id);

                                writer.WriteComma();
                                writer.WritePropertyName(nameof(TimeSeriesDetails.Values));
                                await TimeSeriesHandlerProcessorForGetTimeSeriesRanges.WriteTimeSeriesRangeResultsAsync(context, writer, ranges.Id, ranges.Values, false, token.Token);

                                writer.WriteEndObject();
                            }
                        }
                        writer.WriteEndArray();
                    }
                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/databases/*/timeseries", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Read()
        {
            using (var processor = new TimeSeriesHandlerProcessorForGetTimeSeries(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/timeseries", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Batch()
        {
            using (var processor = new TimeSeriesHandlerProcessorForPostTimeSeries(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenAction("/databases/*/timeseries/config", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetTimeSeriesConfiguration()
        {
            using (var processor = new ConfigurationHandlerProcessorForGetTimeSeriesConfiguration(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/timeseries/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task PostTimeSeriesConfiguration()
        {
            using (var processor = new ConfigurationHandlerProcessorForPostTimeSeriesConfiguration(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/timeseries/names/config", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task ConfigTimeSeriesNames()
        {
            using (var processor = new TimeSeriesHandlerProcessorForPostTimeSeriesNamesConfiguration(this))
                await processor.ExecuteAsync();
        }

        public sealed class ExecuteTimeSeriesBatchCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
        {
            private readonly DocumentDatabase _database;
            private readonly string _documentId;
            private readonly TimeSeriesOperation _operation;
            private readonly bool _fromEtl;

            public string LastChangeVector;
            public string DocCollection;

            public ExecuteTimeSeriesBatchCommand(DocumentDatabase database, string documentId, TimeSeriesOperation operation, bool fromEtl)
            {
                _database = database;
                _documentId = documentId;
                _operation = operation;
                _fromEtl = fromEtl;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                DocCollection = GetDocumentCollection(_database, context, _documentId, _fromEtl);

                if (DocCollection == null)
                    return 0L;

                var changes = 0L;
                var tss = _database.DocumentsStorage.TimeSeriesStorage;

                if (_operation.Deletes?.Count > 0)
                {
                    foreach (var removal in _operation.Deletes)
                    {
                        var deletionRange = new TimeSeriesStorage.DeletionRangeRequest
                        {
                            DocumentId = _documentId,
                            Collection = DocCollection,
                            Name = _operation.Name,
                            From = removal.From ?? DateTime.MinValue,
                            To = removal.To ?? DateTime.MaxValue
                        };

                        LastChangeVector = tss.DeleteTimestampRange(context, deletionRange);

                        changes++;
                    }
                }

                if (_operation.Increments?.Count > 0)
                {
                    LastChangeVector = tss.IncrementTimestamp(context,
                        _documentId,
                        DocCollection,
                        _operation.Name,
                        _operation.Increments
                    );

                    changes += _operation.Increments.Count;
                }

                if (_operation.Appends?.Count > 0 == false)
                    return changes;

                LastChangeVector = tss.AppendTimestamp(context,
                    _documentId,
                    DocCollection,
                    _operation.Name,
                    _operation.Appends
                );

                changes += _operation.Appends.Count;

                return changes;
            }

            public static string GetDocumentCollection(DocumentDatabase database, DocumentsOperationContext context, string documentId, bool fromEtl)
            {
                try
                {
                    var doc = database.DocumentsStorage.Get(context, documentId, throwOnConflict: true);
                    if (doc == null)
                    {
                        if (fromEtl)
                            return null;

                        ThrowMissingDocument(documentId);
                        return null;// never hit
                    }

                    if (doc.Flags.HasFlag(DocumentFlags.Artificial))
                        ThrowArtificialDocument(doc);

                    return CollectionName.GetCollectionName(doc.Data);
                }
                catch (DocumentConflictException)
                {
                    if (fromEtl)
                        return null;

                    // this is fine, we explicitly support
                    // setting the flag if we are in conflicted state is
                    // done by the conflict resolver

                    // avoid loading same document again, we validate write using the metadata instance
                    return database.DocumentsStorage.ConflictsStorage.GetCollection(context, documentId);
                }
            }

            [DoesNotReturn]
            private static void ThrowMissingDocument(string docId)
            {
                throw new DocumentDoesNotExistException(docId, "Cannot operate on time series of a missing document");
            }

            [DoesNotReturn]
            public static void ThrowArtificialDocument(Document doc)
            {
                throw new InvalidOperationException($"Document '{doc.Id}' has '{nameof(DocumentFlags.Artificial)}' flag set. " +
                                                    "Cannot put TimeSeries on artificial documents.");
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
            {
                throw new System.NotImplementedException();
            }
        }

        private static readonly TimeSeriesStorage.AppendOptions AppendOptionsForSmuggler = new TimeSeriesStorage.AppendOptions
        {
            VerifyName = false,
            FromSmuggler = true
        };

        public sealed class SmugglerTimeSeriesBatchCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>, IDisposable
        {
            private readonly DocumentDatabase _database;

            private readonly Dictionary<string, List<TimeSeriesItem>> _dictionary;

            private readonly Dictionary<string, List<TimeSeriesDeletedRangeItemForSmuggler>> _deletedRanges;

            private readonly DocumentsOperationContext _context;

            public DocumentsOperationContext Context => _context;

            private IDisposable _releaseContext;

            private bool _isDisposed;

            private readonly List<IDisposable> _toDispose;

            private readonly List<AllocatedMemoryData> _toReturn;

            public string LastChangeVector;

            private BlittableJsonDocumentBuilder _builder;
            private BlittableMetadataModifier _metadataModifier;

            public SmugglerTimeSeriesBatchCommand(DocumentDatabase database)
            {
                _database = database;
                _dictionary = new Dictionary<string, List<TimeSeriesItem>>(StringComparer.OrdinalIgnoreCase);
                _deletedRanges = new Dictionary<string, List<TimeSeriesDeletedRangeItemForSmuggler>>(StringComparer.OrdinalIgnoreCase);
                _toDispose = new();
                _toReturn = new();
                _releaseContext = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var tss = _database.DocumentsStorage.TimeSeriesStorage;

                var changes = 0L;

                foreach (var (docId, items) in _deletedRanges)
                {
                    foreach (var item in items)
                    {
                        using (item)
                        {
                            var deletionRangeRequest = new TimeSeriesStorage.DeletionRangeRequest
                            {
                                DocumentId = docId,
                                Collection = item.Collection,
                                Name = item.Name,
                                From = item.From,
                                To = item.To
                            };
                            tss.DeleteTimestampRange(context, deletionRangeRequest, remoteChangeVector: null, updateMetadata: false);
                        }
                    }

                    changes += items.Count;
                }

                foreach (var (docId, items) in _dictionary)
                {
                    var collectionName = _database.DocumentsStorage.ExtractCollectionName(context, items[0].Collection);

                    foreach (var item in items)
                    {
                        using (item)
                        {
                            using (var slicer = new TimeSeriesSliceHolder(context, docId, item.Name).WithBaseline(item.Baseline))
                            {
                                if (tss.TryAppendEntireSegmentFromSmuggler(context, slicer.TimeSeriesKeySlice, collectionName, item))
                                {
                                    // on import we remove all @time-series from the document, so we need to re-add them
                                    tss.AddTimeSeriesNameToMetadata(context, item.DocId, item.Name, NonPersistentDocumentFlags.FromSmuggler);
                                    continue;
                                }
                            }

                            var values = item.Segment.YieldAllValues(context, context.Allocator, item.Baseline);
                            tss.AppendTimestamp(context, docId, item.Collection, item.Name, values, AppendOptionsForSmuggler);
                        }
                    }

                    changes += items.Count;
                }

                return changes;
            }

            public bool AddToDictionary(TimeSeriesItem item)
            {
                bool newItem = false;
                if (_dictionary.TryGetValue(item.DocId, out var itemsList) == false)
                {
                    _dictionary[item.DocId] = itemsList = new List<TimeSeriesItem>();
                    newItem = true;
                }

                itemsList.Add(item);
                return newItem;
            }

            public bool AddToDeletedRanges(TimeSeriesDeletedRangeItemForSmuggler item)
            {
                bool newItem = false;

                if (_deletedRanges.TryGetValue(item.DocId, out var deletedRangesList) == false)
                {
                    _deletedRanges[item.DocId] = deletedRangesList = [];
                    newItem = true;
                }

                deletedRangesList.Add(item);
                return newItem;
            }


            public void AddToDisposal(IDisposable disposable)
            {
                _toDispose.Add(disposable);
            }

            public void AddToReturn(AllocatedMemoryData allocatedMemoryData)
            {
                _toReturn.Add(allocatedMemoryData);
            }

            public BlittableJsonDocumentBuilder GetOrCreateBuilder(UnmanagedJsonParser parser, JsonParserState state, string debugTag, BlittableMetadataModifier modifier = null)
            {
                return _builder ??= new BlittableJsonDocumentBuilder(_context, BlittableJsonDocumentBuilder.UsageMode.ToDisk, debugTag, parser, state, modifier: modifier);
            }

            public BlittableMetadataModifier GetOrCreateMetadataModifier(string firstEtagOfLegacyRevision = null, long legacyRevisionsCount = 0, bool legacyImport = false,
                bool readLegacyEtag = false, DatabaseItemType operateOnTypes = DatabaseItemType.None)
            {
                _metadataModifier ??= new BlittableMetadataModifier(_context, legacyImport, readLegacyEtag, operateOnTypes);
                _metadataModifier.FirstEtagOfLegacyRevision = firstEtagOfLegacyRevision;
                _metadataModifier.LegacyRevisionsCount = legacyRevisionsCount;

                return _metadataModifier;
            }

            public void Dispose()
            {
                if (_isDisposed)
                    return;

                _isDisposed = true;

                foreach (var disposable in _toDispose)
                {
                    disposable.Dispose();
                }
                _toDispose.Clear();

                foreach (var returnable in _toReturn)
                    _context.ReturnMemory(returnable);
                _toReturn.Clear();

                _builder?.Dispose();
                _metadataModifier?.Dispose();

                _releaseContext?.Dispose();
                _releaseContext = null;
            }
            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
            {
                throw new NotImplementedException();
            }
        }

        [RavenAction("/databases/*/timeseries/debug/segments-summary", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetSegmentSummary()
        {
            using (var processor = new TimeSeriesHandlerProcessorForGetDebugSegmentsSummary(this))
                await processor.ExecuteAsync();
        }
    }
}
