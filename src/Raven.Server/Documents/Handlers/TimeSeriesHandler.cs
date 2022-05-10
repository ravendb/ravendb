using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Documents.Handlers.Processors.Configuration;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class TimeSeriesHandler : DatabaseRequestHandler
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

        public class ExecuteTimeSeriesBatchCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly DocumentDatabase _database;
            private readonly string _documentId;
            private readonly TimeSeriesOperation _operation;
            private readonly bool _fromEtl;

            public string LastChangeVector;

            public ExecuteTimeSeriesBatchCommand(DocumentDatabase database, string documentId, TimeSeriesOperation operation, bool fromEtl)
            {
                _database = database;
                _documentId = documentId;
                _operation = operation;
                _fromEtl = fromEtl;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                string docCollection = GetDocumentCollection(_database, context, _documentId, _fromEtl);

                if (docCollection == null)
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
                            Collection = docCollection,
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
                        docCollection,
                        _operation.Name,
                        _operation.Increments
                    );

                    changes += _operation.Increments.Count;
                }

                if (_operation.Appends?.Count > 0 == false)
                    return changes;

                LastChangeVector = tss.AppendTimestamp(context,
                    _documentId,
                    docCollection,
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

            private static void ThrowMissingDocument(string docId)
            {
                throw new DocumentDoesNotExistException(docId, "Cannot operate on time series of a missing document");
            }

            public static void ThrowArtificialDocument(Document doc)
            {
                throw new InvalidOperationException($"Document '{doc.Id}' has '{nameof(DocumentFlags.Artificial)}' flag set. " +
                                                    "Cannot put TimeSeries on artificial documents.");
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                throw new System.NotImplementedException();
            }
        }

        private static readonly TimeSeriesStorage.AppendOptions AppendOptionsForSmuggler = new TimeSeriesStorage.AppendOptions
        {
            VerifyName = false,
            FromSmuggler = true
        };

        internal class SmugglerTimeSeriesBatchCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly DocumentDatabase _database;

            private readonly Dictionary<string, List<TimeSeriesItem>> _dictionary;

            public string LastChangeVector;

            public SmugglerTimeSeriesBatchCommand(DocumentDatabase database)
            {
                _database = database;
                _dictionary = new Dictionary<string, List<TimeSeriesItem>>();
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var tss = _database.DocumentsStorage.TimeSeriesStorage;

                var changes = 0L;

                foreach (var (docId, items) in _dictionary)
                {
                    var collectionName = _database.DocumentsStorage.ExtractCollectionName(context, items[0].Collection);

                    foreach (var item in items)
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

                    changes += items.Count;
                }

                return changes;
            }

            public void AddToDictionary(TimeSeriesItem item)
            {
                if (_dictionary.TryGetValue(item.DocId, out var itemsList) == false)
                {
                    _dictionary[item.DocId] = itemsList = new List<TimeSeriesItem>();
                }

                itemsList.Add(item);
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                throw new System.NotImplementedException();
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
