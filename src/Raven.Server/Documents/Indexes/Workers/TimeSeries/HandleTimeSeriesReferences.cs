using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Server.Utils;
using Voron;

namespace Raven.Server.Documents.Indexes.Workers.TimeSeries
{
    public sealed class HandleTimeSeriesReferences : HandleReferences
    {
        private readonly TimeSeriesStorage _timeSeriesStorage;

        protected override ReferenceType Type => ReferenceType.TimeSeries;

        public HandleTimeSeriesReferences(Index index, Dictionary<string, HashSet<CollectionName>> referencedCollections, TimeSeriesStorage timeSeriesStorage, DocumentsStorage documentsStorage, IndexStorage indexStorage, Config.Categories.IndexingConfiguration configuration)
            : base(index, referencedCollections, documentsStorage, indexStorage, indexStorage.ReferencesForDocuments, configuration)
        {
            _timeSeriesStorage = timeSeriesStorage;
        }

        protected override IndexItem GetItem(DocumentsOperationContext databaseContext, Slice key)
        {
            var timeSeries = _timeSeriesStorage.GetTimeSeries(databaseContext, key);
            if (timeSeries == null)
                return null;

            return new TimeSeriesIndexItem(timeSeries.LuceneKey, timeSeries.DocId, timeSeries.Etag, default, timeSeries.Name, timeSeries.SegmentSize, timeSeries);
        }

        public override void HandleDelete(Tombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            using (DocumentIdWorker.GetSliceFromId(indexContext, tombstone.LowerId, out Slice documentIdPrefixWithTsKeySeparator, SpecialChars.RecordSeparator))
                _referencesStorage.RemoveReferencesByPrefix(documentIdPrefixWithTsKeySeparator, collection, null, indexContext.Transaction);
        }
    }
}
