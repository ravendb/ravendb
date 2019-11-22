using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;

namespace Raven.Server.Documents.Indexes.Workers.TimeSeries
{
    public sealed class MapTimeSeries : MapItems
    {
        private readonly TimeSeriesStorage _timeSeriesStorage;

        public MapTimeSeries(Index index, TimeSeriesStorage timeSeriesStorage, IndexStorage indexStorage, MapReduceIndexingContext mapReduceContext, IndexingConfiguration configuration)
            : base(index, indexStorage, mapReduceContext, configuration)
        {
            _timeSeriesStorage = timeSeriesStorage;
        }

        protected override IEnumerable<IndexItem> GetItemsEnumerator(DocumentsOperationContext databaseContext, string collection, long lastEtag, int pageSize)
        {
            foreach (var timeSeries in GetTimeSeriesEnumerator(databaseContext, collection, lastEtag, pageSize))
            {
                yield return new IndexItem(timeSeries.DocIdAndName, timeSeries.DocIdAndName, timeSeries.Etag, default, timeSeries.Name, timeSeries.SegmentSize, timeSeries);
            }
        }

        private IEnumerable<TimeSeriesSegmentEntry> GetTimeSeriesEnumerator(DocumentsOperationContext databaseContext, string collection, long lastEtag, int pageSize)
        {
            if (collection == Constants.Documents.Collections.AllDocumentsCollection)
            {
                //return _documentsStorage.GetDocumentsFrom(databaseContext, lastEtag + 1, 0, pageSize);
                throw new NotImplementedException("TODO ppekrol");
            }

            return _timeSeriesStorage.GetTimeSeriesFrom(databaseContext, collection, lastEtag + 1, pageSize);
        }
    }
}
