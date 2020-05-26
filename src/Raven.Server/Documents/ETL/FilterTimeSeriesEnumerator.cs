using System.Collections;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL
{
    public class FilterTimeSeriesEnumerator : IEnumerator<TimeSeriesSegmentEntry>
    {
        private readonly IEnumerator<TimeSeriesSegmentEntry> _timeSeries;
        private readonly EtlStatsScope _stats;
        private readonly DocumentsStorage _docsStorage;
        private readonly DocumentsOperationContext _context;
        private readonly long _lastProcessedDocEtagInBatch;

        public FilterTimeSeriesEnumerator(IEnumerator<TimeSeriesSegmentEntry> timeSeries, EtlStatsScope stats, DocumentsStorage docsStorage, DocumentsOperationContext context, long lastProcessedDocEtagInBatch)
        {
            _timeSeries = timeSeries;
            _stats = stats;
            _docsStorage = docsStorage;
            _context = context;
            _lastProcessedDocEtagInBatch = lastProcessedDocEtagInBatch;
        }

        public bool MoveNext()
        {
            Current = null;

            while (_timeSeries.MoveNext())
            {
                var current = _timeSeries.Current;

                if (CanMoveNextAndNotExceedLastDocumentInBatch(current.Etag, _lastProcessedDocEtagInBatch) == false)
                    return false;

                var doc = _docsStorage.Get(_context, current.DocId);

                if (doc != null && current.Etag > doc.Etag)
                {
                    Current = current;
                    return true;
                }

                //TODO check relevant to time series 
                // Time series has lower etag than its document - we skip it to avoid
                // sending a time series of document that can not exist on the destination side
                
                _stats.RecordChangeVector(current.ChangeVector);
                _stats.RecordLastFilteredOutEtag(current.Etag, EtlItemType.TimeSeriesSegment);
            }

            return false;
        }

        private static bool CanMoveNextAndNotExceedLastDocumentInBatch(long itemEtag, long lastProcessedDocEtagInBatch)
        {
            // we need to ensure we iterate counters only up to last processed doc etag in current batch

            if (lastProcessedDocEtagInBatch == 0)
            {
                // there was no document transformed in current batch
                // it means we're done with documents, we can send all time series

                return true;
            }

            if (itemEtag < lastProcessedDocEtagInBatch)
            {
                // don't cross last transformed document
                // transformation of time series is done _after_ transformation of docs
                // we'll send time series with greater etags in next batch

                return true;
            }

            return false;
        }

        public void Reset()
        {
            throw new System.NotImplementedException();
        }

        public TimeSeriesSegmentEntry Current { get; private set; }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }
    }
}
