using System;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL
{
    public class PreventCountersIteratingTooFarEnumerator<T> : IEnumerator<T> where T : ExtractedItem
    {
        private readonly IEnumerator<T> _countersOrCounterTombstones;
        private readonly long _lastProcessedDocEtagInBatch;

        public PreventCountersIteratingTooFarEnumerator(IEnumerator<T> countersOrCounterTombstones, long lastProcessedDocEtagInBatch)
        {
            _countersOrCounterTombstones = countersOrCounterTombstones;
            _lastProcessedDocEtagInBatch = lastProcessedDocEtagInBatch;
        }

        public bool MoveNext()
        {
            Current = null;

            if (_countersOrCounterTombstones.MoveNext() == false)
            {
                Current = null;
                return false;
            }

            Current = _countersOrCounterTombstones.Current;

            if (CanMoveNext(Current.Etag, _lastProcessedDocEtagInBatch))
                return true;

            Current = null;
            return false;
        }

        public static bool CanMoveNext(long itemEtag, long lastProcessedDocEtagInBatch)
        {
            // we need to ensure we iterate counters only up to last processed doc etag in current batch

            if (lastProcessedDocEtagInBatch == 0)
            {
                // there was no document transformed in current batch
                // it means we're done with documents, we can send all counters

                return true;
            }

            if (itemEtag < lastProcessedDocEtagInBatch)
            {
                // don't cross last transformed document
                // transformation of counters is done _after_ transformation of docs
                // we'll send counters with greater etags in next batch

                return true;
            }

            return false;
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }

        public T Current { get; private set; }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }
    }
}
