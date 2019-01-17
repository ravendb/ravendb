using System.Collections;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL
{
    public class FilterCountersEnumerator : IEnumerator<CounterGroupDetail>
    {
        private readonly IEnumerator<CounterGroupDetail> _counters;
        private readonly EtlStatsScope _stats;
        private readonly DocumentsStorage _docsStorage;
        private readonly DocumentsOperationContext _context;

        public FilterCountersEnumerator(IEnumerator<CounterGroupDetail> counters, EtlStatsScope stats, DocumentsStorage docsStorage, DocumentsOperationContext context)
        {
            _counters = counters;
            _stats = stats;
            _docsStorage = docsStorage;
            _context = context;
        }

        public bool MoveNext()
        {
            Current = null;

            while (_counters.MoveNext())
            {
                var current = _counters.Current;

                var doc = _docsStorage.Get(_context, current.CounterKey);

                if (doc != null && current.Etag > doc.Etag)
                {
                    Current = current;
                    return true;
                }

                _stats.RecordChangeVector(current.ChangeVector);
                _stats.RecordLastFilteredOutEtag(current.Etag, EtlItemType.Counter);
            }

            return false;
        }

        public void Reset()
        {
            throw new System.NotImplementedException();
        }

        public CounterGroupDetail Current { get; private set; }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }
    }
}
