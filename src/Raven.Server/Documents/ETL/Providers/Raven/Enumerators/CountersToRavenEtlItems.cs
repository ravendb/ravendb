using System.Collections;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Raven.Enumerators
{
    public class CountersToRavenEtlItems : IEnumerator<RavenEtlItem>
    {
        private readonly DocumentsOperationContext _context;
        private readonly IEnumerator<CounterGroupDetail> _counters;
        private readonly string _collection;

        public CountersToRavenEtlItems(DocumentsOperationContext context, IEnumerator<CounterGroupDetail> counters, string collection)
        {
            _context = context;
            _counters = counters;
            _collection = collection;
        }

        private bool Filter(RavenEtlItem item)
        {
            var doc = _context.DocumentDatabase.DocumentsStorage.Get(_context, item.DocumentId, DocumentFields.Default);

            // counter has lower etag than its document - we skip it to avoid
            // sending a counter of document that can not exist on the destination side
            return doc == null || item.Etag <= doc.Etag;
        }

        public bool MoveNext()
        {
            if (_counters.MoveNext() == false)
                return false;

            Current = new RavenEtlItem(_counters.Current, _collection);
            Current.Filtered = Filter(Current);

            return true;
        }

        public void Reset()
        {
            throw new System.NotImplementedException();
        }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }

        public RavenEtlItem Current { get; private set; }
    }
}
