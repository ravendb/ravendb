using System.Collections;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Counters;

namespace Raven.Server.Documents.ETL.Providers.Raven.Enumerators
{
    public class CountersToRavenEtlItems : IEnumerator<RavenEtlItem>
    {
        private readonly IEnumerator<CounterDetail> _counters;
        private readonly string _collection;

        public CountersToRavenEtlItems(IEnumerator<CounterDetail> counters, string collection)
        {
            _counters = counters;
            _collection = collection;
        }

        public bool MoveNext()
        {
            if (_counters.MoveNext() == false)
                return false;

            Current = new RavenEtlItem(_counters.Current, _collection);

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
