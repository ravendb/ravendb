using System.Collections;
using System.Collections.Generic;
using Raven.Server.Documents.TimeSeries;

namespace Raven.Server.Documents.ETL.Providers.Raven.Enumerators
{
    public class TimeSeriesToRavenEtlItems : IEnumerator<RavenEtlItem>
    {
        private readonly IEnumerator<TimeSeriesSegmentEntry> _timeSeries;
        private readonly string _collection;

        public TimeSeriesToRavenEtlItems(IEnumerator<TimeSeriesSegmentEntry> timeSeries, string collection)
        {
            _timeSeries = timeSeries;
            _collection = collection;
        }

        public bool MoveNext()
        {
            if (_timeSeries.MoveNext() == false)
                return false;

            Current = new RavenEtlItem(_timeSeries.Current, _collection);
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
