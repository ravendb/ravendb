using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Server.Documents.ETL.Providers.Raven.Enumerators;
using Raven.Server.Documents.ETL.Stats;

namespace Raven.Server.Documents.ETL
{
    public class ExtractedItemsEnumerator<T> : IEnumerator<T>, IEnumerable<T> where T : ExtractedItem
    {
        private readonly List<IExtractEnumerator<T>> _workEnumerators = new List<IExtractEnumerator<T>>();
        private T _currentItem;
        private readonly EtlStatsScope _extractionStats;

        public ExtractedItemsEnumerator(EtlStatsScope stats)
        {
            _extractionStats = stats.For(EtlOperations.Extract, start: false);
        }

        public void AddEnumerator(IExtractEnumerator<T> enumerator)
        {
            if (enumerator == null)
                return;

            using (_extractionStats.Start())
            {
                if (enumerator.MoveNext())
                {
                    _workEnumerators.Add(enumerator);
                }
            }
        }

        public bool MoveNext()
        {
            if (_workEnumerators.Count == 0)
                return false;

            var fetch = true;
            using (_extractionStats.Start())
            {
                while (fetch)
                {
                    if (_workEnumerators.Count == 0)
                        return false;

                    fetch = false;
                    var enumerator = _workEnumerators[0];
                    for (var index = 1; index < _workEnumerators.Count; index++)
                    {
                        if (_workEnumerators[index].Current.Etag < enumerator.Current.Etag)
                        {
                            enumerator = _workEnumerators[index];
                        }
                    }

                    _currentItem = enumerator.Current;

                    if (enumerator.Filter())
                    {
                        _extractionStats.RecordChangeVector(_currentItem.ChangeVector);
                        _extractionStats.RecordLastFilteredOutEtag(_currentItem.Etag, _currentItem.Type);
                        fetch = true;
                    }

                    if (enumerator.MoveNext()) 
                        continue;

                    _workEnumerators.Remove(enumerator);
                }

                _extractionStats.RecordExtractedItem(_currentItem.Type);
                return true;
            }
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public T Current => _currentItem;

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            foreach (var workEnumerator in _workEnumerators)
            {
                workEnumerator.Dispose();
            }
            _workEnumerators.Clear();
        }

        public IEnumerator<T> GetEnumerator()
        {
            while (MoveNext())
            {
                yield return _currentItem;
            } 
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
