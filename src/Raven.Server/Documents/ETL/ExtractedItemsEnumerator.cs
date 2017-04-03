using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Server.Documents.ETL.Stats;

namespace Raven.Server.Documents.ETL
{
    public class ExtractedItemsEnumerator<T> : IEnumerator<T> where T : ExtractedItem
    {
        private readonly List<IEnumerator<T>> _workEnumerators = new List<IEnumerator<T>>();
        private T _currentItem;
        private readonly EtlStatsScope _extractionStats;

        public ExtractedItemsEnumerator(EtlStatsScope stats)
        {
            _extractionStats = stats.For(EtlOperations.Extract, start: false);
        }

        public void AddEnumerator(IEnumerator<T> enumerator)
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

            using (_extractionStats.Start())
            {
                var current = _workEnumerators[0];
                for (var index = 1; index < _workEnumerators.Count; index++)
                {
                    if (_workEnumerators[index].Current.Etag < current.Current.Etag)
                    {
                        current = _workEnumerators[index];
                    }
                }

                _currentItem = current.Current;

                if (current.MoveNext() == false)
                {
                    _workEnumerators.Remove(current);
                }

                _extractionStats.RecordExtractedItem();

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
    }
}