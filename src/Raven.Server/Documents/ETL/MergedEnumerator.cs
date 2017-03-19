using System;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL
{
    public class MergedEnumerator<T> : IEnumerator<T> where T : ExtractedItem
    {
        private readonly List<IEnumerator<T>> _workEnumerators = new List<IEnumerator<T>>();
        private T _currentItem;

        public void AddEnumerator(IEnumerator<T> enumerator)
        {
            if (enumerator == null)
                return;

            if (enumerator.MoveNext())
            {
                _workEnumerators.Add(enumerator);
            }
        }

        public bool MoveNext()
        {
            if (_workEnumerators.Count == 0)
                return false;

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
            return true;
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