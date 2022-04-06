using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Replication.Senders
{
    public class MergedEnumerator<T> : IEnumerator<T> 
    {
        private readonly IComparer<T> _comparer;
        protected  readonly List<IEnumerator<T>> _workEnumerators = new List<IEnumerator<T>>();
        protected T _currentItem;

        public MergedEnumerator(IComparer<T> comparer)
        {
            _comparer = comparer;
        }

        public virtual void AddEnumerator(IEnumerator<T> enumerator)
        {
            if (enumerator == null)
                return;

            if (enumerator.MoveNext())
            {
                _workEnumerators.Add(enumerator);
            }
        }

        public virtual bool MoveNext()
        {
            if (_workEnumerators.Count == 0)
                return false;

            var current = _workEnumerators[0];
            for (var index = 1; index < _workEnumerators.Count; index++)
            {
                if (_comparer.Compare(_workEnumerators[index].Current, current.Current) < 0)
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
            throw new NotSupportedException();
        }

        object IEnumerator.Current => Current;

        public T Current => _currentItem;

        public void Dispose()
        {
            foreach (var workEnumerator in _workEnumerators)
            {
                workEnumerator.Dispose();
            }

            _workEnumerators.Clear();
        }
    }

    public class MergedAsyncEnumerator<T> : IAsyncEnumerator<T> 
    {
        private readonly IComparer<T> _comparer;
        protected  readonly List<IAsyncEnumerator<T>> _workEnumerators = new List<IAsyncEnumerator<T>>();
        protected T _currentItem;
        private IAsyncEnumerator<T> _currentWorker;

        public MergedAsyncEnumerator(IComparer<T> comparer)
        {
            _comparer = comparer;
        }

        public virtual async ValueTask AddAsyncEnumerator(IAsyncEnumerator<T> enumerator)
        {
            if (enumerator == null)
                return;

            if (await enumerator.MoveNextAsync())
            {
                _workEnumerators.Add(enumerator);
            }
        }

        public bool _firstMove = true;

        public virtual async ValueTask<bool> MoveNextAsync()
        {
            if (_workEnumerators.Count == 0)
                return false;

            if (_firstMove == false)
            {
                if (await _currentWorker.MoveNextAsync() == false)
                {
                    _workEnumerators.Remove(_currentWorker);
                }

                if (_workEnumerators.Count == 0)
                    return false;
            }
            _firstMove = false;

            _currentWorker = _workEnumerators[0];
            for (var index = 1; index < _workEnumerators.Count; index++)
            {
                if (_comparer.Compare(_workEnumerators[index].Current, _currentWorker.Current) > 0)
                {
                    _currentWorker = _workEnumerators[index];
                }
            }

            _currentItem = _currentWorker.Current;
            return true;
        }

        public void Reset() => throw new NotSupportedException();

        public T Current => _currentItem;

        public async ValueTask DisposeAsync()
        {
            foreach (var workEnumerator in _workEnumerators)
            {
                await workEnumerator.DisposeAsync();
            }

            _workEnumerators.Clear();
        }
    }
}
