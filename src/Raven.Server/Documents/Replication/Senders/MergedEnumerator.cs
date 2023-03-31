using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Replication.Senders
{
    public class MergedEnumerator<T> : IEnumerator<T>
    {
        protected readonly IComparer<T> Comparer;
        protected readonly List<IEnumerator<T>> WorkEnumerators = new();
        protected T CurrentItem;

        protected IEnumerator<T> CurrentEnumerator;

        public MergedEnumerator(IComparer<T> comparer)
        {
            Comparer = comparer;
        }

        public virtual void AddEnumerator(IEnumerator<T> enumerator)
        {
            if (enumerator == null)
                return;

            if (enumerator.MoveNext())
            {
                WorkEnumerators.Add(enumerator);
            }
            else
            {
                enumerator.Dispose();
            }
        }

        public virtual bool MoveNext()
        {
            if (CurrentEnumerator != null)
            {
                if (CurrentEnumerator.MoveNext() == false)
                {
                    using (CurrentEnumerator)
                    {
                        WorkEnumerators.Remove(CurrentEnumerator);
                        CurrentEnumerator = null;
                    }
                }
            }

            if (WorkEnumerators.Count == 0)
                return false;

            CurrentEnumerator = WorkEnumerators[0];
            for (var index = 1; index < WorkEnumerators.Count; index++)
            {
                if (Comparer.Compare(WorkEnumerators[index].Current, CurrentEnumerator.Current) < 0)
                {
                    CurrentEnumerator = WorkEnumerators[index];
                }
            }

            CurrentItem = CurrentEnumerator.Current;

            return true;
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }

        object IEnumerator.Current => Current;

        public T Current => CurrentItem;

        public void Dispose()
        {
            foreach (var workEnumerator in WorkEnumerators)
            {
                workEnumerator.Dispose();
            }

            WorkEnumerators.Clear();
        }
    }

    public sealed class MergedAsyncEnumerator<T> : IAsyncEnumerator<T>
    {
        private readonly IComparer<T> _comparer;
        private readonly List<IAsyncEnumerator<T>> _workEnumerators = new();
        private T _currentItem;

        private IAsyncEnumerator<T> _currentEnumerator;

        public MergedAsyncEnumerator(IComparer<T> comparer)
        {
            _comparer = comparer;
        }

        public async ValueTask AddAsyncEnumerator(IAsyncEnumerator<T> enumerator)
        {
            if (enumerator == null)
                return;

            if (await enumerator.MoveNextAsync())
            {
                _workEnumerators.Add(enumerator);
            }
            else
            {
                await enumerator.DisposeAsync();
            }
        }

        public async ValueTask<bool> MoveNextAsync()
        {
            if (_currentEnumerator != null)
            {
                if (await _currentEnumerator.MoveNextAsync() == false)
                {
                    await using (_currentEnumerator)
                    {
                        _workEnumerators.Remove(_currentEnumerator);
                        _currentEnumerator = null;
                    }
                }
            }

            if (_workEnumerators.Count == 0)
                return false;

            _currentEnumerator = _workEnumerators[0];
            for (var index = 1; index < _workEnumerators.Count; index++)
            {
                if (_comparer.Compare(_workEnumerators[index].Current, _currentEnumerator.Current) < 0)
                {
                    _currentEnumerator = _workEnumerators[index];
                }
            }

            _currentItem = _currentEnumerator.Current;

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
