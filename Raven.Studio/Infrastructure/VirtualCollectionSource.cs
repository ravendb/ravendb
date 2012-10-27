using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Studio.Infrastructure
{
    public abstract class VirtualCollectionSource<T> : IVirtualCollectionSource<T>, INotifyBusyness
    {
        private readonly object lockObject = new object();
        public event EventHandler<VirtualCollectionSourceChangedEventArgs> CollectionChanged;
        public event EventHandler<EventArgs> CountChanged;
        public event EventHandler<EventArgs> IsBusyChanged;

        private int? _count;
        private bool _isBusy;
        private int _outstandingTasks;

        public virtual int? Count
        {
            get
            {
                lock (lockObject)
                {
                    return _count;
                }
            }
        }

        public bool IsBusy
        {
            get
            {
                lock(lockObject)
                {
                    return _isBusy;
                }
            }
            private set
            {
                bool hasChanged;

                lock (lockObject)
                {
                    hasChanged = (_isBusy != value);
                    _isBusy = value;
                }

                if (hasChanged)
                {
                    OnIsBusyChanged(EventArgs.Empty);
                }
            }
        }

        public Task<IList<T>> GetPageAsync(int start, int pageSize, IList<SortDescription> sortDescriptions)
        {
            IncrementOutstandingTasks();

            return GetPageAsyncOverride(start, pageSize, sortDescriptions)
                .ContinueWith(t =>
                                  {
                                      DecrementOutstandingTasks();
                                      return t.Result;
                                  }, TaskContinuationOptions.ExecuteSynchronously);
        }

        protected abstract Task<IList<T>> GetPageAsyncOverride(int start, int pageSize,
                                                               IList<SortDescription> sortDescriptions);

        protected void IncrementOutstandingTasks()
        {
            IsBusy = Interlocked.Increment(ref _outstandingTasks) > 0;
        }

        protected void DecrementOutstandingTasks()
        {
            IsBusy = Interlocked.Decrement(ref _outstandingTasks) > 0;
        }

        public void Refresh(RefreshMode mode)
        {
            InvalidateCount();
            if (mode == RefreshMode.ClearStaleData)
            {
                OnCollectionChanged(new VirtualCollectionSourceChangedEventArgs(ChangeType.Reset));
            }
            else
            {
                OnCollectionChanged(new VirtualCollectionSourceChangedEventArgs(ChangeType.Refresh));
            }
        }

        protected void OnCollectionChanged(VirtualCollectionSourceChangedEventArgs e)
        {
            var handler = CollectionChanged;
            if (handler != null) handler(this, e);
        }

        protected void OnIsBusyChanged(EventArgs e)
        {
            var handler = IsBusyChanged;
            if (handler != null) handler(this, e);
        }

        protected void SetCount(int newCount)
        {
            bool fileCountChanged;

            lock (lockObject)
            {
                fileCountChanged = newCount != _count;
                _count = newCount;
            }

            if (fileCountChanged)
            {
                OnCountChanged(EventArgs.Empty);
            }
        }

        protected void InvalidateCount()
        {
            lock (lockObject)
            {
                _count = null;
            }
        }

        protected void OnCountChanged(EventArgs e)
        {
            EventHandler<EventArgs> handler = CountChanged;
            if (handler != null) handler(this, e);
        }

    }
}