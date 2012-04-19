using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Globalization;
using System.Reactive.Disposables;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Studio.Infrastructure
{
    /// <summary>
    /// Implements a collection that loads its items by pages only when requested
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <remarks>The trick to ensuring that the silverlight datagrid doesn't attempt to enumerate all
    /// items from its DataSource in one shot is to implement both IList and ICollectionView.</remarks>
    public class VirtualCollection<T> : IList<VirtualItem<T>>, IList, ICollectionView, INotifyPropertyChanged, INotifyOnDataFetchErrors where T : class
    {
        private readonly IVirtualCollectionSource<T> _source;
        private readonly int _pageSize;
        public event NotifyCollectionChangedEventHandler CollectionChanged;
        public event EventHandler<DataFetchErrorEventArgs> DataFetchError;
        public event EventHandler<EventArgs> FetchSucceeded;
        public event PropertyChangedEventHandler PropertyChanged;

        private volatile uint _state; // used to ensure that data-requests are not stale
        private readonly SparseList<VirtualItem<T>> _virtualItems;
        private readonly HashSet<int> _fetchedPages = new HashSet<int>();
        private readonly HashSet<int> _requestedPages = new HashSet<int>();
        private int _itemCount;
        private readonly TaskScheduler _synchronizationContextScheduler;
        private bool _isRefreshDeferred;
        private int _currentItem;

        private readonly SortDescriptionCollection _sortDescriptions = new SortDescriptionCollection();

        public VirtualCollection(IVirtualCollectionSource<T> source, int pageSize)
        {
            if (pageSize < 1)
            {
                throw new ArgumentException("pageSize must be bigger than 0");
            }

            _source = source;
            _source.CollectionChanged += HandleSourceCollectionChanged;
            _source.DataFetchError += HandleSourceDataFetchError;
            _pageSize = pageSize;
            _virtualItems = new SparseList<VirtualItem<T>>(DetermineSparseListPageSize(pageSize));
            _currentItem = -1;
            _synchronizationContextScheduler = TaskScheduler.FromCurrentSynchronizationContext();

            (_sortDescriptions as INotifyCollectionChanged).CollectionChanged += HandleSortDescriptionsChanged;
        }

        private void HandleSourceDataFetchError(object sender, DataFetchErrorEventArgs e)
        {
            Task.Factory.StartNew(() => OnDataFetchError(e), CancellationToken.None, TaskCreationOptions.None, _synchronizationContextScheduler);
        }

        private void HandleSortDescriptionsChanged(object sender, NotifyCollectionChangedEventArgs e)
        {
            Refresh();
        }

        private void HandleSourceCollectionChanged(object sender, VirtualCollectionChangedEventArgs e)
        {
            var stateWhenUpdateRequested = _state;
            Task.Factory.StartNew(() => UpdateData(e.Mode, stateWhenUpdateRequested), CancellationToken.None, TaskCreationOptions.None, _synchronizationContextScheduler);
        }

        private int DetermineSparseListPageSize(int fetchPageSize)
        {
            // we don't want the sparse list to have pages that are too small,
            // because that will harm performance by fragmenting the list across memory,
            // but too big, and we'll be wasting lots of space
            const int TargetSparseListPageSize = 100;

            if (fetchPageSize > TargetSparseListPageSize)
            {
                return fetchPageSize;
            }
            else
            {
                // return the smallest multiple of fetchPageSize that is bigger than TargetSparseListPageSize
                return (int)Math.Ceiling((double)TargetSparseListPageSize / fetchPageSize) * fetchPageSize;
            }
        }

        private void MarkExistingItemsAsStale()
        {
            foreach (var page in _fetchedPages)
            {
                var startIndex = page * _pageSize;
                var endIndex = (page + 1) * _pageSize;

                for (int i = startIndex; i < endIndex; i++)
                {
                    if (_virtualItems[i] != null)
                    {
                        _virtualItems[i].IsStale = true;
                    }
                }
            }
        }

        private void BeginGetPage(int page)
        {
            if (IsPageAlreadyRequested(page))
            {
                return;
            }

            _requestedPages.Add(page);

            var stateWhenRequestInitiated = _state;

            _source.GetPageAsync(page*_pageSize, _pageSize, _sortDescriptions).ContinueWith(
                t =>
                    {
                        if (!t.IsFaulted)
                        {
                            UpdatePage(page, t.Result, stateWhenRequestInitiated);
                            OnFetchSucceeded(EventArgs.Empty);
                        }
                        else
                        {
                            OnDataFetchError(new DataFetchErrorEventArgs(t.Exception));
                        }
                    },
                _synchronizationContextScheduler);
        }

        private bool IsPageAlreadyRequested(int page)
        {
            return _fetchedPages.Contains(page) || _requestedPages.Contains(page);
        }

        private void UpdatePage(int page, IList<T> results, uint stateWhenRequested)
        {
            if (stateWhenRequested != _state)
            {
                // this request may contain out-of-date data, so ignore it
                return;
            }

            _requestedPages.Remove(page);
            _fetchedPages.Add(page);

            var startIndex = page * _pageSize;

            for (int i = 0; i < results.Count; i++)
            {
                var index = startIndex + i;
                var virtualItem = _virtualItems[index] ?? (_virtualItems[index] = new VirtualItem<T>(this, index));
                virtualItem.Item = results[i];
            }
        }

        void INotifyOnDataFetchErrors.Retry()
        {
            Refresh();
        }

        public void RealizeItemRequested(int index)
        {
            var page = index / _pageSize;
            BeginGetPage(page);
        }

        public bool Contains(object item)
        {
            if (item is VirtualItem<T>)
            {
                return Contains(item as VirtualItem<T>);
            }
            else
            {
                return false;
            }
        }

        public void Refresh()
        {
            if (!_isRefreshDeferred)
            {
                _source.Refresh();
                OnFetchSucceeded(EventArgs.Empty);
            }
        }

        protected void UpdateData(InterimDataMode mode, uint stateWhenUpdateRequested)
        {
            if (_state != stateWhenUpdateRequested)
            {
                return;
            }

            _state++;

            if (mode == InterimDataMode.ShowStaleData)
            {
                MarkExistingItemsAsStale();
            }
            else
            {
                ClearExistingData();
            }

            _fetchedPages.Clear();
            _requestedPages.Clear();

            UpdateCount();

            OnCollectionChanged(new NotifyCollectionChangedEventArgs(NotifyCollectionChangedAction.Reset));
        }

        private void ClearExistingData()
        {
            foreach (var page in _fetchedPages)
            {
                var startIndex = page * _pageSize;
                var endIndex = (page + 1) * _pageSize;

                for (int i = startIndex; i < endIndex; i++)
                {
                    if (_virtualItems[i] != null)
                    {
                        _virtualItems[i].Item = null;
                    }
                }
            }
        }

        private void UpdateCount()
        {
            if (_itemCount == _source.Count)
            {
                return;
            }

            var wasCurrentBeyondLast = IsCurrentAfterLast;

            _itemCount = _source.Count;

            if (IsCurrentAfterLast && !wasCurrentBeyondLast)
            {
                UpdateCurrentPosition(_itemCount - 1, allowCancel: false);
            }

            OnPropertyChanged(new PropertyChangedEventArgs("Count"));
        }

        public int IndexOf(VirtualItem<T> item)
        {
            return item.Index;
        }

        object IList.this[int index]
        {
            get { return this[index]; }
            set { throw new NotImplementedException(); }
        }

        public VirtualItem<T> this[int index]
        {
            get
            {
                RealizeItemRequested(index);
                return _virtualItems[index] ?? (_virtualItems[index] = new VirtualItem<T>(this, index));
            }
            set { throw new NotImplementedException(); }
        }

        public IDisposable DeferRefresh()
        {
            _isRefreshDeferred = true;

            return Disposable.Create(() => { _isRefreshDeferred = false; Refresh(); });
        }

        public bool MoveCurrentToFirst()
        {
            return UpdateCurrentPosition(0);
        }

        public bool MoveCurrentToLast()
        {
            return UpdateCurrentPosition(_itemCount - 1);
        }

        public bool MoveCurrentToNext()
        {
            return UpdateCurrentPosition(CurrentPosition + 1);
        }

        public bool MoveCurrentToPrevious()
        {
            return UpdateCurrentPosition(CurrentPosition - 1);
        }

        public bool MoveCurrentTo(object item)
        {
            return MoveCurrentToPosition(((IList)this).IndexOf(item));
        }

        public bool MoveCurrentToPosition(int position)
        {
            return UpdateCurrentPosition(position);
        }

        public CultureInfo Culture { get; set; }

        public IEnumerable SourceCollection
        {
            get { return this; }
        }

        public Predicate<object> Filter
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        public bool CanFilter
        {
            get { return false; }
        }

        public SortDescriptionCollection SortDescriptions
        {
            get { return _sortDescriptions; }
        }

        public bool CanSort
        {
            get { return true; }
        }

        public bool CanGroup
        {
            get { return false; }
        }

        public ObservableCollection<GroupDescription> GroupDescriptions
        {
            get { throw new NotImplementedException(); }
        }

        public ReadOnlyObservableCollection<object> Groups
        {
            get { throw new NotImplementedException(); }
        }

        public bool IsEmpty
        {
            get { return _itemCount == 0; }
        }

        public object CurrentItem
        {
            get { return 0 < CurrentPosition && CurrentPosition < _itemCount ? this[CurrentPosition] : null; }
        }

        public int CurrentPosition
        {
            get { return _currentItem; }
            private set
            {
                _currentItem = value;
                OnCurrentChanged(EventArgs.Empty);
            }
        }

        private bool UpdateCurrentPosition(int newCurrentPosition, bool allowCancel = true)
        {
            var changingEventArgs = new CurrentChangingEventArgs(allowCancel);

            OnCurrentChanging(changingEventArgs);

            if (!changingEventArgs.Cancel)
            {
                CurrentPosition = newCurrentPosition;
            }

            return !IsCurrentBeforeFirst && !IsCurrentAfterLast;
        }

        public bool IsCurrentAfterLast
        {
            get { return CurrentPosition >= _itemCount; }
        }

        public bool IsCurrentBeforeFirst
        {
            get { return CurrentPosition < 0; }
        }

        public event CurrentChangingEventHandler CurrentChanging;

        protected void OnCurrentChanging(CurrentChangingEventArgs e)
        {
            CurrentChangingEventHandler handler = CurrentChanging;
            if (handler != null) handler(this, e);
        }

        public event EventHandler CurrentChanged;

        protected void OnCurrentChanged(EventArgs e)
        {
            EventHandler handler = CurrentChanged;
            if (handler != null) handler(this, e);
        }

        public IEnumerator<VirtualItem<T>> GetEnumerator()
        {
            for (int i = 0; i < _itemCount; i++)
            {
                yield return this[i];
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public bool Contains(VirtualItem<T> item)
        {
            return item.Parent == this;
        }

        public void CopyTo(VirtualItem<T>[] array, int arrayIndex)
        {
            throw new NotImplementedException();
        }

        void ICollection.CopyTo(Array array, int index)
        {
            throw new NotImplementedException();
        }

        public int Count
        {
            get { return _itemCount; }
        }

        object ICollection.SyncRoot
        {
            get { throw new NotImplementedException(); }
        }

        bool ICollection.IsSynchronized
        {
            get { throw new NotImplementedException(); }
        }

        public bool IsReadOnly
        {
            get { return true; }
        }

        bool IList.IsFixedSize
        {
            get { throw new NotImplementedException(); }
        }

        #region Not Implemented IList methods


        protected void OnCollectionChanged(NotifyCollectionChangedEventArgs e)
        {
            NotifyCollectionChangedEventHandler handler = CollectionChanged;
            if (handler != null) handler(this, e);
        }

        protected void OnPropertyChanged(PropertyChangedEventArgs e)
        {
            PropertyChangedEventHandler handler = PropertyChanged;
            if (handler != null) handler(this, e);
        }

        protected void OnDataFetchError(DataFetchErrorEventArgs e)
        {
            EventHandler<DataFetchErrorEventArgs> handler = DataFetchError;
            if (handler != null) handler(this, e);
        }

        protected void OnFetchSucceeded(EventArgs e)
        {
            EventHandler<EventArgs> handler = FetchSucceeded;
            if (handler != null) handler(this, e);
        }


        public void Add(VirtualItem<T> item)
        {
            throw new NotImplementedException();
        }

        int IList.Add(object value)
        {
            throw new NotImplementedException();
        }

        bool IList.Contains(object value)
        {
            if (value is VirtualItem<T>)
            {
                return Contains(value as VirtualItem<T>);
            }
            else
            {
                return false;
            }
        }

        public void Clear()
        {
            throw new NotImplementedException();
        }

        int IList.IndexOf(object value)
        {
            var virtualItem = value as VirtualItem<T>;
            if (virtualItem == null)
            {
                return -1;
            }
            else
            {
                return virtualItem.Index;
            }
        }

        void IList.Insert(int index, object value)
        {
            throw new NotImplementedException();
        }

        void IList.Remove(object value)
        {
            throw new NotImplementedException();
        }

        public bool Remove(VirtualItem<T> item)
        {
            throw new NotImplementedException();
        }

        public void Insert(int index, VirtualItem<T> item)
        {
            throw new NotImplementedException();
        }

        public void RemoveAt(int index)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
