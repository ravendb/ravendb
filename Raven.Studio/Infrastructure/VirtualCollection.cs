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
using System.Linq;
using VirtualCollection.VirtualCollection;

namespace Raven.Studio.Infrastructure
{
    /// <summary>
    /// Implements a collection that loads its items by pages only when requested
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <remarks>The trick to ensuring that the silverlight datagrid doesn't attempt to enumerate all
    /// items from its DataSource in one shot is to implement both IList and ICollectionView.</remarks>
    public class VirtualCollection<T> : IList<VirtualItem<T>>, IList, ICollectionView, INotifyPropertyChanged,
                                        IEnquireAboutItemVisibility where T : class
    {
        private const int IndividualItemNotificationLimit = 100;
        private const int MaxConcurrentPageRequests = 4;

        public event NotifyCollectionChangedEventHandler CollectionChanged;

        public event PropertyChangedEventHandler PropertyChanged;
        public event EventHandler<QueryItemVisibilityEventArgs> QueryItemVisibility;
        public event EventHandler<ItemsRealizedEventArgs> ItemsRealized;
        public event CurrentChangingEventHandler CurrentChanging;
        public event EventHandler CurrentChanged;
        private readonly IVirtualCollectionSource<T> _source;
        private readonly int _pageSize;
        private readonly IEqualityComparer<T> _equalityComparer;

        private uint _state; // used to ensure that data-requests are not stale

        private readonly SparseList<VirtualItem<T>> _virtualItems;
        private readonly HashSet<int> _fetchedPages = new HashSet<int>();
        private readonly HashSet<int> _requestedPages = new HashSet<int>();

        private readonly MostRecentUsedList<int> _mostRecentlyRequestedPages;
        private int _itemCount;
        private readonly TaskScheduler _synchronizationContextScheduler;
        private bool _isRefreshDeferred;
        private int _currentItem;

        private int _inProcessPageRequests;
        private Stack<PageRequest> _pendingPageRequests = new Stack<PageRequest>();

        private readonly SortDescriptionCollection _sortDescriptions = new SortDescriptionCollection();

        public VirtualCollection(IVirtualCollectionSource<T> source, int pageSize, int cachedPages)
            : this(source, pageSize, cachedPages, EqualityComparer<T>.Default)
        {

        }

        public VirtualCollection(IVirtualCollectionSource<T> source, int pageSize, int cachedPages,
                                 IEqualityComparer<T> equalityComparer)
        {
            if (pageSize < 1)
            {
                throw new ArgumentException("pageSize must be bigger than 0");
            }
            if (equalityComparer == null)
            {
                throw new ArgumentNullException("equalityComparer");
            }

            _source = source;
            _source.CollectionChanged += HandleSourceCollectionChanged;
            _pageSize = pageSize;
            _equalityComparer = equalityComparer;
            _virtualItems = CreateItemsCache(pageSize);
            _currentItem = -1;
            _synchronizationContextScheduler = TaskScheduler.FromCurrentSynchronizationContext();
            _mostRecentlyRequestedPages = new MostRecentUsedList<int>(cachedPages);
            _mostRecentlyRequestedPages.ItemEvicted += HandlePageEvicted;

            (_sortDescriptions as INotifyCollectionChanged).CollectionChanged += HandleSortDescriptionsChanged;
        }

        public IVirtualCollectionSource<T> Source
        {
            get { return _source; }
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
        public bool IsCurrentAfterLast
        {
            get { return CurrentPosition >= _itemCount; }
        }

        public bool IsCurrentBeforeFirst
        {
            get { return CurrentPosition < 0; }
        }
        public int Count
        {
            get { return _itemCount; }
        }

        object ICollection.SyncRoot
        {
            get { throw new NotImplementedException(); }
        }

        public bool IsSynchronized
        {
            get { return false; }
        }

        public bool IsReadOnly
        {
            get { return true; }
        }

        bool IList.IsFixedSize
        {
            get { return false; }
        }

        protected uint State
        {
            get { return _state; }
        }

        public void RealizeItemRequested(int index)
        {
            var page = index/_pageSize;
            BeginGetPage(page);
        }

        public void Refresh()
        {
            Refresh(RefreshMode.PermitStaleDataWhilstRefreshing);
        }

        public void Refresh(RefreshMode mode)
        {
            if (!_isRefreshDeferred)
            {
                _source.Refresh(mode);
            }
        }

        private void HandlePageEvicted(object sender, ItemEvictedEventArgs<int> e)
        {
            _requestedPages.Remove(e.Item);
            _fetchedPages.Remove(e.Item);
            _virtualItems.RemoveRange(e.Item*_pageSize, _pageSize);
        }

        private SparseList<VirtualItem<T>> CreateItemsCache(int fetchPageSize)
        {
            // we don't want the sparse list to have pages that are too small,
            // because that will harm performance by fragmenting the list across memory,
            // but too big, and we'll be wasting lots of space
            const int TargetSparseListPageSize = 100;

            var pageSize = fetchPageSize;

            if (pageSize < TargetSparseListPageSize)
            {
                // make pageSize the smallest multiple of fetchPageSize that is bigger than TargetSparseListPageSize
                pageSize = (int) Math.Ceiling((double) TargetSparseListPageSize/pageSize)*pageSize;
            }

            return new SparseList<VirtualItem<T>>(pageSize);
        }

        private void HandleSortDescriptionsChanged(object sender, NotifyCollectionChangedEventArgs e)
        {
            Refresh();
        }

        private void HandleSourceCollectionChanged(object sender, VirtualCollectionSourceChangedEventArgs e)
        {
            if (e.ChangeType == ChangeType.Refresh)
            {
                Task.Factory.StartNew(UpdateData, CancellationToken.None,
                                      TaskCreationOptions.None, _synchronizationContextScheduler);
            }
            else if (e.ChangeType == ChangeType.Reset)
            {
                Task.Factory.StartNew(Reset, CancellationToken.None,
                                      TaskCreationOptions.None, _synchronizationContextScheduler);
            }
        }

        private void MarkExistingItemsAsStale()
        {
            foreach (var page in _fetchedPages)
            {
                var startIndex = page*_pageSize;
                var endIndex = (page + 1)*_pageSize;

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

            _mostRecentlyRequestedPages.Add(page);
            _requestedPages.Add(page);

            _pendingPageRequests.Push(new PageRequest(page, State));

            ProcessPageRequests();
        }

        private void ProcessPageRequests()
        {
            while (_inProcessPageRequests < MaxConcurrentPageRequests && _pendingPageRequests.Count > 0)
            {
                var request = _pendingPageRequests.Pop();

                // if we encounter a requested posted for an early collection state,
                // we can ignore it, and all that came before it
                if (State != request.StateWhenRequested)
                {
                    _pendingPageRequests.Clear();
                    break;
                }

                // check that the page is still requested (the user might have scrolled, causing the 
                // page to be ejected from the cache
                if (!_requestedPages.Contains(request.Page))
                {
                    break;
                }

                _inProcessPageRequests++;

                _source.GetPageAsync(request.Page*_pageSize, _pageSize, _sortDescriptions).ContinueWith(
                    t =>
                    {
                        if (!t.IsFaulted)
                        {
                            UpdatePage(request.Page, t.Result, request.StateWhenRequested);
                        }
                        else
                        {
                            MarkPageAsError(request.Page, request.StateWhenRequested);
                        }

                        // fire off any further requests
                        _inProcessPageRequests--;
                        ProcessPageRequests();
                    },
                    _synchronizationContextScheduler);
            }
        }

        private void MarkPageAsError(int page, uint stateWhenRequestInitiated)
        {
            if (stateWhenRequestInitiated != State)
            {
                return;
            }

            bool stillRelevant = _requestedPages.Remove(page);
            if (!stillRelevant)
            {
                return;
            }

            var startIndex = page*_pageSize;

            for (int i = 0; i < _pageSize; i++)
            {
                var index = startIndex + i;
                var virtualItem = _virtualItems[index];
                if (virtualItem != null)
                {
                    virtualItem.ErrorFetchingValue();
                }
            }
        }

        private bool IsPageAlreadyRequested(int page)
        {
            return _fetchedPages.Contains(page) || _requestedPages.Contains(page);
        }

        private void UpdatePage(int page, IList<T> results, uint stateWhenRequested)
        {
            if (stateWhenRequested != State)
            {
                // this request may contain out-of-date data, so ignore it
                return;
            }

            bool stillRelevant = _requestedPages.Remove(page);
            if (!stillRelevant)
            {
                return;
            }

            _fetchedPages.Add(page);

            var startIndex = page*_pageSize;

            for (int i = 0; i < results.Count; i++)
            {
                var index = startIndex + i;
                var virtualItem = _virtualItems[index] ?? (_virtualItems[index] = new VirtualItem<T>(this, index));
                if (virtualItem.Item == null || results[i] == null ||
                    !_equalityComparer.Equals(virtualItem.Item, results[i]))
                {
                    virtualItem.SupplyValue(results[i]);
                }
            }

            if (results.Count > 0)
            {
                OnItemsRealized(new ItemsRealizedEventArgs(startIndex, results.Count));
            }
        }

        protected void UpdateData()
        {
            IncrementState();

            MarkExistingItemsAsStale();

            _fetchedPages.Clear();
            _requestedPages.Clear();

            UpdateCount();

            var queryItemVisibilityArgs = new QueryItemVisibilityEventArgs();
            OnQueryItemVisibility(queryItemVisibilityArgs);

            if (queryItemVisibilityArgs.FirstVisibleIndex.HasValue)
            {
                var firstVisiblePage = queryItemVisibilityArgs.FirstVisibleIndex.Value/_pageSize;
                var lastVisiblePage = queryItemVisibilityArgs.LastVisibleIndex.Value/_pageSize;

                int numberOfVisiblePages = lastVisiblePage - firstVisiblePage + 1;
                EnsurePageCacheSize(numberOfVisiblePages);

                for (int i = firstVisiblePage; i <= lastVisiblePage; i++)
                {
                    BeginGetPage(i);
                }
            }
            else
            {
                // in this case we have no way of knowing which items are currently visible,
                // so we signal a collection reset, and wait to see which pages are requested by the UI
                OnCollectionChanged(new NotifyCollectionChangedEventArgs(NotifyCollectionChangedAction.Reset));
            }
        }

        private void IncrementState()
        {
            _state++;
        }

        private void EnsurePageCacheSize(int numberOfPages)
        {
            if (_mostRecentlyRequestedPages.Size < numberOfPages)
            {
                _mostRecentlyRequestedPages.Size = numberOfPages;
            }
        }

        private void Reset()
        {
            IncrementState();

            foreach (var page in _fetchedPages)
            {
                var startIndex = page*_pageSize;
                var endIndex = (page + 1)*_pageSize;

                for (int i = startIndex; i < endIndex; i++)
                {
                    if (_virtualItems[i] != null)
                    {
                        _virtualItems[i].ClearValue();
                    }
                }
            }
            UpdateCount(0);
        }

        private void UpdateCount()
        {
            UpdateCount(_source.Count);
        }

        private void UpdateCount(int count)
        {
            if (_itemCount == count)
            {
                return;
            }

            var wasCurrentBeyondLast = IsCurrentAfterLast;

            var originalItemCount = _itemCount;
            var delta = count - originalItemCount;
            _itemCount = count;

            if (IsCurrentAfterLast && !wasCurrentBeyondLast)
            {
                UpdateCurrentPosition(_itemCount - 1, allowCancel: false);
            }

            OnPropertyChanged(new PropertyChangedEventArgs("Count"));

            if (Math.Abs(delta) > IndividualItemNotificationLimit || _itemCount == 0)
            {
                OnCollectionChanged(new NotifyCollectionChangedEventArgs(NotifyCollectionChangedAction.Reset));
            }
            else if (delta > 0)
            {
                for (int i = 0; i < delta; i++)
                {
                    OnCollectionChanged(new NotifyCollectionChangedEventArgs(NotifyCollectionChangedAction.Add, null,
                                                                             originalItemCount + i));
                }
            }
            else if (delta < 0)
            {
                for (int i = 1; i <= Math.Abs(delta); i++)
                {
                    OnCollectionChanged(new NotifyCollectionChangedEventArgs(NotifyCollectionChangedAction.Remove,
                                                                             _virtualItems[originalItemCount - i],
                                                                             originalItemCount - i));
                }
            }
        }

        public int IndexOf(VirtualItem<T> item)
        {
            return item.Index;
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

            return new Disposer(() =>
            {
                _isRefreshDeferred = false;
                Refresh();
            });
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
            return MoveCurrentToPosition(((IList) this).IndexOf(item));
        }

        public bool MoveCurrentToPosition(int position)
        {
            return UpdateCurrentPosition(position);
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

        protected void OnCurrentChanging(CurrentChangingEventArgs e)
        {
            CurrentChangingEventHandler handler = CurrentChanging;
            if (handler != null) handler(this, e);
        }


        protected void OnCurrentChanged(EventArgs e)
        {
            EventHandler handler = CurrentChanged;
            if (handler != null) handler(this, e);
        }

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

        protected void OnItemsRealized(ItemsRealizedEventArgs e)
        {
            EventHandler<ItemsRealizedEventArgs> handler = ItemsRealized;
            if (handler != null) handler(this, e);
        }

        protected void OnQueryItemVisibility(QueryItemVisibilityEventArgs e)
        {
            EventHandler<QueryItemVisibilityEventArgs> handler = QueryItemVisibility;
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

        private struct PageRequest
        {
            public readonly int Page;
            public readonly uint StateWhenRequested;

            public PageRequest(int page, uint state)
            {
                Page = page;
                StateWhenRequested = state;
            }
        }
    }
}
