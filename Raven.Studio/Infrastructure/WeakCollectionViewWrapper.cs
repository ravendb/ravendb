using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Globalization;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

namespace Raven.Studio.Infrastructure
{
    /// <summary>
    /// This class exists because of a bug/oversight in Silverlight, where ListBox does not unsubscribe to
    /// CurrentChanged events of its ItemsSource. The solution used here is to give the ListBox a wrapper around the actual VirtualCollection
    /// that subscribes using weak events.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class WeakCollectionViewWrapper<T> : IList, ICollectionView, IEnquireAboutItemVisibility where T : IList, ICollectionView
    {
        private readonly T innerCollection;

        public WeakCollectionViewWrapper(T innerCollection)
        {
            this.innerCollection = innerCollection;

            var currentChangedListener = new WeakEventListener<WeakCollectionViewWrapper<T>, object, EventArgs>(this)
            {
                OnEventAction =
                    (instance, source, eventArgs) =>
                    instance.OnCurrentChanged(eventArgs),
                OnDetachAction =
                    listener => innerCollection.CurrentChanged -= listener.OnEvent
            };

            innerCollection.CurrentChanged += currentChangedListener.OnEvent;

            var currentChangingListener = new WeakEventListener<WeakCollectionViewWrapper<T>, object, CurrentChangingEventArgs>(this)
            {
                OnEventAction =
                    (instance, source, eventArgs) =>
                    instance.OnCurrentChanging(eventArgs),
                OnDetachAction =
                    listener => innerCollection.CurrentChanging -= listener.OnEvent
            };

            innerCollection.CurrentChanging += currentChangingListener.OnEvent;

            var propertyChangedListener = new WeakEventListener<WeakCollectionViewWrapper<T>, object, NotifyCollectionChangedEventArgs>(this)
            {
                OnEventAction =
                    (instance, source, eventArgs) =>
                    instance.OnCollectionChanged(eventArgs),
                OnDetachAction =
                    listener => innerCollection.CollectionChanged -= listener.OnEvent
            };

            innerCollection.CollectionChanged += propertyChangedListener.OnEvent;

            var enquireAboutItemVisibility = innerCollection as IEnquireAboutItemVisibility;
            if (enquireAboutItemVisibility != null)
            {
                var queryItemVisibilityListener =
                    new WeakEventListener<WeakCollectionViewWrapper<T>, object, QueryItemVisibilityEventArgs>(this)
                    {
                        OnEventAction =
                            (instance, source, eventArgs) =>
                            instance.OnQueryItemVisibility(eventArgs),
                        OnDetachAction =
                            listener => enquireAboutItemVisibility.QueryItemVisibility -= listener.OnEvent
                    };

                enquireAboutItemVisibility.QueryItemVisibility += queryItemVisibilityListener.OnEvent;
            }
        }

        public IEnumerator GetEnumerator()
        {
            return ((IEnumerable)innerCollection).GetEnumerator();
        }

        public void CopyTo(Array array, int index)
        {
            ((ICollection)innerCollection).CopyTo(array, index);
        }

        public object SyncRoot
        {
            get { return ((ICollection)innerCollection).SyncRoot; }
        }

        public bool IsSynchronized
        {
            get { return innerCollection.IsSynchronized; }
        }

        public int Add(object value)
        {
            return ((IList)innerCollection).Add(value);
        }

        bool ICollectionView.Contains(object value)
        {
            return ((IList)innerCollection).Contains(value);
        }

        public int IndexOf(object value)
        {
            return ((IList)innerCollection).IndexOf(value);
        }

        public void Insert(int index, object value)
        {
            ((IList)innerCollection).Insert(index, value);
        }

        public void Remove(object value)
        {
            ((IList)innerCollection).Remove(value);
        }

        object IList.this[int index]
        {
            get { return innerCollection[index]; }
            set { ((IList)innerCollection)[index] = value; }
        }

        public bool IsFixedSize
        {
            get { return ((IList)innerCollection).IsFixedSize; }
        }

        public event NotifyCollectionChangedEventHandler CollectionChanged;

        private void OnCollectionChanged(NotifyCollectionChangedEventArgs e)
        {
            NotifyCollectionChangedEventHandler handler = CollectionChanged;
            if (handler != null) handler(this, e);
        }

        public bool Contains(object item)
        {
            return ((ICollectionView)innerCollection).Contains(item);
        }

        public void Refresh()
        {
            innerCollection.Refresh();
        }

        public IDisposable DeferRefresh()
        {
            return innerCollection.DeferRefresh();
        }

        public bool MoveCurrentToFirst()
        {
            return innerCollection.MoveCurrentToFirst();
        }

        public bool MoveCurrentToLast()
        {
            return innerCollection.MoveCurrentToLast();
        }

        public bool MoveCurrentToNext()
        {
            return innerCollection.MoveCurrentToNext();
        }

        public bool MoveCurrentToPrevious()
        {
            return innerCollection.MoveCurrentToPrevious();
        }

        public bool MoveCurrentTo(object item)
        {
            return innerCollection.MoveCurrentTo(item);
        }

        public bool MoveCurrentToPosition(int position)
        {
            return innerCollection.MoveCurrentToPosition(position);
        }

        public CultureInfo Culture
        {
            get { return innerCollection.Culture; }
            set { innerCollection.Culture = value; }
        }
        public IEnumerable SourceCollection
        {
            get { return innerCollection.SourceCollection; }
        }
        public Predicate<object> Filter
        {
            get { return innerCollection.Filter; }
            set { innerCollection.Filter = value; }
        }
        public bool CanFilter
        {
            get { return innerCollection.CanFilter; }
        }
        public SortDescriptionCollection SortDescriptions
        {
            get { return innerCollection.SortDescriptions; }
        }
        public bool CanSort
        {
            get { return innerCollection.CanSort; }
        }
        public bool CanGroup
        {
            get { return innerCollection.CanGroup; }
        }
        public ObservableCollection<GroupDescription> GroupDescriptions
        {
            get { return innerCollection.GroupDescriptions; }
        }
        public ReadOnlyObservableCollection<object> Groups
        {
            get { return innerCollection.Groups; }
        }
        public bool IsEmpty
        {
            get { return innerCollection.IsEmpty; }
        }
        public object CurrentItem
        {
            get { return innerCollection.CurrentItem; }
        }
        public int CurrentPosition
        {
            get { return innerCollection.CurrentPosition; }
        }
        public bool IsCurrentAfterLast
        {
            get { return innerCollection.IsCurrentAfterLast; }
        }
        public bool IsCurrentBeforeFirst
        {
            get { return innerCollection.IsCurrentBeforeFirst; }
        }

        public event CurrentChangingEventHandler CurrentChanging;

        private void OnCurrentChanging(CurrentChangingEventArgs e)
        {
            CurrentChangingEventHandler handler = CurrentChanging;
            if (handler != null) handler(this, e);
        }

        public event EventHandler CurrentChanged;

        private void OnCurrentChanged(EventArgs e)
        {
            EventHandler handler = CurrentChanged;
            if (handler != null) handler(this, e);
        }

        public int Count
        {
            get { return innerCollection.Count; }
        }

        public bool IsReadOnly
        {
            get { return innerCollection.IsReadOnly; }
        }

        public void Clear()
        {
            innerCollection.Clear();
        }

        public void RemoveAt(int index)
        {
            innerCollection.RemoveAt(index);
        }

        public event EventHandler<QueryItemVisibilityEventArgs> QueryItemVisibility;

        protected void OnQueryItemVisibility(QueryItemVisibilityEventArgs e)
        {
            EventHandler<QueryItemVisibilityEventArgs> handler = QueryItemVisibility;
            if (handler != null) handler(this, e);
        }
    }
}
