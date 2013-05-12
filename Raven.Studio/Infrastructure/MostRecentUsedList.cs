using System;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Studio.Infrastructure
{
    public class MostRecentUsedList<T> : IEnumerable<T>
    {
        public event EventHandler<ItemEvictedEventArgs<T>> ItemEvicted;

        private int _size;
        private readonly LinkedList<T> _list = new LinkedList<T>();
        private readonly HashSet<T> _fastLookup = new HashSet<T>();
 
        public MostRecentUsedList(int size)
        {
            _size = size;
        }

        public int Size
        {
            get { return _size; }
            set
            {
                _size = value;
                Trim();
            }
        }

        public void Add(T item)
        {
            if (_fastLookup.Contains(item))
            {
                _list.Remove(item);
            }
            else
            {
                _fastLookup.Add(item);
            }
            
            _list.AddFirst(item);

            Trim();
        }

        private void Trim()
        {
            while (_list.Count > Size)
            {
                var item = _list.Last;
                _list.RemoveLast();
                _fastLookup.Remove(item.Value);

                OnItemEvicted(new ItemEvictedEventArgs<T>(item.Value));
            }
        }

        public IEnumerator<T> GetEnumerator()
        {
            return _list.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Clear()
        {
            _list.Clear();
            _fastLookup.Clear();
        }

        protected void OnItemEvicted(ItemEvictedEventArgs<T> e)
        {
            var handler = ItemEvicted;
            if (handler != null) handler(this, e);
        }

        public void AddRange(IEnumerable<T> items)
        {
            foreach (var item in items)
            {
                Add(item);
            }
        }
    }

    public class ItemEvictedEventArgs<T> : EventArgs
    {
        public T Item { get; private set; }

        public ItemEvictedEventArgs(T item)
        {
            Item = item;
        }
    }
}