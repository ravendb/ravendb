using System;
using System.Collections;
using System.Collections.Generic;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Studio.Features.Documents;

namespace Raven.Studio.Infrastructure
{
    public class MostRecentUsedList<T> : IEnumerable<T>
    {
        public event EventHandler<ItemEvictedEventArgs<T>> ItemEvicted;

        private int _size;
        private LinkedList<T> _list = new LinkedList<T>();
 
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
            if (_list.Contains(item))
            {
                _list.Remove(item);
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
        }

        protected void OnItemEvicted(ItemEvictedEventArgs<T> e)
        {
            EventHandler<ItemEvictedEventArgs<T>> handler = ItemEvicted;
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
