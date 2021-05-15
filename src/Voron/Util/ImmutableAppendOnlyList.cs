using Sparrow.Binary;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Voron.Util
{
    /// <summary>
    /// A list that can be used by readers as a true immutable read-only list 
    /// and that supports relatively efficient "append to the end" and "remove 
    /// from the front" operations, by sharing the underlying array whenever 
    /// possible. Implemented as a class so it can be used to "CAS" when making 
    /// changes in order to allow lockless immutability. 
    /// Caution: multithreaded append operations are not safe, also not when
    /// using CAS.
    /// </summary>
    public sealed class ImmutableAppendOnlyList<T> : IReadOnlyList<T>
    {
        private delegate void RangeCopier(IEnumerable<T> source, T[] dest, int destOffset, int count);
        private readonly T[] _values;
        private readonly int _head;
        private readonly int _count;

        private ImmutableAppendOnlyList(T[] values, int head, int count)
        {
            _values = values;
            _head = head;
            _count = count;
        }

        /// <summary>
        /// A "new" empty instance of a list.
        /// </summary>
        public static readonly ImmutableAppendOnlyList<T> Empty = new ImmutableAppendOnlyList<T>(new T[0], 0, 0);

        /// <summary>
        /// Creates a new list with the given items as the initial content.
        /// </summary>
        public static ImmutableAppendOnlyList<T> CreateFrom(IEnumerable<T> items)
        {
            if (items == null)
                return Empty;
            var values = items.ToArray();
            return values.Length == 0 ? Empty : new ImmutableAppendOnlyList<T>(values, 0, values.Length);
        }

        /// <summary>
        /// Creates a new list with the given initial capacity.
        /// </summary>
        public static ImmutableAppendOnlyList<T> Create(int initialCapacity)
        {
            return initialCapacity > 0 ? new ImmutableAppendOnlyList<T>(new T[initialCapacity], 0, 0) : Empty;
        }

        /// <summary>
        /// Returns the item at the given index.
        /// </summary>
        public T this[int itemIndex]
        {
            get
            {
                if ((uint)itemIndex >= (uint)_count)
                    throw new IndexOutOfRangeException("itemIndex");
                return _values[_head + itemIndex];
            }
        }

        /// <summary>
        /// Returns the number of items in the list.
        /// </summary>
        public int Count
        {
            get { return _count; }
        }

        /// <summary>
        /// Returns whether the collection is empty, i.e. contains no items
        /// </summary>
        public bool IsEmpty
        {
            get { return _count == 0; }
        }

        /// <summary>
        /// Returns an enumerator over the items in the collection.
        /// </summary>
        public IEnumerator<T> GetEnumerator()
        {
            var end = (_head + _count);
            for (var i = _head; i < end; i++)
                yield return _values[i];
        }

        /// <summary>
        /// Returns an enumerator over the items in the collection.
        /// </summary>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return ((IReadOnlyList<T>)this).GetEnumerator();
        }

        /// <summary>
        /// Returns a new list with the given item appended to the end.
        /// </summary>
        public ImmutableAppendOnlyList<T> Append(T item)
        {
            var newCount = _count + 1;
            var tail = _head + _count;
            if (tail == _values.Length)
            {
                var newArray = GrowTo(Math.Max(8, Bits.PowerOf2(newCount)));
                newArray[_count] = item;
                return new ImmutableAppendOnlyList<T>(newArray, 0, newCount);
            }
            _values[tail] = item;
            return new ImmutableAppendOnlyList<T>(_values, _head, newCount);
        }

        /// <summary>
        /// Returns a new list with the given set of items appended to the end.
        /// </summary>
        public ImmutableAppendOnlyList<T> AppendRange(IEnumerable<T> items)
        {
            var nToAdd = 0;
            RangeCopier copier = CopyEnumerable;
            if (items != null)
            {
                var collectionType = items.GetType();
                if (collectionType.IsArray)
                {
                    nToAdd = ((T[])items).Length;
                    copier = CopyArray;
                }
                else if (items is ICollection iCollection)
                {
                    nToAdd = iCollection.Count;
                    if (items is List<T>)
                        copier = CopyList;
                }
                else // cannot optimize, have to do this manually
                {
                    return items.Aggregate(this, (current, item) => current.Append(item));
                }
            }
            if (nToAdd == 0)
                return this;

            var newCount = _count + nToAdd;
            var tail = _head + _count;
            if (tail + nToAdd > _values.Length)
            {
                var newArray = GrowTo(_count + nToAdd * 2);
                copier(items, newArray, _count, nToAdd);
                return new ImmutableAppendOnlyList<T>(newArray, 0, newCount);
            }
            copier(items, _values, tail, nToAdd);
            return new ImmutableAppendOnlyList<T>(_values, _head, newCount);
        }

        /// <summary>
        /// Returns a new list from which the first element is removed.
        /// </summary>
        public ImmutableAppendOnlyList<T> RemoveFront()
        {
            return _count == 1
                ? Empty
                : new ImmutableAppendOnlyList<T>(_values, _head + 1, _count - 1);
        }

        /// <summary>
        /// Returns a new list from which the first element is removed, where
        /// this element is provided as output in <paramref name="removed"/>.
        /// </summary>
        public ImmutableAppendOnlyList<T> RemoveFront(out T removed)
        {
            removed = _values[_head];
            return _count == 1
                ? Empty
                : new ImmutableAppendOnlyList<T>(_values, _head + 1, _count - 1);
        }

        /// <summary>
        /// Returns a new list from which the given number of first elements
        /// are removed.
        /// </summary>
        public ImmutableAppendOnlyList<T> RemoveFront(int nToRemove)
        {
            return nToRemove >= _count
                ? Empty
                : new ImmutableAppendOnlyList<T>(_values, _head + nToRemove, _count - nToRemove);
        }

        /// <summary>
        /// Returns a new list from which the given number of first elements
        /// are removed and stored as output in the given list.
        /// </summary>
        public ImmutableAppendOnlyList<T> RemoveFront(int nToRemove, List<T> removed)
        {
            var remove = nToRemove > _count ? _count : nToRemove;
            if (removed != null)
                removed.AddRange(_values.Skip(_head).Take(remove));
            return remove == _count
                ? Empty
                : new ImmutableAppendOnlyList<T>(_values, _head + nToRemove, _count - nToRemove);
        }

        /// <summary>
        /// Returns a new list from which all first elements that meet the 
        /// given predicate are removed and stored as output in the given list.
        /// </summary>
        public ImmutableAppendOnlyList<T> RemoveWhile(Predicate<T> shouldRemove, List<T> removed = null)
        {
            var newHead = _head;
            var tail = _head + _count;
            while (newHead < tail && shouldRemove(_values[newHead]))
                newHead++;
            return newHead == _head
                ? this
                : RemoveFront(newHead - _head, removed);
        }

        private T[] GrowTo(int newLength)
        {
            var newArray = new T[newLength];
            Array.Copy(_values, _head, newArray, 0, _count);
            return newArray;
        }

        private static void CopyArray(IEnumerable<T> source, T[] dest, int destOffset, int count)
        {
            Array.Copy((T[])source, 0, dest, destOffset, count);
        }

        private static void CopyList(IEnumerable<T> source, T[] dest, int destOffset, int count)
        {
            ((List<T>)source).CopyTo(0, dest, destOffset, count);
        }

        private static void CopyEnumerable(IEnumerable<T> source, T[] dest, int destOffset, int count)
        {
            // We want to guard against the source enumerable changing from 
            // under us. We do not add more than the given count, and throw 
            // if there is less.
            using (var enumerator = source.GetEnumerator())
            {
                for (int i = 0; i < count; i++)
                {
                    enumerator.MoveNext(); // should throw if we attempt to advance past the end.
                    dest[destOffset++] = enumerator.Current;
                }
            }
        }
    }
}
