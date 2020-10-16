using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;

namespace Sparrow.Collections
{
    public class FastList<T> : IList<T>
    {
        private uint _version = 0;
        private uint _size = 0;

        private static readonly T[] EmptyArray = new T[0];

        private T[] _items;

        // Constructs a List. The list is initially empty and has a capacity
        // of zero. Upon adding the first element to the list the capacity is
        // increased to _defaultCapacity, and then increased in multiples of two
        // as required.
        public FastList()
        {
            _items = EmptyArray;
        }

        public FastList(int capacity)
        {
            if (capacity < 0)
                throw new ArgumentOutOfRangeException(nameof(capacity));

            _items = capacity == 0 ? EmptyArray : new T[capacity];          
        }


        public T this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return _items[index];
            }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _items[index] = value;
                _version++;
            }
        }

        public T this[uint index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return _items[index];
            }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _items[index] = value;
                _version++;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref T GetAsRef(uint index)
        {
            return ref _items[index];
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref T GetAsRef(int index)
        {
            return ref _items[index];
        }

        // Gets and sets the capacity of this list.  The capacity is the size of
        // the internal array used to hold items.  When set, the internal 
        // array of the list is reallocated to the given capacity.
        // 
        public int Capacity
        {
            get { return _items.Length; }
            set
            {
                if (value < _size)
                    throw new ArgumentOutOfRangeException(nameof(value));

                if (value != _items.Length)
                {
                    if (value > 0)
                    {
                        T[] newItems = new T[value];
                        if (_size > 0)
                        {
                            Array.Copy(_items, 0, newItems, 0, (int)_size);
                        }
                        _items = newItems;
                    }
                    else
                    {
                        _items = EmptyArray;
                    }
                }
            }
        }

        public int Count => (int)_size;
        public bool IsReadOnly => false;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(T item)
        {
            if (_size == _items.Length)
                goto Unlikely;

            _items[_size++] = item;
            _version++;
            return;

            Unlikely:
            AddUnlikely(item, (int)_size + 1);
        }

        public void CopyTo(FastList<int> dest)
        {
            int size = (int)_size;
            if (dest.Capacity < size)
                dest.Capacity = size;

            dest._size = (uint)size;
            Array.Copy( _items, dest._items, size);
            dest._version++;
        }

        private void AddUnlikely(T item, int size)
        {
            EnsureCapacity(size);
            _items[_size++] = item;
            _version++;
        }

        private const int DefaultCapacity = 16;
        private const int MaxArrayLength = 0X7FEFFFFF;

        // Ensures that the capacity of this list is at least the given minimum
        // value. If the current capacity of the list is less than min, the
        // capacity is increased to twice the current capacity or to min,
        // whichever is larger.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureCapacity(int min)
        {
            if (_items.Length < min)
            {
                int newCapacity = _items.Length == 0 ? DefaultCapacity : _items.Length * 2;
                // Allow the list to grow to maximum possible capacity (~2G elements) before encountering overflow.
                // Note that this check works even when _items.Length overflowed thanks to the (uint) cast
                if ((uint)newCapacity > MaxArrayLength)
                    newCapacity = MaxArrayLength;

                if (newCapacity < min)
                    newCapacity = min;

                Capacity = newCapacity;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear()
        {
            // PERF: We are using this to avoid the Array.Clear cost when using structs. 
            //       When RuntimeHelpers.IsReferenceOrContainsReferences<T>() becomes available, we can use Clear instead. 
            if (typeof(T) == typeof(int) || typeof(T) == typeof(uint) || typeof(T) == typeof(byte) ||
                typeof(T) == typeof(short) || typeof(T) == typeof(long) || typeof(T) == typeof(ulong))
            {
                _size = 0;
                _version++;
            }
            else
            {
                int size = (int)_size;

                _size = 0;
                _version++;
                if (size > 0)
                    Array.Clear(_items, 0, size); // Clear the elements so that the gc can reclaim the references.
            }
        }

        public void Trim(int size)
        {
            var oldSize = (int)_size;

            if (size < 0 || size > oldSize)
                throw new ArgumentOutOfRangeException(nameof(size));

            if (size == oldSize)
                return; // no-op

            _size = (uint)size;
            _version++;

            Array.Clear(_items, size, oldSize - size);
        }

        /// <summary>
        /// This method is like Clear but will not release the references contained in it; therefore
        /// the garbage collector will not collect those objects even if they are not being used.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WeakClear()
        {
            _size = 0;
            _version++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Contains(T item)
        {
            // PERF: IndexOf calls Array.IndexOf, which internally
            // calls EqualityComparer<T>.Default.IndexOf, which
            // is specialized for different types. This
            // boosts performance since instead of making a
            // virtual method call each iteration of the loop,
            // via EqualityComparer<T>.Default.Equals, we
            // only make one virtual call to EqualityComparer.IndexOf.

            return _size != 0 && IndexOf(item) != -1;
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            // Delegate rest of error checking to Array.Copy.
            Array.Copy(_items, 0, array, arrayIndex, (int)_size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int IndexOf(T item)
        {
            Contract.Ensures(Contract.Result<int>() >= -1);
            Contract.Ensures(Contract.Result<int>() < Count);
            return Array.IndexOf(_items, item, 0, (int)_size);
        }

        public void Insert(int index, T item)
        {
            throw new NotSupportedException();
        }
    
        public bool Remove(T item)
        {
            int index = IndexOf(item);
            if (index >= 0)
            {
                RemoveAt(index);
                return true;
            }

            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T RemoveLast()
        {
            if (_size == 0)
                return default;

            _size--;
            _version++;

            T val = _items[_size];
            _items[_size] = default;
            return val;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RemoveAt(int index)
        {
            if ((uint) index >= _size)
                goto ERROR;

            _size--;
            _version++;

            if (index < _size)
                Array.Copy(_items, index + 1, _items, index, (int)_size - index);
            return;

            ERROR:
            ThrowWhenIndexIsOutOfRange(index);
        }

        public void ThrowWhenIndexIsOutOfRange(int index)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        // Removes a range of elements from this list.
        // 
        public void RemoveRange(int index, int count)
        {
            uint uindex = (uint)index;
            if (uindex >= _size || (int)_size - index < count)
                goto ERROR;

            if (count <= 0)
                return;

            _size -= (uint) count;
            if (index < _size)
            {
                Array.Copy(_items, index + count, _items, index, (int)_size - index);
            }

            _version++;

            if (typeof(T) != typeof(int) && typeof(T) != typeof(uint) && typeof(T) != typeof(byte) &&
                typeof(T) != typeof(short) && typeof(T) != typeof(long) && typeof(T) != typeof(ulong))
            {
                Array.Clear(_items, (int) _size, count);
            }

            return;            

            ERROR:
            ThrowWhenIndexIsOutOfRange(index);
        }


        public Enumerator GetEnumerator()
        {
            return new Enumerator(this);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new Enumerator(this);
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            return new Enumerator(this);
        }        

        public struct Enumerator : IEnumerator<T>
        {
            private readonly FastList<T> _list;
            private readonly uint _version;
            private uint _index;            
            private T _current;

            internal Enumerator(FastList<T> list)
            {
                _list = list;
                _index = 0;
                _version = list._version;
                _current = default(T);
            }

            public void Dispose()
            {
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveNext()
            {
                FastList<T> localList = _list;
                if (_version == localList._version && (_index < localList._size))
                {
                    _current = localList._items[_index];
                    _index++;
                    return true;
                }
                return MoveNextUnlikely();
            }

            private bool MoveNextUnlikely()
            {
                if (_version != _list._version)
                    throw new InvalidOperationException("Version mismatch");

                _index = _list._size + 1;
                _current = default(T);
                return false;
            }

            public T Current => _current;

            Object IEnumerator.Current
            {
                get
                {
                    if (_index == 0 || _index == _list._size + 1)
                        throw new InvalidOperationException("Cant happen");

                    return Current;
                }
            }

            void IEnumerator.Reset()
            {
                if (_version != _list._version)
                    throw new InvalidOperationException("Version mismatch");

                _index = 0;
                _current = default(T);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Sort(IComparer<T> comparer)
        {
            Array.Sort<T>(_items, 0, (int)_size, comparer);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Sort<TSorter>(ref Sorter<T, TSorter> sorter) where TSorter : struct, IComparer<T>
        {
            sorter.Sort(_items, 0, (int)_size);
        }


        public struct ResetBehavior : IResetSupport<FastList<T>>
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Reset(FastList<T> builder)
            {
                builder.Clear();
            }
        }

        public struct WeakResetBehavior : IResetSupport<FastList<T>>
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Reset(FastList<T> builder)
            {
                builder.WeakClear();
            }
        }

        public T[] ToArray()
        {
            var copy = new T[_size];
            CopyTo(copy, 0);
            return copy;
        }
    }
}

