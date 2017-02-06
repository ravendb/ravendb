using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Sparrow.Json.Parsing;

namespace Sparrow.Json
{
    public unsafe class BlittableJsonReaderArray : BlittableJsonReaderBase,IEnumerable<object>
    {
        private readonly int _count;
        private readonly byte* _metadataPtr;

        private readonly byte* _dataStart;
        private readonly long _currentOffsetSize;
        private Dictionary<int, Tuple<object,BlittableJsonToken>> _cache;

        public DynamicJsonArray Modifications;

        public BlittableJsonReaderObject Parent => _parent;

        public BlittableJsonReaderArray(int pos, BlittableJsonReaderObject parent, BlittableJsonToken type)
        {
            _parent = parent;
            byte arraySizeOffset;
            _count = parent.ReadVariableSizeInt(pos, out arraySizeOffset);

            _dataStart = parent.BasePointer + pos;
            _metadataPtr = parent.BasePointer + pos + arraySizeOffset;

            // analyze main object type and it's offset and propertyIds flags
            _currentOffsetSize = ProcessTokenOffsetFlags(type);
        }

        public byte* DataStart => _dataStart;
        public int Length => _count;

        public BlittableJsonToken GetArrayType()
        {
            return (BlittableJsonToken)(*(_metadataPtr + _currentOffsetSize)) & BlittableJsonReaderBase.TypesMask;
        }

        public object this[int index] => GetValueTokenTupleByIndex(index).Item1;

        public T GetByIndex<T>(int index)
        {
            var obj = GetValueTokenTupleByIndex(index).Item1;
            T result;
            BlittableJsonReaderObject.ConvertType(obj, out result);
            return result;
        }

        public string GetStringByIndex(int index)
        {
            var obj = GetValueTokenTupleByIndex(index).Item1;
            if (obj == null)
                return null;

            var lazyStringValue = obj as LazyStringValue;
            if (lazyStringValue != (LazyStringValue)null)
                return (string)lazyStringValue;
            var lazyCompressedStringValue = obj as LazyCompressedStringValue;
            if (lazyCompressedStringValue != null)
                return lazyCompressedStringValue;
            string result;
            BlittableJsonReaderObject.ConvertType(obj, out result);
            return result;

        }

        public Tuple<object, BlittableJsonToken> GetValueTokenTupleByIndex(int index)
        {

            // try get value from cache, works only with Blittable types, other objects are not stored for now
            Tuple<object, BlittableJsonToken> result;
            if (_cache != null && _cache.TryGetValue(index, out result))
                return result;

            if (index >= _count || index < 0)
                throw new IndexOutOfRangeException($"Cannot access index {index} when our size is {_count}");

            var itemMetadataStartPtr = _metadataPtr + index * (_currentOffsetSize+1);
            var offset = ReadNumber(itemMetadataStartPtr, _currentOffsetSize);
            var token = *(itemMetadataStartPtr + _currentOffsetSize);
            result = Tuple.Create(_parent.GetObject((BlittableJsonToken)token,
                (int) (_dataStart - _parent.BasePointer - offset)), (BlittableJsonToken)token & TypesMask);

            if (result.Item1 is BlittableJsonReaderBase)
            {
                if (_cache == null)
                {
                    _cache = new Dictionary<int, Tuple<object,BlittableJsonToken>>();
                }
                _cache[index] = result;
            }
            return result;
        }

        public IEnumerable<object> Items
        {
            get
            {
                for (int i = 0; i < _count; i++)
                    yield return this[i];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<object> GetEnumerator()
        {
            return Items.GetEnumerator();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;

            if (ReferenceEquals(this, obj))
                return true;

            var array = obj as BlittableJsonReaderArray;

            if (array != null)
                return Equals(array);

            return false;
        }

        protected bool Equals(BlittableJsonReaderArray other)
        {
            if (_count != other._count)
                return false;

            for (int i = 0; i < _count; i++)
            {
                var x = this[i];
                var y = other[i];

                if (x == null && y == null)
                    continue;

                if ((x?.Equals(y) ?? false) == false)
                    return false;
            }
            
            return true;
        }

        public override int GetHashCode()
        {
            return _count;
        }
    }
}