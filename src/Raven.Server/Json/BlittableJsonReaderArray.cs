using System;
using System.Collections.Generic;
using ConsoleApplication4;

namespace NewBlittable
{
    public unsafe class BlittableJsonReaderArray : BlittableJsonReaderBase
    {
        private BlittableJsonReaderBase _parent;
        private int _count;
        private byte* _positions;
        private byte* _types;
        private byte* _dataStart;
        private long _currentOffsetSize;
        private Dictionary<int, object> cache;

        public BlittableJsonReaderArray(int pos, BlittableJsonReaderBase parent, BlittableJsonToken type)
        {
            _parent = parent;
            byte arraySizeOffset;
            _count = parent.ReadVariableSizeInt(pos, out arraySizeOffset);

            _dataStart = parent._mem + pos;
            _positions = parent._mem + pos + arraySizeOffset;

            // analyze main object type and it's offset and propertyIds flags
            _currentOffsetSize = ProcessTokenOffsetFlags(type);

            _types = parent._mem + pos + arraySizeOffset + _count * _currentOffsetSize;
        }

        public int Length => _count;

        public int Count => _count;

        public object this[int index]
        {
            get
            {
                object result;
                TryGetIndex(index, out result);
                return result;
            }
        }

        public bool TryGetIndex(int index, out object result)
        {
            result = null;

            // try get value from cache, works only with Blittable types, other objects are not stored for now
            if (cache != null && cache.TryGetValue(index, out result))
                return true;

            if (index >= _count || index < 0)
                throw new IndexOutOfRangeException($"Cannot access index {index} when our size is {_count}");


            var offset = ReadNumber(_positions + index * _currentOffsetSize, _currentOffsetSize);
            result = _parent.GetObject((BlittableJsonToken)_types[index],
                (int)(_dataStart - _parent._mem - offset));

            if (result is BlittableJsonReaderBase)
            {
                if (cache == null)
                {
                    cache = new Dictionary<int, object>();
                }
                cache[index] = result;
            }
            return true;
        }

    }
}