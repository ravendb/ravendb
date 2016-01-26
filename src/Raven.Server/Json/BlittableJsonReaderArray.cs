using System;
using System.Collections.Generic;
using Raven.Server.Json.Parsing;

namespace Raven.Server.Json
{
    public unsafe class BlittableJsonReaderArray : BlittableJsonReaderBase
    {
        private int _count;
        private byte* _metadataPtr;
        //private byte* _types;
        private byte* _dataStart;
        private long _currentOffsetSize;
        private Dictionary<int, Tuple<object,BlittableJsonToken>> cache;

        public DynamicJsonArray Modifications;

        public BlittableJsonReaderArray(int pos, BlittableJsonReaderObject parent, BlittableJsonToken type)
        {
            _parent = parent;
            byte arraySizeOffset;
            _count = parent.ReadVariableSizeInt(pos, out arraySizeOffset);

            _dataStart = parent._mem + pos;
            _metadataPtr = parent._mem + pos + arraySizeOffset;

            // analyze main object type and it's offset and propertyIds flags
            _currentOffsetSize = ProcessTokenOffsetFlags(type);

          //  _types = parent._mem + pos + arraySizeOffset + _count * _currentOffsetSize;
        }

        public int Length => _count;

        public int Count => _count;

        public object this[int index] => GetByIndex(index);

        public object GetByIndex(int index)
        {
            return GetValueTokenTupleByIndex(index).Item1;
        }

        public Tuple<object, BlittableJsonToken> GetValueTokenTupleByIndex(int index)
        {

            // try get value from cache, works only with Blittable types, other objects are not stored for now
            Tuple<object, BlittableJsonToken> result;
            if (cache != null && cache.TryGetValue(index, out result))
                return result;

            if (index >= _count || index < 0)
                throw new IndexOutOfRangeException($"Cannot access index {index} when our size is {_count}");


            var itemMetadataStartPtr = _metadataPtr + index * (_currentOffsetSize+1);
            var offset = ReadNumber(itemMetadataStartPtr, _currentOffsetSize);
            var token = *(itemMetadataStartPtr + _currentOffsetSize);
            result = Tuple.Create(_parent.GetObject((BlittableJsonToken)token,
                (int) (_dataStart - _parent._mem - offset)), (BlittableJsonToken)token & typesMask);

            if (result.Item1 is BlittableJsonReaderBase)
            {
                if (cache == null)
                {
                    cache = new Dictionary<int, Tuple<object,BlittableJsonToken>>();
                }
                cache[index] = result;
            }
            return result;
        }

    }
}