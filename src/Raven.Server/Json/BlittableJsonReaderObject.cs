using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using Raven.Server.Json.Parsing;

namespace Raven.Server.Json
{
    public unsafe class BlittableJsonReaderObject : BlittableJsonReaderBase
    {
        private readonly unsafe byte* _metadataPtr;
        private readonly int _propCount;
        private readonly long _currentOffsetSize;
        private readonly long _currentPropertyIdSize;
        private readonly unsafe byte* _objStart;
        private LazyStringValue[] _propertyNames;

        public DynamicJsonValue Modifications;

        private Dictionary<string, Tuple<object, BlittableJsonToken>> _objectsPathCache;


        public unsafe BlittableJsonReaderObject(byte* mem, int size, RavenOperationContext context)
        {
            _mem = mem; // get beginning of memory pointer
            _size = size; // get document size
            _context = context;

            // init document level properties
            var propStartPos = size - sizeof(int) - sizeof(byte); //get start position of properties
            _propNames = (mem + (*(int*)(mem + propStartPos)));
            var propNamesOffsetFlag = (BlittableJsonToken)(*_propNames);
            switch (propNamesOffsetFlag)
            {
                case BlittableJsonToken.OffsetSizeByte:
                    _propNamesDataOffsetSize = sizeof(byte);
                    break;
                case BlittableJsonToken.OffsetSizeShort:
                    _propNamesDataOffsetSize = sizeof(short);
                    break;
                case BlittableJsonToken.OffsetSizeInt:
                    _propNamesDataOffsetSize = sizeof(int);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(
                        $"Property names offset flag should be either byte, short of int, instead of {propNamesOffsetFlag}");
            }
            // get pointer to property names array on document level

            // init root level object properties
            var objStartOffset = *(int*)(mem + (size - sizeof(int) - sizeof(int) - sizeof(byte)));
            // get offset of beginning of data of the main object
            byte propCountOffset = 0;
            _propCount = ReadVariableSizeInt(objStartOffset, out propCountOffset); // get main object properties count
            _objStart = objStartOffset + mem;
            _metadataPtr = objStartOffset + mem + propCountOffset;
            // get pointer to current objects property tags metadata collection

            var currentType = (BlittableJsonToken)(*(mem + size - sizeof(byte)));
            // get current type byte flags

            // analyze main object type and it's offset and propertyIds flags
            _currentOffsetSize = ProcessTokenOffsetFlags(currentType);
            _currentPropertyIdSize = ProcessTokenPropertyFlags(currentType);
        }

        public unsafe BlittableJsonReaderObject(int pos, BlittableJsonReaderObject parent, BlittableJsonToken type)
        {
            _parent = parent;
            _context = parent._context;
            _mem = parent._mem;
            _size = parent._size;
            _propNames = parent._propNames;

            var propNamesOffsetFlag = (BlittableJsonToken)(*(byte*)_propNames);
            switch (propNamesOffsetFlag)
            {
                case BlittableJsonToken.OffsetSizeByte:
                    _propNamesDataOffsetSize = sizeof(byte);
                    break;
                case BlittableJsonToken.OffsetSizeShort:
                    _propNamesDataOffsetSize = sizeof(short);
                    break;
                case BlittableJsonToken.PropertyIdSizeInt:
                    _propNamesDataOffsetSize = sizeof(int);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(
                        $"Property names offset flag should be either byte, short of int, instead of {propNamesOffsetFlag}");
            }

            _objStart = _mem + pos;
            byte propCountOffset;
            _propCount = ReadVariableSizeInt(pos, out propCountOffset);
            _metadataPtr = _objStart + propCountOffset;

            // analyze main object type and it's offset and propertyIds flags
            _currentOffsetSize = ProcessTokenOffsetFlags(type);
            _currentPropertyIdSize = ProcessTokenPropertyFlags(type);
        }

        public int Count => _propCount;


        /// <summary>
        /// Returns an array of property names, ordered in the order it was stored 
        /// </summary>
        /// <returns></returns>
        public string[] GetPropertyNames()
        {
            var idsAndOffsets = new BlittableJsonDocument.PropertyTag[_propCount];
            var sortedNames = new string[_propCount];

            var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof(byte));

            // Prepare an array of all offsets and property ids
            for (var i = 0; i < _propCount; i++)
            {
                idsAndOffsets[i] = GetPropertyTag(i, metadataSize);
            }

            // sort according to offsets
            Array.Sort(idsAndOffsets, (tag1, tag2) => tag2.Position - tag1.Position);

            // generate string array, sorted according to it's offsets
            for (int i = 0; i < _propCount; i++)
            {
                sortedNames[i] = GetPropertyName(idsAndOffsets[i].PropertyId);
            }
            return sortedNames;
        }

        private BlittableJsonDocument.PropertyTag GetPropertyTag(int index, long metadataSize)
        {
            var propPos = _metadataPtr + index * metadataSize;
            var propertyId = ReadNumber(propPos + _currentOffsetSize, _currentPropertyIdSize);
            var propertyOffset = ReadNumber(propPos, _currentOffsetSize);
            var type = *(propPos + _currentOffsetSize + _currentPropertyIdSize);
            return new BlittableJsonDocument.PropertyTag
            {
                Position = propertyOffset,
                PropertyId = propertyId,
                Type = type
            };
        }

        private unsafe LazyStringValue GetPropertyName(int propertyId)
        {
            if (_parent != null)
                return _parent.GetPropertyName(propertyId);

            if (_propertyNames == null)
            {
                var totalNumberOfProps = (_size - (_propNames - _mem) - 1) / _propNamesDataOffsetSize;
                _propertyNames = new LazyStringValue[totalNumberOfProps];
            }

            var propertyName = _propertyNames[propertyId];
            if (propertyName == null)
            {
                var propertyNameOffsetPtr = _propNames + sizeof(byte) + propertyId * _propNamesDataOffsetSize;
                var propertyNameOffset = ReadNumber(propertyNameOffsetPtr, _propNamesDataOffsetSize);

                // Get the relative "In Document" position of the property Name
                var propRelativePos = _propNames - propertyNameOffset - _mem;

                _propertyNames[propertyId] = propertyName = ReadStringLazily((int)propRelativePos);
            }

            return propertyName;
        }

        public object this[string name]
        {
            get
            {
                object result = null;
                if (TryGetMember(name, out result) == false)
                    throw new ArgumentException($"Member named {name} does not exist");
                return result;
            }
        }

        public bool TryGetMember(string name, out object result)
        {
            Tuple<object, BlittableJsonToken> res;
            var found = TryGetMemberAsTypeValueTuple(name, out res);
            result = res.Item1;
            return found;
        }

        public Tuple<LazyStringValue, object> GetPropertyByIndex(int index)
        {
            if (index < 0 || index >= _propCount)
                throw new ArgumentOutOfRangeException(nameof(index));

            var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof(byte));
            var propertyTag = GetPropertyTag(index, metadataSize);
            var value = GetObject((BlittableJsonToken)propertyTag.Type, (int)(_objStart - _mem- propertyTag.Position));
            var stringValue = GetPropertyName(propertyTag.PropertyId);

            return Tuple.Create(stringValue, value);
        }

        public bool TryGetMemberAsTypeValueTuple(string name, out Tuple<object, BlittableJsonToken> result)
        {
            result = null;
            int min = 0, max = _propCount;

            // try get value from cache, works only with Blittable types, other objects are not stored for now
            if (_objectsPathCache != null && _objectsPathCache.TryGetValue(name, out result))
                return true;

            var comparer = _context.GetComparerFor(name);

            int mid = comparer.LastFoundAt ?? (min + max) / 2;
            if (mid > max)
                mid = max;
            do
            {
                var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof(byte));
                var propertyIntPtr = (long)_metadataPtr + (mid) * metadataSize;

                var offset = ReadNumber((byte*)propertyIntPtr, _currentOffsetSize);
                var propertyId = ReadNumber((byte*)propertyIntPtr + _currentOffsetSize, _currentPropertyIdSize);
                var type =
                    (BlittableJsonToken)
                        ReadNumber((byte*)(propertyIntPtr + _currentOffsetSize + _currentPropertyIdSize),
                            _currentPropertyIdSize);


                var cmpResult = ComparePropertyName(propertyId, comparer);
                if (cmpResult == 0)
                {
                    // found it...
                    result = Tuple.Create(GetObject(type, (int)((long)_objStart - (long)_mem - (long)offset)),
                        type & typesMask);
                    if (result.Item1 is BlittableJsonReaderBase)
                    {
                        if (_objectsPathCache == null)
                        {
                            _objectsPathCache = new Dictionary<string, Tuple<object, BlittableJsonToken>>();
                        }
                        _objectsPathCache[name] = result;
                    }
                    return true;
                }
                if (cmpResult > 0)
                {
                    min = mid + 1;
                }
                else
                {
                    max = mid - 1;
                }

                mid = (min + max) / 2;

            } while (min <= max);
            return false;
        }

        /// <summary>
        /// Compares property names between received StringToByteComparer and the string stored in the document's property names storage
        /// </summary>
        /// <param name="propertyId">Position of the string in the property ids storage</param>
        /// <param name="comparer">Comparer of a specific string value</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe int ComparePropertyName(int propertyId, LazyStringValue comparer)
        {
            // Get the offset of the property name from the _proprNames position
            var propertyNameOffsetPtr = _propNames + 1 + propertyId * _propNamesDataOffsetSize;
            var propertyNameOffset = ReadNumber(propertyNameOffsetPtr, _propNamesDataOffsetSize);

            // Get the relative "In Document" position of the property Name
            var propertyNameRelativePosition = _propNames - propertyNameOffset;
            var position = propertyNameRelativePosition - _mem;

            byte propertyNameLengthDataLength;

            // Get the property name size
            var size = ReadVariableSizeInt((int)position, out propertyNameLengthDataLength);

            // Return result of comparison between property name and received comparer
            return comparer.Compare(propertyNameRelativePosition + propertyNameLengthDataLength, size);
        }

        public void WriteTo(Stream stream, bool originalPropertyOrder = false)
        {
            var writer = new BlittableJsonTextWriter(_context, stream);
            if (originalPropertyOrder)
                WriteToOrdered(writer);
            else
                WriteTo(writer);
            writer.Flush();
        }

        private struct PropertyPos : IComparable<PropertyPos>
        {
            public int PropertyId;
            public int PropertyOffset;
            public BlittableJsonToken Type;
            public int CompareTo(PropertyPos other)
            {
                return other.PropertyOffset - PropertyOffset;
            }
        }

        // keeping this here because we aren't sure whatever it is worth it to 
        // get the same order of the documents for the perf cost
        private unsafe void WriteToOrdered(BlittableJsonTextWriter writer)
        {
            writer.WriteStartObject();
            var props = new PropertyPos[_propCount];
            var metadataSize = _currentOffsetSize + _currentPropertyIdSize + sizeof(byte);
            for (int i = 0; i < props.Length; i++)
            {
                var propertyIntPtr = _metadataPtr + i * metadataSize;
                var propertyOffset = ReadNumber(propertyIntPtr, _currentOffsetSize);
                var propertyId = ReadNumber(propertyIntPtr + _currentOffsetSize, _currentPropertyIdSize);
                var type = (BlittableJsonToken)(*(propertyIntPtr + _currentOffsetSize + _currentPropertyIdSize));
                props[i].PropertyOffset = propertyOffset;
                props[i].Type = type;
                props[i].PropertyId = propertyId;
            }
            Array.Sort(props);
            for (int i = 0; i < props.Length; i++)
            {
                if (i != 0)
                {
                    writer.WriteComma();
                }

                var lazyStringValue = GetPropertyName(props[i].PropertyId);
                writer.WritePropertyName(lazyStringValue);

                var val = GetObject(props[i].Type, (int)(_objStart - _mem - props[i].PropertyOffset));
                WriteValue(writer, props[i].Type & typesMask, val, originalPropertyOrder: true);
            }

            writer.WriteEndObject();
        }

        private void WriteTo(BlittableJsonTextWriter writer)
        {
            writer.WriteStartObject();
            var metadataSize = _currentOffsetSize + _currentPropertyIdSize + sizeof(byte);
            for (int i = 0; i < _propCount; i++)
            {
                if (i != 0)
                {
                    writer.WriteComma();
                }
                var propertyIntPtr = _metadataPtr + (i * metadataSize);
                var propertyOffset = ReadNumber(propertyIntPtr, _currentOffsetSize);
                var propertyId = ReadNumber(propertyIntPtr + _currentOffsetSize, _currentPropertyIdSize);
                var type = (BlittableJsonToken)(*(propertyIntPtr + _currentOffsetSize + _currentPropertyIdSize));

                writer.WritePropertyName(GetPropertyName(propertyId));

                var val = GetObject(type, (int)(_objStart - _mem - propertyOffset));
                WriteValue(writer, type & typesMask, val);
            }

            writer.WriteEndObject();
        }

        private void WriteValue(BlittableJsonTextWriter writer, BlittableJsonToken token, object val, bool originalPropertyOrder = false)
        {
            switch (token)
            {
                case BlittableJsonToken.StartArray:
                    WriteArrayToStream((BlittableJsonReaderArray)val, writer, originalPropertyOrder);
                    break;
                case BlittableJsonToken.StartObject:
                    var blittableJsonReaderObject = ((BlittableJsonReaderObject)val);
                    if (originalPropertyOrder)
                        blittableJsonReaderObject.WriteToOrdered(writer);
                    else
                        blittableJsonReaderObject.WriteTo(writer);
                    break;
                case BlittableJsonToken.String:
                    writer.WriteString((LazyStringValue)val);
                    break;
                case BlittableJsonToken.CompressedString:
                    writer.WriteString((LazyCompressedStringValue)val);
                    break;
                case BlittableJsonToken.Integer:
                    writer.WriteInteger((long)val);
                    break;
                case BlittableJsonToken.Float:
                    writer.WriteDouble((LazyDoubleValue)val);
                    break;
                case BlittableJsonToken.Boolean:
                    writer.WriteBool((bool)val);
                    break;
                case BlittableJsonToken.Null:
                    writer.WriteNull();
                    break;
                default:
                    throw new DataMisalignedException($"Unidentified Type {token}");
            }
        }

        private void WriteArrayToStream(BlittableJsonReaderArray blittableArray, BlittableJsonTextWriter writer, bool originalPropertyOrder)
        {
            writer.WriteStartArray();
            var length = blittableArray.Length;
            for (var i = 0; i < length; i++)
            {
                var propertyValueAndType = blittableArray.GetValueTokenTupleByIndex(i);

                if (i != 0)
                {
                    writer.WriteComma();
                }
                // write field value
                WriteValue(writer, propertyValueAndType.Item2, propertyValueAndType.Item1, originalPropertyOrder);

            }
            writer.WriteEndArray();
        }

        internal object GetObject(BlittableJsonToken type, int position)
        {

            switch (type & typesMask)
            {
                case BlittableJsonToken.StartObject:
                    return new BlittableJsonReaderObject(position, _parent ?? this, type);
                case BlittableJsonToken.StartArray:
                    return new BlittableJsonReaderArray(position, _parent ?? this, type);
                case BlittableJsonToken.Integer:
                    return ReadVariableSizeLong(position);
                case BlittableJsonToken.String:
                    return ReadStringLazily(position);
                case BlittableJsonToken.CompressedString:
                    return ReadCompressStringLazily(position);
                case BlittableJsonToken.Boolean:
                    return ReadNumber(_mem + position, 1) == 1;
                case BlittableJsonToken.Null:
                    return null;
                case BlittableJsonToken.Float:
                    return new LazyDoubleValue(ReadStringLazily(position));
                default:
                    throw new ArgumentOutOfRangeException((type).ToString());
            }
        }

    }
}