using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;
using Sparrow.Json.Parsing;

namespace Sparrow.Json
{
    public unsafe class BlittableJsonReaderObject : BlittableJsonReaderBase, IDisposable
    {
        private readonly BlittableJsonDocumentBuilder _builder;
        private readonly CachedProperties _cachedProperties;
        private readonly byte* _metadataPtr;
        private readonly int _propCount;
        private readonly long _currentOffsetSize;
        private readonly long _currentPropertyIdSize;
        private readonly byte* _objStart;
        private LazyStringValue[] _propertyNames;

        public DynamicJsonValue Modifications;

        private Dictionary<StringSegment, object> _objectsPathCache;
        private Dictionary<int, object> _objectsPathCacheByIndex;

        public override string ToString()
        {
            var memoryStream = new MemoryStream();
            _context.Write(memoryStream, this);
            memoryStream.Position = 0;
            return new StreamReader(memoryStream).ReadToEnd();
        }

        public BlittableJsonReaderObject(byte* mem, int size, JsonOperationContext context, 
            BlittableJsonDocumentBuilder builder = null,
            CachedProperties cachedProperties = null)
        {
            _builder = builder;
            _cachedProperties = cachedProperties;
            _mem = mem; // get beginning of memory pointer
            _size = size; // get document size
            _context = context;

            byte offset;
            var propOffsetStart = _size - 2;
            var propsOffset = ReadVariableSizeIntInReverse(_mem, propOffsetStart, out offset);
            // init document level properties
            if (_cachedProperties == null)
            {
                SetupPropertiesAccess(mem, propsOffset);
            }
            else
            {
                if (_cachedProperties.Version != propsOffset)
                    throw new InvalidOperationException(
                        $"This object requires an external properties cache with version {propsOffset}, but got one with {_cachedProperties.Version}");
            }
            // get pointer to property names array on document level

            // init root level object properties
            var objStartOffset = ReadVariableSizeIntInReverse(_mem, propOffsetStart - offset, out offset);
            // get offset of beginning of data of the main object
            byte propCountOffset;
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

        private void SetupPropertiesAccess(byte* mem, int propsOffset)
        {
            _propNames = (mem + propsOffset);
            var propNamesOffsetFlag = (BlittableJsonToken) (*_propNames);
            switch (propNamesOffsetFlag)
            {
                case BlittableJsonToken.OffsetSizeByte:
                    _propNamesDataOffsetSize = sizeof (byte);
                    break;
                case BlittableJsonToken.OffsetSizeShort:
                    _propNamesDataOffsetSize = sizeof (short);
                    break;
                case BlittableJsonToken.OffsetSizeInt:
                    _propNamesDataOffsetSize = sizeof (int);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(
                        $"Property names offset flag should be either byte, short of int, instead of {propNamesOffsetFlag}");
            }
        }

        public unsafe BlittableJsonReaderObject(int pos, BlittableJsonReaderObject parent, BlittableJsonToken type)
        {
            _parent = parent;
            _context = parent._context;
            _mem = parent._mem;
            _size = parent._size;
            _propNames = parent._propNames;
            _cachedProperties = parent._cachedProperties;

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

            _objStart = _mem + pos;
            byte propCountOffset;
            _propCount = ReadVariableSizeInt(pos, out propCountOffset);
            _metadataPtr = _objStart + propCountOffset;

            // analyze main object type and it's offset and propertyIds flags
            _currentOffsetSize = ProcessTokenOffsetFlags(type);
            _currentPropertyIdSize = ProcessTokenPropertyFlags(type);
        }

        public int Size => _size;

        public int Count => _propCount;
        public byte* BasePointer => _mem;


        /// <summary>
        /// Returns an array of property names, ordered in the order it was stored 
        /// </summary>
        /// <returns></returns>
        public string[] GetPropertyNames()
        {
            var idsAndOffsets = new BlittableJsonDocumentBuilder.PropertyTag[_propCount];
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

        private BlittableJsonDocumentBuilder.PropertyTag GetPropertyTag(int index, long metadataSize)
        {
            var propPos = _metadataPtr + index * metadataSize;
            var propertyId = ReadNumber(propPos + _currentOffsetSize, _currentPropertyIdSize);
            var propertyOffset = ReadNumber(propPos, _currentOffsetSize);
            var type = *(propPos + _currentOffsetSize + _currentPropertyIdSize);
            return new BlittableJsonDocumentBuilder.PropertyTag
            {
                Position = propertyOffset,
                PropertyId = propertyId,
                Type = type
            };
        }

        private unsafe LazyStringValue GetPropertyName(int propertyId)
        {
            if (_cachedProperties != null)
                return _cachedProperties.GetProperty(propertyId);

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

        public bool TryGet<T>(string name, out T obj)
        {
            return TryGet(new StringSegment(name,0,name.Length),out obj);
        }

        public bool TryGet<T>(StringSegment name, out T obj)
        {
            object result;
            if (TryGetMember(name, out result) == false)
            {
                obj = default(T);
                return false;
            }
            ConvertType(result, out obj);
            return true;
        }

        internal static void ConvertType<T>(object result, out T obj)
        {
            if (result == null)
            {
                obj = default(T);
            }
            else if (result is T)
            {
                obj = (T) result;
            }
            else
            {
                try
                {
                    // todo: review this, is it good enough?
                    if (typeof(T).GetTypeInfo().IsEnum)
                        result = Enum.Parse(typeof(T), result.ToString());

                    obj = result == null ? default(T) : (T)Convert.ChangeType(result, typeof(T));
                }
                catch (Exception e)
                {
                    throw new FormatException($"Could not convert {result.GetType().FullName} to {typeof (T).FullName}",e);
                }
            }
        }

        public bool TryGet(string name, out string str)
        {
            return TryGet(new StringSegment(name,0,name.Length),out str);
        }


        public bool TryGet(StringSegment name, out string str)
        {
            object result;
            if (TryGetMember(name, out result) == false)
            {
                str = null;
                return false;
            }
            return ChangeTypeToString(result, out str);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool ChangeTypeToString(object result, out string str)
        {
            var lazyCompressedStringValue = result as LazyCompressedStringValue;
            if (lazyCompressedStringValue != null)
            {
                str = lazyCompressedStringValue;
                return true;
            }

            var lazyStringValue = result as LazyStringValue;
            if (lazyStringValue != null)
            {
                str = lazyStringValue;
                return true;
            }

            str = null;
            return false;
        }

        public bool TryGetMember(string name, out object result)
        {
            return TryGetMember(new StringSegment(name,0,name.Length),out result);
        }


        public bool TryGetMember(StringSegment name, out object result)
        {
            // try get value from cache, works only with Blittable types, other objects are not stored for now
            if (_objectsPathCache != null && _objectsPathCache.TryGetValue(name, out result))
            {
                return true;
            }
            var index = GetPropertyIndex(name);
            if (index == -1)
            {
                result = null;
                return false;
            }
            var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof(byte));
            var propertyTag = GetPropertyTag(index, metadataSize);
            result = GetObject((BlittableJsonToken) propertyTag.Type, (int) (_objStart - _mem - propertyTag.Position));
            if (result is BlittableJsonReaderBase)
            {
                if (_objectsPathCache == null)
                {
                    _objectsPathCache = new Dictionary<StringSegment, object>();
                    _objectsPathCacheByIndex = new Dictionary<int, object>();
                }
                _objectsPathCache[name] = result;
                _objectsPathCacheByIndex[index] = result;
            }
            return true;
        }

        public Tuple<LazyStringValue, object, BlittableJsonToken> GetPropertyByIndex(int index)
        {
            if (index < 0 || index >= _propCount)
                throw new ArgumentOutOfRangeException(nameof(index));

            var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof(byte));
            var propertyTag = GetPropertyTag(index, metadataSize);
            var stringValue = GetPropertyName(propertyTag.PropertyId);
            var blittableJsonToken = (BlittableJsonToken)propertyTag.Type;

            object result;
            if (_objectsPathCacheByIndex != null && _objectsPathCacheByIndex.TryGetValue(index, out result))
            {
                return Tuple.Create(stringValue, result, blittableJsonToken);
            }

            var value = GetObject(blittableJsonToken, (int)(_objStart - _mem - propertyTag.Position));
            // we explicitly don't add it to the cache here, we don't need to.
            // users will always access the props by name, not by id.
            return Tuple.Create(stringValue, value, blittableJsonToken);
        }

        public int GetPropertyIndex(string name)
        {
            return GetPropertyIndex(new StringSegment(name,0,name.Length));
        }


        public int GetPropertyIndex(StringSegment name)
        {
            if (_cachedProperties != null)
            {
                var propName = _context.GetLazyStringForFieldWithCaching(name.Value);
                return _cachedProperties.GetPropertyId(propName);
            }

            int min = 0, max = _propCount;
            var comparer = _context.GetLazyStringForFieldWithCaching(name.Value);

            int mid = comparer.LastFoundAt ?? (min + max) / 2;
            if (mid > max)
                mid = max;
            do
            {
                var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof(byte));
                var propertyIntPtr = (long)_metadataPtr + (mid) * metadataSize;

                var propertyId = ReadNumber((byte*)propertyIntPtr + _currentOffsetSize, _currentPropertyIdSize);


                var cmpResult = ComparePropertyName(propertyId, comparer);
                if (cmpResult == 0)
                {
                    return mid;
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
            return -1;
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

        public int[] GetPropertiesByInsertionOrder()
        {
            var props = new int[_propCount];
            var offsets = new int[_propCount];
            var metadataSize = _currentOffsetSize + _currentPropertyIdSize + sizeof(byte);
            for (int i = 0; i < props.Length; i++)
            {
                var propertyIntPtr = _metadataPtr + i * metadataSize;
                offsets[i] = ReadNumber(propertyIntPtr, _currentOffsetSize);
                props[i] = i;
            }
            Array.Sort(offsets, props, NumericDescendingComparer.Instance);
            return props;
        }

        

        internal object GetObject(BlittableJsonToken type, int position)
        {
            switch (type & TypesMask)
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

        public void Dispose()
        {
            _builder?.Dispose();
        }

        public void CopyTo(byte* ptr)
        {
            Memory.Copy(ptr, _mem, _size);
        }

    }
}