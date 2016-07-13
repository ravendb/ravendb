using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
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
            return TryGet(new StringSegment(name, 0, name.Length), out obj);
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
                obj = (T)result;
            }
            else
            {
                try
                {
                    var nullableType = Nullable.GetUnderlyingType(typeof(T));
                    if (nullableType != null)
                    {
                        if (nullableType.GetTypeInfo().IsEnum)
                        {
                            obj = (T)Enum.Parse(nullableType, result.ToString());
                            return;
                        }

                        obj = (T)Convert.ChangeType(result, nullableType);
                        return;
                    }

                    if (typeof(T).GetTypeInfo().IsEnum)
                    {
                        obj = (T)Enum.Parse(typeof(T), result.ToString());
                        return;
                    }

                    obj = (T)Convert.ChangeType(result, typeof(T));
                }
                catch (Exception e)
                {
                    throw new FormatException($"Could not convert {result.GetType().FullName} to {typeof(T).FullName}", e);
                }
            }
        }

        public bool TryGet(string name, out double dbl)
        {
            return TryGet(new StringSegment(name, 0, name.Length), out dbl);
        }

        public bool TryGet(StringSegment name, out double dbl)
        {
            object result;
            if (TryGetMember(name, out result) == false)
            {
                dbl = 0;
                return false;
            }

            var lazyDouble = result as LazyDoubleValue;
            if (lazyDouble != null)
            {
                dbl = lazyDouble;
                return true;
            }

            dbl = 0;
            return false;
        }

        public bool TryGet(string name, out string str)
        {
            return TryGet(new StringSegment(name, 0, name.Length), out str);
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
            return TryGetMember(new StringSegment(name, 0, name.Length), out result);
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
            result = GetObject((BlittableJsonToken)propertyTag.Type, (int)(_objStart - _mem - propertyTag.Position));
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
            return GetPropertyIndex(new StringSegment(name, 0, name.Length));
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

        public void BlittableValidation()
        {
            byte offset;
            var currentSize = Size - 1;
            int rootPropOffsetSize;
            int rootPropIdSize;

            if (currentSize < 1)
                throw new InvalidDataException("Illegal data");
            var rootToken = TokenValidation(*(_mem + currentSize), out rootPropOffsetSize, out rootPropIdSize);
            if (rootToken != BlittableJsonToken.StartObject)
                throw new InvalidDataException("Illegal root object");
            currentSize--;

            var propsOffsetList = ReadVariableSizeIntInReverse(_mem, currentSize, out offset);
            if (offset > currentSize)
                throw new InvalidDataException("Properties names offset not valid");
            currentSize -= offset;

            var rootMetadataOffset = ReadVariableSizeIntInReverse(_mem, currentSize, out offset);
            if (offset > currentSize)
                throw new InvalidDataException("Root metadata offset not valid");
            currentSize -= offset;

            if ((propsOffsetList > currentSize) || (propsOffsetList <= 0))
                throw new InvalidDataException("Properties names offset not valid");

            int propNamesOffsetSize;
            var token = (BlittableJsonToken)(*(_mem + propsOffsetList));
            propNamesOffsetSize = ProcessTokenOffsetFlags(token);

            if (((token & (BlittableJsonToken)0xC0) != 0) || ((TypesMask & token) != 0x00))
                throw new InvalidDataException("Properties names token not valid");

            var numberOfProps = (currentSize - propsOffsetList) / propNamesOffsetSize;
            currentSize = PropertiesNamesValidation(numberOfProps, propsOffsetList,
                propNamesOffsetSize, propsOffsetList);

            if ((rootMetadataOffset > currentSize) || (rootMetadataOffset < 0))
                throw new InvalidDataException("Root metadata offset not valid");
            var current = PropertiesValidation(rootToken, rootPropOffsetSize, rootPropIdSize,
                rootMetadataOffset, numberOfProps);

            if (current != currentSize)
                throw new InvalidDataException("Root metadata not valid");
        }

        private int PropertiesNamesValidation(int numberOfProps, int propsOffsetList, int propsNamesOffsetSize,
            int currentSize)
        {
            var offsetCounter = 0;
            for (var i = numberOfProps - 1; i >= 0; i--)
            {
                int stringLength;
                var nameOffset = 0;
                nameOffset = ReadNumber((_mem + propsOffsetList + 1 + i * propsNamesOffsetSize),
                    propsNamesOffsetSize);
                if ((nameOffset > currentSize) || (nameOffset < 0))
                    throw new InvalidDataException("Properties names offset not valid");
                stringLength = StringValidation(propsOffsetList - nameOffset);
                if (offsetCounter + stringLength != nameOffset)
                    throw new InvalidDataException("Properties names offset not valid");
                offsetCounter = nameOffset;
                currentSize -= stringLength;
            }
            return currentSize;
        }

        private int StringValidation(int stringOffset)
        {
            byte lenOffset;
            byte escOffset;
            int stringLength;
            stringLength = ReadVariableSizeInt(stringOffset, out lenOffset);
            if (stringLength < 0)
                throw new InvalidDataException("String not valid");
            var str = stringOffset + lenOffset;
            var escCount = ReadVariableSizeInt(stringOffset + lenOffset + stringLength, out escOffset);
            if (escCount != 0)
            {
                for (var i = 0; i < escCount; i++)
                {
                    var escCharOffset = ReadNumber(_mem + str + stringLength + escOffset + i, 1);
                    var escChar = (char)ReadNumber(_mem + str + stringLength + escOffset - 1 - escCharOffset, 1);
                    switch (escChar)
                    {
                        case '\\':
                        case '/':
                        case '"':
                        case 'b':
                        case 'f':
                        case 'n':
                        case 'r':
                        case 't':
                            break;
                        default:
                            throw new InvalidDataException("String not valid, invalid escape character: " + escChar);
                    };
                }
            }
            return stringLength + escOffset + escCount + lenOffset;
        }

        private BlittableJsonToken TokenValidation(byte tokenStart, out int propOffsetSize,
            out int propIdSize)
        {
            var token = (BlittableJsonToken)tokenStart;
            var tokenType = ProcessTokenTypeFlags(token);
            propOffsetSize = ((tokenType == BlittableJsonToken.StartObject) ||
                              (tokenType == BlittableJsonToken.StartArray))
                ? ProcessTokenOffsetFlags(token)
                : 0;

            propIdSize = (tokenType == BlittableJsonToken.StartObject)
                ? ProcessTokenPropertyFlags(token)
                : 0;
            return tokenType;
        }

        private int PropertiesValidation(BlittableJsonToken rootTokenTypen, int mainPropOffsetSize, int mainPropIdSize,
            int objStartOffset, int numberOfPropsNames)
        {
            byte offset;
            var numberOfProperties = ReadVariableSizeInt(_mem + objStartOffset, 0, out offset);
            var current = objStartOffset + offset;

            if (numberOfProperties < 0)
                throw new InvalidDataException("Number of properties not valid");

            for (var i = 1; i <= numberOfProperties; i++)
            {
                var propOffset = ReadNumber(_mem + current, mainPropOffsetSize);
                if ((propOffset > objStartOffset) || (propOffset < 0))
                    throw new InvalidDataException("Properties offset not valid");
                current += mainPropOffsetSize;

                if (rootTokenTypen == BlittableJsonToken.StartObject)
                {
                    var id = ReadNumber(_mem + current, mainPropIdSize);
                    if ((id > numberOfPropsNames) || (id < 0))
                        throw new InvalidDataException("Properties id not valid");
                    current += mainPropIdSize;
                }

                int propOffsetSize;
                int propIdSize;
                var tokenType = TokenValidation(*(_mem + current), out propOffsetSize, out propIdSize);
                current++;

                var propValueOffset = objStartOffset - propOffset;

                switch (tokenType)
                {
                    case BlittableJsonToken.StartObject:
                        PropertiesValidation(tokenType, propOffsetSize, propIdSize, propValueOffset, numberOfPropsNames);
                        break;
                    case BlittableJsonToken.StartArray:
                        PropertiesValidation(tokenType, propOffsetSize, propIdSize, propValueOffset, numberOfPropsNames);
                        break;
                    case BlittableJsonToken.Integer:
                        ReadVariableSizeLong(propValueOffset);
                        break;
                    case BlittableJsonToken.Float:
                        var floatLen = ReadNumber(_mem + objStartOffset - propOffset, 1);
                        var floatStringBuffer = new string(' ', floatLen);
                        fixed (char* pChars = floatStringBuffer)
                        {
                            for (int j = 0; j < floatLen; j++)
                            {
                                pChars[j] = (char)ReadNumber((_mem + objStartOffset - propOffset + 1 + j), sizeof(byte));
                            }
                        }
                        double _double;
                        var result = double.TryParse(floatStringBuffer,NumberStyles.Float,CultureInfo.InvariantCulture, out _double);
                        if (!(result))
                            throw new InvalidDataException("Double not valid (" + floatStringBuffer + ")");
                        break;
                    case BlittableJsonToken.String:
                        StringValidation(propValueOffset);
                        break;
                    case BlittableJsonToken.CompressedString:
                        var stringLength = ReadVariableSizeInt(propValueOffset, out offset);
                        var compressedStringLength = ReadVariableSizeInt(propValueOffset + offset, out offset);
                        if ((compressedStringLength > stringLength) ||
                            (compressedStringLength < 0) ||
                            (stringLength < 0))
                            throw new InvalidDataException("Compressed string not valid");
                        break;
                    case BlittableJsonToken.Boolean:
                        var boolProp = ReadNumber(_mem + propValueOffset, 1);
                        if ((boolProp != 0) && (boolProp != 1))
                            throw new InvalidDataException("Bool not valid");
                        break;
                    case BlittableJsonToken.Null:
                        if (ReadNumber(_mem + propValueOffset, 1) != 0)
                            throw new InvalidDataException("Null not valid");
                        break;
                    default:
                        throw new InvalidDataException("Token type not valid");
                }
            }
            return current;
        }
    }
}