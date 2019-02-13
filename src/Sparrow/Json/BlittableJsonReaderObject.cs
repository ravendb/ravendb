using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Primitives;
using Sparrow.Binary;
using Sparrow.Json.Parsing;

namespace Sparrow.Json
{
    public unsafe class BlittableJsonReaderObject : BlittableJsonReaderBase, IDisposable
    {
        private AllocatedMemoryData _allocatedMemory;
        private UnmanagedWriteBuffer _buffer;
        private byte* _metadataPtr;
        private readonly int _size;
        private readonly int _propCount;
        private readonly long _currentOffsetSize;
        private readonly long _currentPropertyIdSize;
        private readonly bool _isRoot;
        private byte* _objStart;

        public DynamicJsonValue Modifications;

        private Dictionary<StringSegment, object> _objectsPathCache;
        private Dictionary<int, object> _objectsPathCacheByIndex;

        public override string ToString()
        {
            AssertContextNotDisposed();

            using (var memoryStream = new MemoryStream())
            {
                WriteJsonTo(memoryStream);
                memoryStream.Position = 0;

                return new StreamReader(memoryStream).ReadToEnd();
            }
        }

        public void WriteJsonTo(Stream stream)
        {         
            AssertContextNotDisposed();

            _context.Write(stream, this);
        }

        public BlittableJsonReaderObject(byte* mem, int size, JsonOperationContext context, UnmanagedWriteBuffer buffer = default(UnmanagedWriteBuffer))
            : base(context)
        {
            if (size == 0)
                ThrowOnZeroSize(size);

            _isRoot = true;
            _buffer = buffer;
            _mem = mem; // get beginning of memory pointer
            _size = size; // get document size

            NoCache = NoCache;

            byte offset;
            var propOffsetStart = _size - 2;
            var propsOffset = ReadVariableSizeIntInReverse(_mem, propOffsetStart, out offset);
            // init document level properties
            SetupPropertiesAccess(mem, propsOffset);

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

        private static void ThrowOnZeroSize(int size)
        {
            //otherwise SetupPropertiesAccess will throw because of the memory garbage
            //(or won't throw, but this is actually worse!)
            throw new ArgumentException("BlittableJsonReaderObject does not support objects with zero size",
                nameof(size));
        }

        private void SetupPropertiesAccess(byte* mem, int propsOffset)
        {
            AssertContextNotDisposed();

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

        public BlittableJsonReaderObject(int pos, BlittableJsonReaderObject parent, BlittableJsonToken type)
            : base(parent._context)
        {
            _isRoot = false;
            _parent = parent;
            _mem = parent._mem;
            _size = parent._size;
            _propNames = parent._propNames;

            NoCache = parent.NoCache;

            var propNamesOffsetFlag = (BlittableJsonToken)(*_propNames);

            if (propNamesOffsetFlag == BlittableJsonToken.OffsetSizeByte)
                _propNamesDataOffsetSize = sizeof(byte);
            else if (propNamesOffsetFlag == BlittableJsonToken.OffsetSizeShort)
                _propNamesDataOffsetSize = sizeof(short);
            else if (propNamesOffsetFlag == BlittableJsonToken.OffsetSizeInt)
                _propNamesDataOffsetSize = sizeof(int);
            else
                ThrowOutOfRangeException(propNamesOffsetFlag);

            _objStart = _mem + pos;
            byte propCountOffset;
            _propCount = ReadVariableSizeInt(pos, out propCountOffset);
            _metadataPtr = _objStart + propCountOffset;

            // analyze main object type and it's offset and propertyIds flags
            _currentOffsetSize = ProcessTokenOffsetFlags(type);
            _currentPropertyIdSize = ProcessTokenPropertyFlags(type);
        }

        private static void ThrowOutOfRangeException(BlittableJsonToken token)
        {
            throw new ArgumentOutOfRangeException(
                $"Property names offset flag should be either byte, short of int, instead of {token}");
        }

        private static void ThrowObjectDisposed()
        {
            throw new ObjectDisposedException("blittable object has been disposed");
        }

        public int Size => _size;

        public int Count => _propCount;

        public byte* BasePointer
        {
            get
            {
                AssertContextNotDisposed();

                if (_parent != null)
                    InvalidAttemptToCopyNestedObject();

                return _mem;
            }
        }

        public ulong DebugHash
        {
            get
            {
                AssertContextNotDisposed();

                return Hashing.XXHash64.Calculate(_mem, (ulong)_size);
            }
        }

        public long Location => (long)_objStart;


        /// <summary>
        /// Returns an array of property names, ordered in the order it was stored 
        /// </summary>
        /// <returns></returns>
        public string[] GetPropertyNames()
        {
            AssertContextNotDisposed();

            var offsets = new int[_propCount];
            var propertyNames = new string[_propCount];

            var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof(byte));

            for (int i = 0; i < _propCount; i++)
            {
                BlittableJsonToken token;
                int position;
                int id;
                GetPropertyTypeAndPosition(i, metadataSize, out token, out position, out id);
                offsets[i] = position;
                propertyNames[i] = GetPropertyName(id);
            }

            // sort according to offsets
            Sorter<int, string, NumericDescendingComparer> sorter;
            sorter.Sort(offsets, propertyNames);

            return propertyNames;
        }

        private LazyStringValue GetPropertyName(int propertyId)
        {
            AssertContextNotDisposed();

            var propertyNameOffsetPtr = _propNames + sizeof(byte) + propertyId * _propNamesDataOffsetSize;
            var propertyNameOffset = ReadNumber(propertyNameOffsetPtr, _propNamesDataOffsetSize);

            // Get the relative "In Document" position of the property Name
            var propRelativePos = _propNames - propertyNameOffset - _mem;

            var propertyName = ReadStringLazily((int)propRelativePos);
            return propertyName;
        }


        public object this[string name]
        {
            get
            {
                if (TryGetMember(name, out object result) == false)
                    throw new ArgumentException($"Member named {name} does not exist");
                return result;
            }
        }

        public bool TryGet<T>(string name, out T obj)
        {
            return TryGet(new StringSegment(name), out obj);
        }

        public bool TryGetWithoutThrowingOnError<T>(string name, out T obj)
        {
            if (TryGetMember(name, out object result) == false)
            {
                obj = default(T);
                return false;
            }

            return TryConvertType(result, out obj);
        }

        public bool TryGetWithoutThrowingOnError<T>(StringSegment name, out T obj)
        {
            if (TryGetMember(name, out object result) == false)
            {
                obj = default(T);
                return false;
            }

            return TryConvertType(result, out obj);
        }

        public bool TryGet<T>(StringSegment name, out T obj)
        {
            if (TryGetMember(name, out object result) == false)
            {
                obj = default(T);
                return false;
            }

            ConvertType(result, out obj);
            return true;
        }

        private static void ThrowFormatException(object value, string fromType, string toType)
        {
            throw new FormatException($"Could not convert {fromType} ('{value}') to {toType}");
        }

        private static void ThrowFormatException(object value, string fromType, string toType, Exception e)
        {
            throw new FormatException($"Could not convert {fromType} ('{value}') to {toType}", e);
        }

        internal static bool TryConvertType<T>(object result, out T obj)
        {
            obj = default(T);
            if (result == null)
                return true;

            if (result is T)
            {
                obj = (T)result;
            }
            //just in case -> have better exception in this use-case
            else if (typeof(T) == typeof(BlittableJsonReaderObject) && result.GetType() == typeof(BlittableJsonReaderArray))
            {
                return false;
            }
            //just in case -> have better exception in this use-case
            else if (typeof(T) == typeof(BlittableJsonReaderArray) && result.GetType() == typeof(BlittableJsonReaderObject))
            {
                return false;
            }
            else
            {
                var type = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);

                try
                {
                    if (type.GetTypeInfo().IsEnum)
                    {
                        obj = (T)Enum.Parse(type, result.ToString());
                    }
                    else if (type == typeof(DateTime))
                    {
                        if (ChangeTypeToString(result, out string dateTimeString) == false)
                            return false;
                        if (DateTime.TryParseExact(dateTimeString, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime time) == false)
                            return false;

                        obj = (T)(object)time;
                    }
                    else if (type == typeof(DateTimeOffset))
                    {
                        if (ChangeTypeToString(result, out string dateTimeOffsetString) == false)
                            return false;
                        if (DateTimeOffset.TryParseExact(dateTimeOffsetString, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                                DateTimeStyles.RoundtripKind, out DateTimeOffset time) == false)
                            return false;

                        obj = (T)(object)time;
                    }
                    else if (type == typeof(TimeSpan))
                    {
                        if (ChangeTypeToString(result, out string timeSpanString) == false)
                            return false;
                        if (TimeSpan.TryParseExact(timeSpanString, "c", CultureInfo.InvariantCulture, out TimeSpan timeSpan) == false)
                            return false;
                        obj = (T)(object)timeSpan;
                    }
                    else if (type == typeof(Guid))
                    {
                        if (ChangeTypeToString(result, out string guidString) == false)
                            return false;
                        if (Guid.TryParse(guidString, out Guid guid) == false)
                            return false;
                        obj = (T)(object)guid;
                    }
                    else if (result is LazyStringValue lazyStringValue)
                    {
                        obj = (T)Convert.ChangeType(lazyStringValue.ToString(), type);
                        // TODO: Try
                        // obj = (T)TypeDescriptor.GetConverter(typeof(T)).ConvertFromString(lazyStringValue.ToString());
                    }
                    else if (result is LazyNumberValue lnv)
                    {
                        obj = (T)Convert.ChangeType(lnv, type);
                    }
                    else if (result is LazyCompressedStringValue lazyCompressStringValue)
                    {
                        if (type == typeof(LazyStringValue))
                            obj = (T)(object)lazyCompressStringValue.ToLazyStringValue();
                        else
                            obj = (T)Convert.ChangeType(lazyCompressStringValue.ToString(), type);
                    }
                    else
                    { 
                        obj = (T)Convert.ChangeType(result, type);
                    }
                }
                catch
                {
                    return false;
                }                
            }

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
            //just in case -> have better exception in this use-case
            else if (typeof(T) == typeof(BlittableJsonReaderObject) &&
                     result.GetType() == typeof(BlittableJsonReaderArray))
            {
                obj = default(T);
                ThrowFormatException(result, result.GetType().FullName, nameof(BlittableJsonReaderObject));
            }
            //just in case -> have better exception in this use-case
            else if (typeof(T) == typeof(BlittableJsonReaderArray) &&
                     result.GetType() == typeof(BlittableJsonReaderObject))
            {
                obj = default(T);
                ThrowFormatException(result, result.GetType().FullName, nameof(BlittableJsonReaderArray));
            }
            else
            {
                obj = default(T);
                var type = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
                try
                {
                    if (type.GetTypeInfo().IsEnum)
                    {
                        obj = (T)Enum.Parse(type, result.ToString());
                    }
                    else if (type == typeof(DateTime))
                    {
                        if (ChangeTypeToString(result, out string dateTimeString) == false)
                            ThrowFormatException(result, result.GetType().FullName, "string");
                        if (DateTime.TryParseExact(dateTimeString, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind,
                                out DateTime time) == false)
                            ThrowFormatException(result, result.GetType().FullName, "DateTime");
                        obj = (T)(object)time;
                    }
                    else if (type == typeof(DateTimeOffset))
                    {
                        if (ChangeTypeToString(result, out string dateTimeOffsetString) == false)
                            ThrowFormatException(result, result.GetType().FullName, "string");
                        if (DateTimeOffset.TryParseExact(dateTimeOffsetString, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                                DateTimeStyles.RoundtripKind, out DateTimeOffset time) == false)
                            ThrowFormatException(result, result.GetType().FullName, "DateTimeOffset");
                        obj = (T)(object)time;
                    }
                    else if (type == typeof(TimeSpan))
                    {
                        if (ChangeTypeToString(result, out string timeSpanString) == false)
                            ThrowFormatException(result, result.GetType().FullName, "string");
                        if (TimeSpan.TryParseExact(timeSpanString, "c", CultureInfo.InvariantCulture, out TimeSpan timeSpan) == false)
                            ThrowFormatException(result, result.GetType().FullName, "TimeSpan");
                        obj = (T)(object)timeSpan;
                    }
                    else if (type == typeof(Guid))
                    {
                        if (ChangeTypeToString(result, out string guidString) == false)
                            ThrowFormatException(result, result.GetType().FullName, "string");
                        if (Guid.TryParse(guidString, out Guid guid) == false)
                            ThrowFormatException(result, result.GetType().FullName, "Guid");
                        obj = (T)(object)guid;
                    }
                    else
                    {
                        switch (result)
                        {
                            case LazyStringValue lazyStringValue:
                                obj = (T)Convert.ChangeType(lazyStringValue.ToString(), type);
                                break;
                            case LazyNumberValue lazyNumberValue:
                                obj = (T)Convert.ChangeType(lazyNumberValue, type);
                                break;
                            case LazyCompressedStringValue lazyCompressStringValue:
                                if (type == typeof(LazyStringValue))
                                    obj = (T)(object)lazyCompressStringValue.ToLazyStringValue();
                                else
                                    obj = (T)Convert.ChangeType(lazyCompressStringValue.ToString(), type);
                                break;
                            default:
                                obj = (T)Convert.ChangeType(result, type);
                                break;
                        }
                    }
                }
                catch (Exception e)
                {
                    ThrowFormatException(result, result.GetType().FullName, type.FullName, e);
                }
            }
        }

        public bool TryGet(string name, out double? nullableDbl)
        {
            return TryGet(new StringSegment(name), out nullableDbl);
        }

        public bool TryGet(StringSegment name, out double? nullableDbl)
        {
            if (TryGet(name, out double doubleNum) == false)
            {
                nullableDbl = null;
                return false;
            }

            nullableDbl = doubleNum;
            return true;
        }

        public bool TryGet(string name, out double doubleNum)
        {
            return TryGet(new StringSegment(name), out doubleNum);
        }

        public bool TryGet(StringSegment name, out double doubleNum)
        {
            if (TryGetMember(name, out var result) == false)
            {
                doubleNum = 0;
                return false;
            }

            switch (result)
            {
                case LazyNumberValue lazyDouble:
                    doubleNum = lazyDouble;
                    return true;
                case long longNum:
                    doubleNum = longNum;
                    return true;
            }

            doubleNum = 0;
            return false;
        }

        public bool TryGet(string name, out string str)
        {
            return TryGet(new StringSegment(name), out str);
        }

        public bool TryGet(StringSegment name, out string str)
        {
            if (TryGetMember(name, out var result) == false)
            {
                str = null;
                return false;
            }
            return ChangeTypeToString(result, out str);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool ChangeTypeToString(object result, out string str)
        {
            switch (result)
            {
                case null:
                    str = null;
                    return true;
                case LazyCompressedStringValue lazyCompressedStringValue:
                    str = lazyCompressedStringValue;
                    return true;
                case LazyStringValue lazyStringValue:
                    str = lazyStringValue;
                    return true;
                case StringSegment stringSegmentValue:
                    str = stringSegmentValue.Value;
                    return true;
            }

            str = null;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetMember(string name, out object result)
        {
            return TryGetMember(new StringSegment(name), out result);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryGetObjectByIndex(int index, BlittableJsonToken expectedToken, out object result)
        {
            AssertContextNotDisposed();

            var metadataSize = _currentOffsetSize + _currentPropertyIdSize + sizeof(byte);

            GetPropertyTypeAndPosition(index, metadataSize, out var token, out var position, out var propertyId);
            if (CompareTokens(expectedToken, token) == false)
            {
                result = null;
                return false;
            }
            result = GetObject(token, (int)(_objStart - _mem - position));
            return true;
        }

        private bool CompareTokens(BlittableJsonToken firstToken, BlittableJsonToken secondToken)
        {
            AssertContextNotDisposed();

            var firstClearedToken = (firstToken & TypesMask);
            var secondClearedToken = (secondToken & TypesMask);
            if (firstClearedToken == secondClearedToken)
                return true;
            
            return (firstClearedToken == BlittableJsonToken.EmbeddedBlittable && secondClearedToken == BlittableJsonToken.StartObject 
                    || firstClearedToken == BlittableJsonToken.StartObject && secondClearedToken == BlittableJsonToken.EmbeddedBlittable);
        }

        public bool TryGetMember(StringSegment name, out object result)
        {
            AssertContextNotDisposed();

            if (_mem == null)
                goto ThrowDisposed;

            bool opResult = true;

            // try get value from cache, works only with Blittable types, other objects are not stored for now
            if (_objectsPathCache != null && _objectsPathCache.TryGetValue(name, out result))
                goto Return;

            var index = GetPropertyIndex(name);
            if (index == -1)
            {
                result = null;
                opResult = false;
                goto Return;
            }

            var metadataSize = _currentOffsetSize + _currentPropertyIdSize + sizeof(byte);

            BlittableJsonToken token;
            int position;
            int propertyId;
            GetPropertyTypeAndPosition(index, metadataSize, out token, out position, out propertyId);
            result = GetObject(token, (int)(_objStart - _mem - position));

            if (NoCache == false && result is BlittableJsonReaderBase)
            {
                AddToCache(name, result, index);
            }

        Return:
            return opResult;

        ThrowDisposed:
            ThrowObjectDisposed();
            result = null;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddToCache(StringSegment name, object result, int index)
        {
            AssertContextNotDisposed();

            if (_objectsPathCache == null)
            {
                _context.AcquirePathCache(out _objectsPathCache, out _objectsPathCacheByIndex);
            }
            _objectsPathCache[name] = result;
            _objectsPathCacheByIndex[index] = result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void GetPropertyTypeAndPosition(int index, long metadataSize, out BlittableJsonToken token, out int position, out int propertyId)
        {
            AssertContextNotDisposed();

            var propPos = _metadataPtr + index * metadataSize;
            position = ReadNumber(propPos, _currentOffsetSize);
            propertyId = ReadNumber(propPos + _currentOffsetSize, _currentPropertyIdSize);
            token = (BlittableJsonToken)(*(propPos + _currentOffsetSize + _currentPropertyIdSize));
        }


        public struct PropertyDetails
        {
            public LazyStringValue Name;
            public object Value;
            public BlittableJsonToken Token;
        }

        public void GetPropertyByIndex(int index, ref PropertyDetails prop, bool addObjectToCache = false)
        {
            AssertContextNotDisposed();

            if (_mem == null)
                ThrowObjectDisposed();

            if (index < 0 || index >= _propCount)
                ThrowOutOfRangeException();

            var metadataSize = _currentOffsetSize + _currentPropertyIdSize + sizeof(byte);

            GetPropertyTypeAndPosition(index, metadataSize,
                out var token,
                out var position,
                out var propertyId);

            var stringValue = GetPropertyName(propertyId);

            prop.Token = token;
            prop.Name = stringValue;
            if (_objectsPathCacheByIndex != null && _objectsPathCacheByIndex.TryGetValue(index, out var result))
            {
                prop.Value = result;
                return;
            }

            var value = GetObject(token, (int)(_objStart - _mem - position));

            if (NoCache == false && addObjectToCache)
            {
                AddToCache(stringValue.ToString(), value, index);

            }

            prop.Value = value;
        }

        private static void ThrowOutOfRangeException()
        {
            // ReSharper disable once NotResolvedInText
            throw new ArgumentOutOfRangeException("index");
        }

        public int GetPropertyIndex(string name)
        {
            AssertContextNotDisposed();

            return GetPropertyIndex(new StringSegment(name));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetPropertyIndex(StringSegment name, bool ignoreCase = false)
        {
            AssertContextNotDisposed();

            var comparer = _context.GetLazyStringForFieldWithCaching(name);
            return GetPropertyIndex(comparer, ignoreCase);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetPropertyIndex(LazyStringValue comparer, bool ignoreCase = false)
        {
            AssertContextNotDisposed();

            if (_propCount == 0)
                goto NotFound;

            int min = 0, max = _propCount - 1;

            long currentOffsetSize = _currentOffsetSize;
            long currentPropertyIdSize = _currentPropertyIdSize;
            long metadataSize = currentOffsetSize + currentPropertyIdSize + sizeof(byte);
            byte* metadataPtr = _metadataPtr;

            int mid = (min + max) / 2;
            if (mid > max)
                mid = max;

            do
            {
                var propertyIntPtr = metadataPtr + (mid) * metadataSize;

                var propertyId = ReadNumber(propertyIntPtr + currentOffsetSize, currentPropertyIdSize);

                var cmpResult = ComparePropertyName(propertyId, comparer, ignoreCase);
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

        NotFound:
            return -1;
        }

        /// <summary>
        /// Compares property names between received StringToByteComparer and the string stored in the document's property names storage
        /// </summary>
        /// <param name="propertyId">Position of the string in the property ids storage</param>
        /// <param name="comparer">Comparer of a specific string value</param>
        /// <param name="ignoreCase">Indicates if the comparassion should be case insensitive</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int ComparePropertyName(int propertyId, LazyStringValue comparer, bool ignoreCase = false)
        {
            AssertContextNotDisposed();

            // Get the offset of the property name from the _proprNames position
            var propertyNameOffsetPtr = _propNames + 1 + propertyId * _propNamesDataOffsetSize;
            var propertyNameOffset = ReadNumber(propertyNameOffsetPtr, _propNamesDataOffsetSize);

            // Get the relative "In Document" position of the property Name
            var propertyNameRelativePosition = _propNames - propertyNameOffset;
            var position = propertyNameRelativePosition - _mem;

            // Get the property name size
            var size = ReadVariableSizeInt((int)position, out byte propertyNameLengthDataLength);

            if (ignoreCase)
            {
                return CompareCaseInsensitive(comparer, propertyNameRelativePosition + propertyNameLengthDataLength, size);
            }

            // Return result of comparison between property name and received comparer
            return comparer.Compare(propertyNameRelativePosition + propertyNameLengthDataLength, size);

        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int CompareCaseInsensitive(LazyStringValue lsv, byte* p, int pSize)
        {
            char[] buffer = null;

            try
            {
                if (lsv.HasStringValue == false)
                {
                    buffer = ArrayPool<char>.Shared.Rent(lsv.Size + pSize);
                    fixed (char* chars = buffer)
                    {
                        System.Text.Encoding.UTF8.GetChars(lsv.Buffer, lsv.Size, chars, lsv.Size);
                        var pCount = System.Text.Encoding.UTF8.GetChars(p, pSize, chars + lsv.Length, pSize);
                        var span = new ReadOnlySpan<char>(chars, pCount + lsv.Length);

                        return span.Slice(0, lsv.Length).CompareTo(span.Slice(lsv.Length), StringComparison.OrdinalIgnoreCase);
                    }
                }

                buffer = ArrayPool<char>.Shared.Rent(pSize);
                fixed (char* pChars = buffer)
                {
                    var pCount = System.Text.Encoding.UTF8.GetChars(p, pSize, pChars, pSize);
                    var pSpan = new ReadOnlySpan<char>(pChars, pCount);

                    var lsvSpan = lsv.ToString().AsSpan();

                    return lsvSpan.CompareTo(pSpan, StringComparison.OrdinalIgnoreCase);
                }
            }
            finally
            {
                if (buffer != null)
                {
                    ArrayPool<char>.Shared.Return(buffer);
                }
            }
        }

        public struct InsertionOrderProperties : IDisposable
        {
            internal int[] PropertiesBuffer;
            internal int[] Offsets;

            public int Used;

            public ArraySegment<int> Properties;

            public InsertionOrderProperties(int size)
            {
                var actual = Bits.NextPowerOf2(size);
                PropertiesBuffer = ArrayPool<int>.Shared.Rent(actual);
                Offsets = ArrayPool<int>.Shared.Rent(actual);
                Used = size;
                Properties = new ArraySegment<int>(PropertiesBuffer, 0, Used);
            }

            public void Dispose()
            {
                if(PropertiesBuffer != null)
                {
                    ArrayPool<int>.Shared.Return(PropertiesBuffer);
                    PropertiesBuffer = null;
                }
                if (Offsets != null)
                {
                    ArrayPool<int>.Shared.Return(Offsets);
                    Offsets = null;
                }
                Properties = default;
                Used = 0;
            }
        }

        public InsertionOrderProperties GetPropertiesByInsertionOrder()
        {
            AssertContextNotDisposed();

            if (_metadataPtr == null)
                ThrowObjectDisposed();

            var buffers = new InsertionOrderProperties(_propCount);

            var metadataSize = _currentOffsetSize + _currentPropertyIdSize + sizeof(byte);
            for (int i = 0; i < _propCount; i++)
            {
                var propertyIntPtr = _metadataPtr + i * metadataSize;
                buffers.Offsets[i] = ReadNumber(propertyIntPtr, _currentOffsetSize);
                buffers.PropertiesBuffer[i] = i;
            }

            Sorter<int, int, NumericDescendingComparer> sorter;
            sorter.Sort(buffers.Offsets, buffers.PropertiesBuffer, 0, _propCount);
            buffers.Used = _propCount;

            return buffers;
        }

        public ulong GetHashOfPropertyNames()
        {
            AssertContextNotDisposed();

            ulong hash = (ulong)_propCount;
            for (int i = 0; i < _propCount; i++)
            {
                var propertyNameOffsetPtr = _propNames + sizeof(byte) + i * _propNamesDataOffsetSize;
                var propertyNameOffset = ReadNumber(propertyNameOffsetPtr, _propNamesDataOffsetSize);

                // Get the relative "In Document" position of the property Name
                var propRelativePos = (int)(_propNames - propertyNameOffset - _mem);
                var size = ReadVariableSizeInt(propRelativePos, out var offset);

                hash = Hashing.XXHash64.Calculate(_mem + propRelativePos + offset, (ulong)size, hash);
            }
            return hash;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal object GetObject(BlittableJsonToken type, int position)
        {
            AssertContextNotDisposed();

            BlittableJsonToken actualType = type & TypesMask;
            if (actualType == BlittableJsonToken.String)
                return ReadStringLazily(position);
            if (actualType == BlittableJsonToken.Integer)
                return ReadVariableSizeLong(position);
            if (actualType == BlittableJsonToken.StartObject)
                return new BlittableJsonReaderObject(position, _parent ?? this, type) { NoCache = NoCache };

            return GetObjectUnlikely(type, position, actualType);
        }

        private object GetObjectUnlikely(BlittableJsonToken type, int position, BlittableJsonToken actualType)
        {
            AssertContextNotDisposed();

            switch (actualType)
            {
                case BlittableJsonToken.EmbeddedBlittable:
                    return ReadNestedObject(position);
                case BlittableJsonToken.RawBlob:
                    return ReadRawBlob(position);
                case BlittableJsonToken.StartArray:
                    return new BlittableJsonReaderArray(position, _parent ?? this, type)
                    {
                        NoCache = NoCache
                    };
                case BlittableJsonToken.CompressedString:
                    return ReadCompressStringLazily(position);
                case BlittableJsonToken.Boolean:
                    return ReadNumber(_mem + position, 1) == 1;
                case BlittableJsonToken.Null:
                    return null;
                case BlittableJsonToken.LazyNumber:
                    return new LazyNumberValue(ReadStringLazily(position));
            }

            throw new ArgumentOutOfRangeException((type).ToString());
        }

        public class RawBlob
        {
            public byte* Ptr;
            public int Length;
        }

        private RawBlob ReadRawBlob(int pos)
        {
            var size = ReadVariableSizeInt(pos, out byte offset);
            return new RawBlob
            {
                Ptr = _mem + pos + offset,
                Length = size
            };
        }

        public void Dispose()
        {
            AssertContextNotDisposed();

            if (_mem == null) //double dispose will do nothing
                return;
            if (_allocatedMemory != null && _buffer.IsDisposed == false)
            {
                _context.ReturnMemory(_allocatedMemory);
                _allocatedMemory = null;
            }

            _mem = null;
            _metadataPtr = null;
            _objStart = null;

            if (_objectsPathCache != null)
            {
                foreach (var property in _objectsPathCache)
                {
                    var disposable = property.Value as IDisposable;
                    disposable?.Dispose();
                }

                _context.ReleasePathCache(_objectsPathCache, _objectsPathCacheByIndex);
            }

            _buffer.Dispose();
        }

        public void CopyTo(byte* ptr)
        {
            AssertContextNotDisposed();

            if (_parent != null)
                InvalidAttemptToCopyNestedObject();
            Memory.Copy(ptr, _mem, _size);
        }

        private static void InvalidAttemptToCopyNestedObject()
        {
            throw new InvalidOperationException(
                "Attempted to copy a nested object. This will actually copy the whole object, which is probably not what you wanted.");
        }

        public BlittableJsonReaderObject CloneOnTheSameContext()
        {
            return Clone(_context);
        }

        public BlittableJsonReaderObject Clone(JsonOperationContext context)
        {
            AssertContextNotDisposed();

            if (_parent != null)
                return context.ReadObject(this, "cloning nested obj");

            var mem = context.GetMemory(Size);

            CopyTo(mem.Address);
            var cloned = new BlittableJsonReaderObject(mem.Address, Size, context)
            {
                _allocatedMemory = mem
            };
            if (Modifications != null)
            {
                cloned.Modifications = new DynamicJsonValue(cloned);
                foreach (var property in Modifications.Properties)
                {
                    cloned.Modifications.Properties.Add(property);
                }
            }

            return cloned;
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

        private int PropertiesNamesValidation(int numberOfProps, int propsOffsetList, int propsNamesOffsetSize, int currentSize)
        {
            AssertContextNotDisposed();

            var blittableSize = currentSize;
            var offsetCounter = 0;
            for (var i = numberOfProps - 1; i >= 0; i--)
            {
                int stringLength;
                var nameOffset = 0;
                nameOffset = ReadNumber((_mem + propsOffsetList + 1 + i * propsNamesOffsetSize),
                    propsNamesOffsetSize);
                if ((blittableSize < nameOffset) || (nameOffset < 0))
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
            AssertContextNotDisposed();

            byte lenOffset;
            byte escOffset;
            int stringLength;
            stringLength = ReadVariableSizeInt(stringOffset, out lenOffset);
            if (stringLength < 0)
                throw new InvalidDataException("String not valid");
            var str = stringOffset + lenOffset;
            var totalEscCharLen = 0;
            var escCount = ReadVariableSizeInt(stringOffset + lenOffset + stringLength, out escOffset);
            if (escCount != 0)
            {
                var prevEscCharOffset = 0;
                for (var i = 0; i < escCount; i++)
                {
                    var escCharOffset = ReadVariableSizeInt(str + stringLength + escOffset + totalEscCharLen, out var escCharOffsetLen);
                    escCharOffset += prevEscCharOffset;
                    var escChar = (char)ReadNumber(_mem + str + escCharOffset, 1);
                    switch (escChar)
                    {
                        case '\\':
                        case '/':
                        case '"':
                        case '\b':
                        case '\f':
                        case '\n':
                        case '\r':
                        case '\t':
                            break;
                        default:
                            if (escChar >= 32)
                                throw new InvalidDataException("String not valid, invalid escape character: " + escChar);
                            break;
                    }
                    totalEscCharLen += escCharOffsetLen;
                    prevEscCharOffset = escCharOffset + 1;
                }
            }
            return stringLength + escOffset + totalEscCharLen + lenOffset;
        }

        private BlittableJsonToken TokenValidation(byte tokenStart, out int propOffsetSize,
            out int propIdSize)
        {
            AssertContextNotDisposed();

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
            AssertContextNotDisposed();

            byte offset;
            var numberOfProperties = ReadVariableSizeInt(_mem + objStartOffset, 0, out offset);
            var current = objStartOffset + offset;

            if (numberOfProperties < 0)
                ThrowInvalidNumbeOfProperties();

            for (var i = 1; i <= numberOfProperties; i++)
            {
                var propOffset = ReadNumber(_mem + current, mainPropOffsetSize);
                if ((propOffset > objStartOffset) || (propOffset < 0))
                    ThrowInvalidPropertiesOffest();
                current += mainPropOffsetSize;

                if (rootTokenTypen == BlittableJsonToken.StartObject)
                {
                    var id = ReadNumber(_mem + current, mainPropIdSize);
                    if ((id > numberOfPropsNames) || (id < 0))
                        ThrowInvalidPropertiesId();
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
                    case BlittableJsonToken.LazyNumber:
                        var numberLength = ReadVariableSizeInt(propValueOffset, out byte lengthOffset);
                        var escCount = ReadVariableSizeInt(propValueOffset + lengthOffset + numberLength, out byte escOffset);

                        // if number has any non-ascii symbols, we rull it out immediately
                        if (escCount > 0)
                            ThrowInvalidNumber(propValueOffset);

                        var numberCharsStart = _mem + objStartOffset - propOffset + lengthOffset;

                        // try and validate number using double's validation
                        if (Utf8Parser.TryParse(new ReadOnlySpan<byte>(numberCharsStart, numberLength), out double _, out var consumed) == false ||
                            consumed != numberLength)
                            ThrowInvalidNumber(propValueOffset);
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
                            ThrowInvalidCompressedString();
                        break;
                    case BlittableJsonToken.Boolean:
                        var boolProp = ReadNumber(_mem + propValueOffset, 1);
                        if ((boolProp != 0) && (boolProp != 1))
                            ThrowInvalidBool();
                        break;
                    case BlittableJsonToken.Null:
                        if (ReadNumber(_mem + propValueOffset, 1) != 0)
                            ThrowInvalidNull();
                        break;
                    case BlittableJsonToken.EmbeddedBlittable:
                        byte offsetLen;
                        stringLength = ReadVariableSizeInt(propValueOffset, out offsetLen);
                        var blittableJsonReaderObject = new BlittableJsonReaderObject(_mem + propValueOffset + offsetLen, stringLength, _context);
                        blittableJsonReaderObject.BlittableValidation();
                        break;
                    case BlittableJsonToken.RawBlob:
                        ReadVariableSizeInt(propValueOffset, out _);
                        break;
                    default:
                        ThrowInvalidTokenType();
                        break;
                }
            }
            return current;
        }

        public void AddItemsToStream<T>(ManualBlittableJsonDocumentBuilder<T> writer)
            where T : struct, IUnmanagedWriteBuffer
        {
            AssertContextNotDisposed();

            for (var i = 0; i < Count; i++)
            {
                var prop = new PropertyDetails();
                GetPropertyByIndex(i, ref prop);
                writer.WritePropertyName(prop.Name);
                writer.WriteValue(ProcessTokenTypeFlags(prop.Token), prop.Value);
            }
        }

        private static void ThrowInvalidTokenType()
        {
            throw new InvalidDataException("Token type not valid");
        }

        private static void ThrowInvalidNull()
        {
            throw new InvalidDataException("Null not valid");
        }

        private static void ThrowInvalidBool()
        {
            throw new InvalidDataException("Bool not valid");
        }

        private static void ThrowInvalidCompressedString()
        {
            throw new InvalidDataException("Compressed string not valid");
        }

        private void ThrowInvalidNumber(int numberPosition)
        {
            throw new InvalidDataException("Number not valid (" + ReadStringLazily(numberPosition).ToString() + ")");
        }

        private static void ThrowInvalidPropertiesId()
        {
            throw new InvalidDataException("Properties id not valid");
        }

        private static void ThrowInvalidPropertiesOffest()
        {
            throw new InvalidDataException("Properties offset not valid");
        }

        private static void ThrowInvalidNumbeOfProperties()
        {
            throw new InvalidDataException("Number of properties not valid");
        }

        public override bool Equals(object obj)
        {
            AssertContextNotDisposed();
            if (ReferenceEquals(null, obj))
                return false;

            if (ReferenceEquals(this, obj))
                return true;

            if (obj is BlittableJsonReaderObject blittableJson)
                return Equals(blittableJson);

            return false;
        }

        public bool Equals(BlittableJsonReaderObject other, bool ignoreRavenProperties = false)
        {
            AssertContextNotDisposed();
            if (_propCount != other._propCount)
                return false;

            if (_isRoot && other._isRoot)
            {
                if (_size == other.Size && Memory.CompareInline(_mem, other._mem, _size) == 0)
                    return true;
            }

            var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof(byte));

            for (var i = 0; i < _propCount; i++)
            {
                GetPropertyTypeAndPosition(i, metadataSize, out var token, out var position, out var id);

                var propertyName = GetPropertyName(id);
                if(ignoreRavenProperties && propertyName.StartsWith('@'))
                    continue;

                var otherId = other.GetPropertyIndex(propertyName);

                if (otherId == -1)
                    return false;

                if (other.TryGetObjectByIndex(otherId, token, out var result) == false) 
                    return false;

                var thisId = GetPropertyIndex(propertyName);

                TryGetObjectByIndex(thisId, token, out var current);

                if (current == null && result == null)
                    continue;

                if ((current?.Equals(result) ?? false) == false)
                    return false;
            }

            return true;
        }

        public override int GetHashCode()
        {
            AssertContextNotDisposed();
            return _propCount;
        }

        [Conditional("DEBUG")]
        public static void AssertNoModifications(BlittableJsonReaderObject data, string id, bool assertChildren, bool assertRemovals = true, bool assertProperties = true)
        {
            data.AssertContextNotDisposed();            

            if (assertRemovals == false && assertProperties == false)
                throw new InvalidOperationException($"Both {nameof(assertRemovals)} and {nameof(assertProperties)} cannot be set to false.");

            if (data == null)
                return;

            data.NoCache = true;

            if (assertRemovals && data.Modifications?.Removals?.Count > 0 && data.Modifications.SourceIndex < data.Count)
                throw new InvalidOperationException($"Modifications (removals) detected in '{id}'. JSON: {data}");

            if (assertProperties && data.Modifications?.Properties.Count > 0 && data.Modifications.Properties.Count > data.Modifications.ModificationsIndex)
                throw new InvalidOperationException($"Modifications (properties) detected in '{id}'. JSON: {data}");

            if (assertChildren == false)
                return;

            foreach (var propertyName in data.GetPropertyNames())
            {
                var property = data[propertyName];
                var inner = property as BlittableJsonReaderObject;
                if (inner != null)
                {
                    AssertNoModifications(inner, id, assertChildren: true);
                    continue;
                }

                var innerArray = property as BlittableJsonReaderArray;
                if (innerArray == null)
                    continue;

                foreach (var item in innerArray)
                {
                    var innerItem = item as BlittableJsonReaderObject;
                    if (innerItem == null)
                        continue;

                    AssertNoModifications(innerItem, id, assertChildren: true);
                }
            }
        }

        public bool Contains(LazyStringValue propertyName)
        {
            AssertContextNotDisposed();

            var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof(byte));

            for (int i = 0; i < _propCount; i++)
            {
                GetPropertyTypeAndPosition(i, metadataSize, out BlittableJsonToken token, out int position, out int id);

                if (propertyName == GetPropertyName(id))
                    return true;
            }

            return false;
        }
    }
}
