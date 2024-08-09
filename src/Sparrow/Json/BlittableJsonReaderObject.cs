using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Compression;
using Sparrow.Json.Parsing;
using static Sparrow.DisposableExceptions;
using static Sparrow.PortableExceptions;
using static System.Buffers.Text.Utf8Parser;

namespace Sparrow.Json
{
    public sealed unsafe class BlittableJsonReaderObject : BlittableJsonReaderBase, IDisposable
    {
        private static readonly object BoxedTrue = true;
        private static readonly object BoxedFalse = false;

        private AllocatedMemoryData _allocatedMemory;
        private UnmanagedWriteBuffer _buffer;
        private byte* _metadataPtr;
        private readonly int _size;
        private readonly int _propCount;
        private readonly long _currentOffsetSize;
        private readonly long _currentPropertyIdSize;
        private readonly bool _isRoot;
        private readonly byte* _objStart;
        private readonly byte* _propNames;
        private readonly int _propNamesDataOffsetSize;

        public DynamicJsonValue Modifications;

        private Dictionary<StringSegment, object> _objectsPathCache;
        private Dictionary<int, object> _objectsPathCacheByIndex;

        public override string ToString()
        {
            if (_mem == null)
                return "Disposed";

            ThrowIfDisposedOnDebug(this);

            using (var memoryStream = new MemoryStream())
            {
                WriteJsonToAsync(memoryStream).AsTask().ConfigureAwait(false).GetAwaiter().GetResult();
                memoryStream.Position = 0;

                return new StreamReader(memoryStream).ReadToEnd();
            }
        }

        public ValueTask WriteJsonToAsync(Stream stream, CancellationToken token = default)
        {
            ThrowIfDisposedOnDebug(this);

            return _context.WriteAsync(stream, this, token);
        }
        
        public BlittableJsonReaderObject(byte* mem, int size, JsonOperationContext context, UnmanagedWriteBuffer buffer = default(UnmanagedWriteBuffer))
            : this(mem, size, context)
        {
            _buffer = buffer;
        }

        private BlittableJsonReaderObject(byte* mem, int size, JsonOperationContext context, AllocatedMemoryData allocatedMemory)
            : this(mem, size, context)
        {
            _allocatedMemory = allocatedMemory;
        }

        private BlittableJsonReaderObject(byte* mem, int size, JsonOperationContext context)
            : base(context)
        {
            //otherwise SetupPropertiesAccess will throw because of the memory garbage
            //(or won't throw, but this is actually worse!)
            ThrowIf<ArgumentException>(size == 0, $"{nameof(BlittableJsonReaderObject)} does not support objects with zero size");

            _isRoot = true;
            _mem = mem; // get beginning of memory pointer
            _size = size; // get document size

            NoCache = NoCache;

            var propOffsetStart = _size - 2;
            var propsOffset = ReadVariableSizeIntInReverse(_mem, propOffsetStart, out byte offset);
            if (propsOffset >= size)
                ThrowInvalidPropertiesOffset();

            // init document level properties
            _propNames = (mem + propsOffset);
            _propNamesDataOffsetSize = GetPropertyNamesDataOffsetSize((BlittableJsonToken)(*_propNames));
            if (_propNamesDataOffsetSize <= 0)
                throw new ArgumentException($"Illegal type {(BlittableJsonToken)(*_propNames)}");

            // get pointer to property names array on document level

            // init root level object properties
            var objStartOffset = ReadVariableSizeIntInReverse(_mem, propOffsetStart - offset, out offset);
            // get offset of beginning of data of the main object
            _propCount = ReadVariableSizeInt(objStartOffset, out var propCountOffset); // get main object properties count
            _objStart = objStartOffset + mem;
            _metadataPtr = _objStart + propCountOffset;
            // get pointer to current objects property tags metadata collection

            var currentType = (BlittableJsonToken)(*(mem + size - sizeof(byte)));
            // get current type byte flags

            // analyze main object type and it's offset and propertyIds flags
            _currentOffsetSize = ProcessTokenOffsetFlags(currentType);
            _currentPropertyIdSize = ProcessTokenPropertyFlags(currentType);
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

            _propNamesDataOffsetSize = GetPropertyNamesDataOffsetSize((BlittableJsonToken)(*_propNames));
            if (_propNamesDataOffsetSize <= 0)
                throw new ArgumentException($"Illegal type {(BlittableJsonToken)(*_propNames)}");

            _objStart = _mem + pos;
            _propCount = ReadVariableSizeInt(pos, out var propCountOffset);
            _metadataPtr = _objStart + propCountOffset;

            // analyze main object type and it's offset and propertyIds flags
            _currentOffsetSize = ProcessTokenOffsetFlags(type);
            _currentPropertyIdSize = ProcessTokenPropertyFlags(type);
        }

#if NET6_0_OR_GREATER
        [DoesNotReturn]
#endif
        private static void ThrowOutOfRangeException(BlittableJsonToken token)
        {
            throw new ArgumentOutOfRangeException(
                $"Property names offset flag should be either byte, short of int, instead of {token}");
        }

        public int Size => _size;

        public int Count => _propCount;

        public byte* BasePointer
        {
            get
            {
                ThrowIfDisposedOnDebug(this);

                if (_parent != null)
                    InvalidAttemptToCopyNestedObject();

                return _mem;
            }
        }

        public ulong DebugHash
        {
            get
            {
                ThrowIfDisposedOnDebug(this);

                return Hashing.XXHash64.Calculate(_mem, (ulong)_size);
            }
        }

        public long Location => (long)_objStart;

        public string[] GetSortedPropertyNames()
        {
            ThrowIfDisposedOnDebug(this);

            var propertyNames = new string[_propCount];

            var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof(byte));

            for (int i = 0; i < _propCount; i++)
            {
                GetPropertyTypeAndPosition(i, metadataSize, out _, out int _, out int id);
                propertyNames[i] = GetPropertyName(id);
            }

            return propertyNames;
        }

        /// <summary>
        /// Returns an array of property names, ordered in the order it was stored
        /// </summary>
        /// <returns></returns>
        public string[] GetPropertyNames()
        {
            ThrowIfDisposedOnDebug(this);

            var offsets = new int[_propCount];
            var propertyNames = new string[_propCount];

            var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof(byte));

            for (int i = 0; i < _propCount; i++)
            {
                GetPropertyTypeAndPosition(i, metadataSize, out _, out int position, out int id);
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
            ThrowIfDisposedOnDebug(this);

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
                if (TryGetMember(name, out object result)) 
                    return result;
                
                throw new ArgumentException($"Member named {name} does not exist");
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
                    if (type.IsEnum)
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
                        obj = (T)Convert.ChangeType(lazyStringValue.ToString(), type, CultureInfo.InvariantCulture);
                        // TODO: Try
                        // obj = (T)TypeDescriptor.GetConverter(typeof(T)).ConvertFromString(lazyStringValue.ToString());
                    }
                    else if (result is LazyNumberValue lnv)
                    {
                        obj = (T)Convert.ChangeType(lnv, type, CultureInfo.InvariantCulture);
                    }
                    else if (result is LazyCompressedStringValue lazyCompressStringValue)
                    {
                        if (type == typeof(LazyStringValue))
                            obj = (T)(object)lazyCompressStringValue.ToLazyStringValue();
                        else
                            obj = (T)Convert.ChangeType(lazyCompressStringValue.ToString(), type, CultureInfo.InvariantCulture);
                    }
                    else
                    {
                        obj = (T)Convert.ChangeType(result, type, CultureInfo.InvariantCulture);
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
                    if (type.IsEnum)
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
                    else if (type == typeof(char))
                    {
                        if (ChangeTypeToString(result, out string charString) == false)
                            ThrowFormatException(result, result.GetType().FullName, "string");
                        if (char.TryParse(charString, out char @char) == false)
                            ThrowFormatException(result, result.GetType().FullName, "char");
                        obj = (T)(object)@char;
                    }
#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
                    else if (type == typeof(DateOnly))
                    {
                        if (ChangeTypeToString(result, out string dateOnlyString) == false)
                            ThrowFormatException(result, result.GetType().FullName, "string");

                        DateOnly dateOnly;
                        fixed (char* dateOnlyStringPtr = dateOnlyString.AsSpan())
                        {
                            if (LazyStringParser.TryParseDateOnly(dateOnlyStringPtr, dateOnlyString.Length, out dateOnly) == false)
                                ThrowFormatException(result, result.GetType().FullName, "DateOnly");
                        }
                        obj = (T)(object)dateOnly;
                    }
                    else if (type == typeof(TimeOnly))
                    {
                        if (ChangeTypeToString(result, out string timeOnlyString) == false)
                            ThrowFormatException(result, result.GetType().FullName, "string");
                        
                        TimeOnly timeOnly;

                        fixed (char* timeOnlyStringPtr = timeOnlyString.AsSpan())
                        {
                            if (LazyStringParser.TryParseTimeOnly(timeOnlyStringPtr, timeOnlyString.Length, out timeOnly) == false)
                                ThrowFormatException(result, result.GetType().FullName, "TimeOnly");
                        }
                        obj = (T)(object)timeOnly;
                    }
#endif
                    else
                    {
                        switch (result)
                        {
                            case LazyStringValue lazyStringValue:
                                obj = (T)Convert.ChangeType(lazyStringValue.ToString(), type, CultureInfo.InvariantCulture);
                                break;

                            case LazyNumberValue lazyNumberValue:
                                obj = (T)Convert.ChangeType(lazyNumberValue, type, CultureInfo.InvariantCulture);
                                break;

                            case LazyCompressedStringValue lazyCompressStringValue:
                                if (type == typeof(LazyStringValue))
                                    obj = (T)(object)lazyCompressStringValue.ToLazyStringValue();
                                else
                                    obj = (T)Convert.ChangeType(lazyCompressStringValue.ToString(), type, CultureInfo.InvariantCulture);
                                break;

                            default:
                                obj = (T)Convert.ChangeType(result, type, CultureInfo.InvariantCulture);
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
        internal static bool ChangeTypeToString(object result, out string str)
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
            var metadataSize = _currentOffsetSize + _currentPropertyIdSize + sizeof(byte);

            GetPropertyTypeAndPosition(index, metadataSize, out var token, out var position, out _);
            if (CompareTokens(expectedToken, token) == false)
            {
                result = null;
                return false;
            }
            result = GetObject(token, (int)(_objStart - _mem - position), out _);
            return true;
        }

        private bool CompareTokens(BlittableJsonToken firstToken, BlittableJsonToken secondToken)
        {
            var firstClearedToken = (firstToken & TypesMask);
            var secondClearedToken = (secondToken & TypesMask);
            if (firstClearedToken == secondClearedToken)
                return true;

            return (firstClearedToken == BlittableJsonToken.EmbeddedBlittable && secondClearedToken == BlittableJsonToken.StartObject
                    || firstClearedToken == BlittableJsonToken.StartObject && secondClearedToken == BlittableJsonToken.EmbeddedBlittable);
        }

        public bool TryGetMember(StringSegment name, out object result)
        {
            ThrowIfDisposedOnDebug(this);

            if (_mem == null)
                goto ThrowDisposed;

            // try get value from cache, works only with Blittable types, other objects are not stored for now
            if (_objectsPathCache != null && _objectsPathCache.TryGetValue(name, out result))
                return true;

            var comparer = _context.GetLazyStringForFieldWithCaching(name);
            var index = GetPropertyIndex(comparer);
            if (index == -1)
            {
                Unsafe.SkipInit(out result);
                return false;
            }

            var metadataSize = _currentOffsetSize + _currentPropertyIdSize + sizeof(byte);

            GetPropertyTypeAndPosition(index, metadataSize, out BlittableJsonToken token, out int position, out _);
            result = GetObject(token, (int)(_objStart - _mem - position), out bool isBlittableJsonReader);

            if (NoCache == false && isBlittableJsonReader)
            {
                AddToCache(name, result, index);
            }

            return true;

            ThrowDisposed:
            Throw(this, "blittable object has been disposed");
            result = null;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddToCache(StringSegment name, object result, int index)
        {
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
            ThrowIfDisposedOnDebug(this);
            ThrowIf<ObjectDisposedException>(_mem == null, "blittable object has been disposed");

            if (index < 0 || index >= _propCount)
                throw new ArgumentOutOfRangeException(nameof(index), index, "Unexpected index argument value: " + index);

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

            var value = GetObject(token, (int)(_objStart - _mem - position), out _);

            if (NoCache == false && addObjectToCache)
            {
                AddToCache(stringValue.ToString(CultureInfo.InvariantCulture), value, index);
            }

            prop.Value = value;
        }

        public int GetPropertyIndex(string name)
        {
            ThrowIfDisposedOnDebug(this);

            var comparer = _context.GetLazyStringForFieldWithCaching(name);
            return GetPropertyIndex(comparer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetPropertyIndex(LazyStringValue comparer)
        {
            if (_propCount == 0)
                return -1;

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
        /// <param name="ignoreCase">Indicates if the comparison should be case-insensitive</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int ComparePropertyName(int propertyId, LazyStringValue comparer)
        {
            // Get the offset of the property name from the _propNames position
            var propertyNameOffsetPtr = _propNames + 1 + propertyId * _propNamesDataOffsetSize;
            var propertyNameOffset = ReadNumber(propertyNameOffsetPtr, _propNamesDataOffsetSize);

            // Get the relative "In Document" position of the property Name
            var propertyNameRelativePosition = _propNames - propertyNameOffset;

            // Get the property name size
            var size = VariableSizeEncoding.Read<int>(propertyNameRelativePosition, out var propertyNameLengthDataLength);

            // Return result of comparison between property name and received comparer
            return comparer.Compare(propertyNameRelativePosition + propertyNameLengthDataLength, size);
        }

        public struct InsertionOrderProperties : IDisposable
        {
            internal readonly int* Properties;
            internal readonly int* Offsets;
            internal readonly int Size;

            private readonly JsonOperationContext _context;
            private AllocatedMemoryData _allocation;

            public InsertionOrderProperties(JsonOperationContext context, int size)
            {
                _context = context;
                if (size == 0)
                {
                    Size = 0;
                    _allocation = null;
                    Offsets = Properties = null;
                    return;
                }
                _allocation = context.GetMemory(size * 2 * sizeof(int));
                Properties = (int*)_allocation.Address;
                Offsets = Properties + size;
                Size = size;
            }

            public void Dispose()
            {
                if (_allocation == null)
                    return;
                _context.ReturnMemory(_allocation);
                _allocation = null;
            }
        }

        public InsertionOrderProperties GetPropertiesByInsertionOrder()
        {
            ThrowIfDisposedOnDebug(this);
            ThrowIf<ObjectDisposedException>(_metadataPtr == null, "blittable object has been disposed");

            var buffers = new InsertionOrderProperties(_context, _propCount);
            if (_propCount == 0)
                return buffers;

            var metadataSize = _currentOffsetSize + _currentPropertyIdSize + sizeof(byte);
            for (int i = 0; i < _propCount; i++)
            {
                var propertyIntPtr = _metadataPtr + i * metadataSize;
                buffers.Offsets[i] = ReadNumber(propertyIntPtr, _currentOffsetSize);
                buffers.Properties[i] = i;
            }

            Sorter<int, int, NumericDescendingComparer> sorter;
            sorter.Sort(new Span<int>(buffers.Offsets, buffers.Size), new Span<int>(buffers.Properties, buffers.Size), 0, _propCount);

            return buffers;
        }

        public ulong GetHashOfPropertyNames()
        {
            ThrowIfDisposedOnDebug(this);
            var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof(byte));

            ulong hash = (ulong)_propCount;
            for (int i = 0; i < _propCount; i++)
            {
                GetPropertyTypeAndPosition(i, metadataSize, out _, out int _, out int id);
                var prop = GetPropertyName(id);
                hash = Hashing.XXHash64.Calculate(prop.Buffer, (ulong)prop.Size, hash);
            }
            return hash;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal object GetObject(BlittableJsonToken type, int position, out bool isBlittableJsonReader)
        {
            isBlittableJsonReader = false;
            BlittableJsonToken actualType = type & TypesMask;

            switch (actualType)
            {
                case BlittableJsonToken.String:
                    return ReadStringLazily(position);

                case BlittableJsonToken.Integer:
                    return ReadVariableSizeLong(position);

                case BlittableJsonToken.StartObject:
                    isBlittableJsonReader = true;
                    return new BlittableJsonReaderObject(position, _parent ?? this, type) { NoCache = NoCache };

                case BlittableJsonToken.EmbeddedBlittable:
                    isBlittableJsonReader = true;
                    return ReadNestedObject(position);

                case BlittableJsonToken.RawBlob:
                    return ReadRawBlob(position);

                case BlittableJsonToken.StartArray:
                    isBlittableJsonReader = true;
                    return new BlittableJsonReaderArray(position, _parent ?? this, type)
                    {
                        NoCache = NoCache
                    };

                case BlittableJsonToken.CompressedString:
                    return ReadCompressStringLazily(position);

                case BlittableJsonToken.Boolean:
                    return ReadNumber(_mem + position, sizeof(byte)) == 1 ? BoxedTrue : BoxedFalse;

                case BlittableJsonToken.Null:
                    return null;

                case BlittableJsonToken.LazyNumber:
                    isBlittableJsonReader = false;
                    return new LazyNumberValue(ReadStringLazily(position));
            }

            throw new ArgumentOutOfRangeException(nameof(type), type.ToString(), "Unexpected type: " + type);
        }


        internal sealed class RawBlob
        {
            public RawBlob()
            {
            }

            public RawBlob(byte* ptr, int length)
            {
                Address = ptr;
                Length = length;
            }

            public byte* Address;
            public int Length;
        }

        private RawBlob ReadRawBlob(int pos)
        {
            var size = ReadVariableSizeInt(pos, out var offset);
            return new RawBlob(_mem + pos + offset, size);
        }

        public void Dispose()
        {
            if (_mem == null) //double dispose will do nothing
                return;

            ThrowIfDisposedOnDebug(this);

            if (_allocatedMemory != null)
            {
                _context.ReturnMemory(_allocatedMemory);
                _allocatedMemory = null;
            }

            _mem = null;
            _metadataPtr = null;
            Modifications = null;

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
            ThrowIfDisposedOnDebug(this);

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
            ThrowIfDisposedOnDebug(this);

            if (_parent != null)
                return context.ReadObject(this, "cloning nested obj");

            if (Modifications == null)
            {
                var mem = context.GetMemory(Size);
                CopyTo(mem.Address);
                return new BlittableJsonReaderObject(mem.Address, Size, context, mem);
            }

            return context.ReadObject(this, null);
        }

        public BlittableJsonReaderObject CloneForConcurrentRead(JsonOperationContext externalContext)
        {
            // when we read a blittable we do also some allocations and use context's path cache (e.g. InsertionOrderProperties, ReadStringLazily, GetPropertyByIndex)
            // we cannot use internal BlittableJsonReaderBase._context for that purpose since it must not be used concurrently
            // we have to provide external context that will be used for that purpose during the read action
            // note that we don't copy the blittable data but still read from the original pointer

            ThrowIfDisposedOnDebug(externalContext);
            
            return new BlittableJsonReaderObject(_mem, _size, externalContext)
            {
                NoCache = NoCache
            };
        }

        public void BlittableValidation()
        {
            ThrowIfDisposedOnDebug(this);

            var currentSize = Size - 1;
            if (currentSize < 1)
                throw new InvalidDataException("Illegal data");
            
            var rootToken = TokenValidation(*(_mem + currentSize), out int rootPropOffsetSize, out int rootPropIdSize);
            if (rootToken != BlittableJsonToken.StartObject)
                throw new InvalidDataException("Illegal root object");
            currentSize--;

            var propsOffsetList = ReadVariableSizeIntInReverse(_mem, currentSize, out byte offset);
            if (offset > currentSize)
                throw new InvalidDataException("Properties names offset not valid");
            currentSize -= offset;

            var rootMetadataOffset = ReadVariableSizeIntInReverse(_mem, currentSize, out offset);
            if (offset > currentSize)
                throw new InvalidDataException("Root metadata offset not valid");
            currentSize -= offset;

            if ((propsOffsetList > currentSize) || (propsOffsetList <= 0))
                throw new InvalidDataException("Properties names offset not valid");

            var token = (BlittableJsonToken)(*(_mem + propsOffsetList));
            if ((token & (BlittableJsonToken)0xC0) != 0 || (TypesMask & token) != 0x00)
                throw new InvalidDataException("Properties names token not valid");

            int propNamesOffsetSize = ProcessTokenOffsetFlags(token);
            var numberOfProps = (currentSize - propsOffsetList) / propNamesOffsetSize;
            currentSize = PropertiesNamesValidation(numberOfProps, propsOffsetList, propNamesOffsetSize, propsOffsetList);

            if (rootMetadataOffset > currentSize || rootMetadataOffset < 0)
                throw new InvalidDataException("Root metadata offset not valid");
            
            var current = PropertiesValidation(rootToken, rootPropOffsetSize, rootPropIdSize, rootMetadataOffset, numberOfProps);
            if (current != currentSize)
                throw new InvalidDataException("Root metadata not valid");
        }

        private int PropertiesNamesValidation(int numberOfProps, int propsOffsetList, int propsNamesOffsetSize, int currentSize)
        {
            ThrowIfDisposedOnDebug(this);

            var blittableSize = currentSize;
            var offsetCounter = 0;
            for (var i = numberOfProps - 1; i >= 0; i--)
            {
                int nameOffset = ReadNumber((_mem + propsOffsetList + 1 + i * propsNamesOffsetSize), propsNamesOffsetSize);
                if (blittableSize < nameOffset || nameOffset < 0)
                    throw new InvalidDataException("Properties names offset not valid");
                
                int stringLength = StringValidation(propsOffsetList - nameOffset);
                if (offsetCounter + stringLength != nameOffset)
                    throw new InvalidDataException("Properties names offset not valid");
                
                offsetCounter = nameOffset;
                currentSize -= stringLength;
            }
            return currentSize;
        }

        private int StringValidation(int stringOffset)
        {
            ThrowIfDisposedOnDebug(this);

            int stringLength = ReadVariableSizeInt(stringOffset, out var lenOffset);
            if (stringLength < 0)
                throw new InvalidDataException("String not valid");

            var str = stringOffset + lenOffset;
            var totalEscCharLen = 0;
            var escCount = ReadVariableSizeInt(stringOffset + lenOffset + stringLength, out var escOffset);
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

        private BlittableJsonToken TokenValidation(byte tokenStart, out int propOffsetSize, out int propIdSize)
        {
            ThrowIfDisposedOnDebug(this);

            var token = (BlittableJsonToken)tokenStart;
            var tokenType = ProcessTokenTypeFlags(token);
            propOffsetSize = tokenType is BlittableJsonToken.StartObject or BlittableJsonToken.StartArray
                ? ProcessTokenOffsetFlags(token)
                : 0;

            propIdSize = tokenType == BlittableJsonToken.StartObject
                ? ProcessTokenPropertyFlags(token)
                : 0;
            return tokenType;
        }

        private int PropertiesValidation(BlittableJsonToken rootTokenType, int mainPropOffsetSize, int mainPropIdSize,
            int objStartOffset, int numberOfPropsNames)
        {
            var numberOfProperties = ReadVariableSizeInt(objStartOffset, out var offset);
            var current = objStartOffset + offset;

            if (numberOfProperties < 0)
                ThrowInvalidNumberOfProperties();

            for (var i = 1; i <= numberOfProperties; i++)
            {
                var propOffset = ReadNumber(_mem + current, mainPropOffsetSize);
                if (propOffset > objStartOffset || propOffset < 0)
                    ThrowInvalidPropertiesOffset();
                
                current += mainPropOffsetSize;

                if (rootTokenType == BlittableJsonToken.StartObject)
                {
                    var id = ReadNumber(_mem + current, mainPropIdSize);
                    if (id > numberOfPropsNames || id < 0)
                        ThrowInvalidPropertiesId();
                    current += mainPropIdSize;
                }

                var tokenType = TokenValidation(*(_mem + current), out int propOffsetSize, out int propIdSize);
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
                        var numberLength = ReadVariableSizeInt(propValueOffset, out var lengthOffset);
                        var escCount = ReadVariableSizeInt(propValueOffset + lengthOffset + numberLength, out _);

                        // if number has any non-ascii symbols, we rull it out immediately
                        if (escCount <= 0)
                        {
                            var numberCharsStart = _mem + objStartOffset - propOffset + lengthOffset;

                            // try and validate number using double's validation
                            if (TryParse(new ReadOnlySpan<byte>(numberCharsStart, numberLength), out double _, out var consumed) && consumed == numberLength)
                                break;
                        }
                        ThrowInvalidNumber(propValueOffset);
                        break;

                    case BlittableJsonToken.String:
                        StringValidation(propValueOffset);
                        break;

                    case BlittableJsonToken.CompressedString:
                        var stringLength = ReadVariableSizeInt(propValueOffset, out offset);
                        var compressedStringLength = ReadVariableSizeInt(propValueOffset + offset, out offset);
                        if (compressedStringLength > stringLength ||
                            compressedStringLength < 0 ||
                            stringLength < 0)
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
                        stringLength = ReadVariableSizeInt(propValueOffset, out var offsetLen);
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
            ThrowIfDisposedOnDebug(this);
            
            using (var insertionOrder = GetPropertiesByInsertionOrder())
            {
                var prop = new PropertyDetails();
                for (var i = 0; i < insertionOrder.Size; i++)
                {
                    var propIndex = insertionOrder.Properties[i];
                    GetPropertyByIndex(propIndex, ref prop);
                    writer.WritePropertyName(prop.Name);
                    writer.WriteValue(ProcessTokenTypeFlags(prop.Token), prop.Value);
                }
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
            throw new InvalidDataException($"Number not valid ({ReadStringLazily(numberPosition).ToString(CultureInfo.InvariantCulture)})");
        }

        private static void ThrowInvalidPropertiesId()
        {
            throw new InvalidDataException("Properties id not valid");
        }

        private static void ThrowInvalidPropertiesOffset()
        {
            throw new InvalidDataException("Properties offset not valid");
        }

        private static void ThrowInvalidNumberOfProperties()
        {
            throw new InvalidDataException("Number of properties not valid");
        }

        public override bool Equals(object obj)
        {
            ThrowIfDisposedOnDebug(this);
            
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
            ThrowIfDisposedOnDebug(this);
            
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
                if (ignoreRavenProperties && propertyName.StartsWith('@'))
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

        private int _hashCode;

        [SuppressMessage("ReSharper", "NonReadonlyMemberInGetHashCode")]
        public override int GetHashCode()
        {
            ThrowIfDisposedOnDebug(this);
            
            if (_hashCode == 0)
            {
                ulong hash = GetHashOfPropertyNames();
                PropertyDetails prop = default;
                for (int i = 0; i < _propCount; i++)
                {
                    GetPropertyByIndex(i, ref prop, false);
                    hash = Hashing.Combine(hash, (ulong)(prop.Value?.GetHashCode() ?? 0));
                }

                _hashCode = (int)Hashing.Mix(hash);
                if (_hashCode == 0)
                    _hashCode++;
            }
            return _hashCode;
        }

        [Conditional("DEBUG")]
        public static void AssertNoModifications(BlittableJsonReaderObject data, string id, bool assertChildren, bool assertRemovals = true, bool assertProperties = true)
        {
            if (assertRemovals == false && assertProperties == false)
                throw new InvalidOperationException($"Both {nameof(assertRemovals)} and {nameof(assertProperties)} cannot be set to false.");

            if (data == null)
                return;

            ThrowIfDisposedOnDebug(data);

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
                if (property is BlittableJsonReaderObject inner)
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
            ThrowIfDisposedOnDebug(this);

            var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof(byte));

            for (var i = 0; i < _propCount; i++)
            {
                GetPropertyTypeAndPosition(i, metadataSize, out _, out _, out var id);

                if (propertyName == GetPropertyName(id))
                    return true;
            }

            return false;
        }

        public ReadOnlySpan<byte> AsSpan() => new(BasePointer, Size);
    }
}
