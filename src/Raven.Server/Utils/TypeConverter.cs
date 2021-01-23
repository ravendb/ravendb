using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Object;
using Jint.Runtime.Interop;
using Lucene.Net.Documents;
using Raven.Client;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.JavaScript;
using Raven.Server.Documents.Patch;
using Raven.Server.Exceptions;
using Raven.Server.Extensions;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Utils
{
    internal static class TypeConverter
    {
        private const string TypePropertyName = "$type";

        private static readonly StringSegment ValuesPropertyName = new StringSegment("$values");

        private static readonly StringSegment ValuePropertyName = new StringSegment("$value");

        private static readonly ConcurrentDictionary<Type, IPropertyAccessor> PropertyAccessorCache = new ConcurrentDictionary<Type, IPropertyAccessor>();

        private static readonly ConcurrentDictionary<Type, IPropertyAccessor> PropertyAccessorForMapReduceOutputCache = new ConcurrentDictionary<Type, IPropertyAccessor>();

        public static bool IsSupportedType(object value)
        {
            if (value == null || value is DynamicNullObject)
                return true;

            if (value is DynamicBlittableJson)
                return true;

            if (value is string)
                return true;

            if (value is LazyStringValue || value is LazyCompressedStringValue)
                return true;

            if (value is bool)
                return true;

            if (value is int || value is long || value is double || value is decimal || value is float)
                return true;

            if (value is LazyNumberValue)
                return true;

            if (value is DateTime || value is DateTimeOffset || value is TimeSpan)
                return true;

            if (value is Guid)
                return true;

            if (value is Enum)
                return true;

            if (value is IEnumerable<IFieldable> || value is IFieldable)
                return true;

            if (value is DynamicJsonValue djv)
            {
                foreach (var item in djv.Properties)
                {
                    if (IsSupportedType(item) == false)
                        return false;
                }
                return true;
            }

            if (value is DynamicJsonArray dja)
            {
                foreach (var item in dja)
                {
                    if (IsSupportedType(item) == false)
                        return false;
                }
                return true;
            }

            if (value is IDictionary dictionary)
            {
                return IsSupportedType(dictionary.Values);
            }

            if (value is char[])
                return true;

            if (value is IEnumerable<char>)
                return true;

            if (value is byte[])
                return true;

            if (value is IDynamicJsonValueConvertible)
                return true;

            if (value is IEnumerable enumerable)
            {
                if (ShouldTreatAsEnumerable(enumerable))
                {
                    var isSupportedEnumerable = true;
                    var objectEnumerable = (IEnumerable)value;

                    foreach (var x in objectEnumerable)
                    {
                        isSupportedEnumerable &= IsSupportedType(x);
                    }

                    return isSupportedEnumerable;
                }
            }

            var accessor = GetPropertyAccessor(value);

            var isSupported = true;
            var hasProperties = false;
            foreach (var property in accessor.GetPropertiesInOrder(value))
            {
                hasProperties = true;

                if (isSupported == false)
                    return false;

                var propertyValue = property.Value;
                if (propertyValue is IEnumerable<object> propertyValueAsEnumerable && ShouldTreatAsEnumerable(propertyValue))
                {
                    var isSupportedInternal = true;
                    foreach (var x in propertyValueAsEnumerable)
                    {
                        isSupportedInternal &= IsSupportedType(x);
                    }
                    isSupported &= isSupportedInternal;
                    continue;
                }

                isSupported &= IsSupportedType(propertyValue);
            }

            return hasProperties & isSupported;
        }

        public static object ToBlittableSupportedType(object value, bool flattenArrays = false, bool forIndexing = false, Engine engine = null, JsonOperationContext context = null)
        {
            return ToBlittableSupportedType(value, value, flattenArrays, forIndexing, 0, engine, context);
        }

        private static object ToBlittableSupportedType(object root, object value, bool flattenArrays, bool forIndexing, int recursiveLevel, Engine engine, JsonOperationContext context)
        {
            if (recursiveLevel > MaxAllowedRecursiveLevelForType)
                NestingLevelTooDeep(root);

            if (value is JsValue js)
            {
                if (js.IsNull())
                {
                    if (forIndexing && js is DynamicJsNull dynamicJsNull)
                        return dynamicJsNull.IsExplicitNull ? DynamicNullObject.ExplicitNull : DynamicNullObject.Null;

                    return null;
                }
                if (js.IsUndefined())
                    return null;
                if (js.IsString())
                    return js.AsString();
                if (js.IsBoolean())
                    return js.AsBoolean();
                if (js.IsNumber())
                    return js.AsNumber();
                if (js.IsDate())
                    return js.AsDate().ToDateTime();
                //object wrapper is an object so it must come before the object
                if (js is ObjectWrapper ow)
                {
                    var target = ow.Target;
                    switch (target)
                    {
                        case LazyStringValue lsv:
                            return lsv;
                        case LazyCompressedStringValue lcsv:
                            return lcsv;
                        case LazyNumberValue lnv:
                            return lnv; //should be already blittable supported type.
                    }
                    ThrowInvalidObject(js);
                }
                //Array is an object in Jint
                else if (js.IsArray())
                {
                    var arr = js.AsArray();
                    var convertedArray = EnumerateArray(root, arr, flattenArrays, forIndexing, recursiveLevel + 1, engine, context);
                    return new DynamicJsonArray(flattenArrays ? Flatten(convertedArray) : convertedArray);
                }
                else if (js.IsObject())
                {
                    return JsBlittableBridge.Translate(context, engine, js.AsObject());
                }
                ThrowInvalidObject(js);
                return null;
            }

            if (value == null || value is DynamicNullObject)
                return null;

            if (value is DynamicBlittableJson dynamicDocument)
                return dynamicDocument.BlittableJson;

            if (value is string)
                return value;

            if (value is LazyStringValue || value is LazyCompressedStringValue)
                return value;

            if (value is bool)
                return value;

            if (value is int || value is long || value is double || value is decimal || value is float || value is short || value is byte)
                return value;

            if (value is LazyNumberValue)
                return value;

            if (value is DateTime || value is DateTimeOffset || value is TimeSpan)
                return value;

            if (value is Guid guid)
                return guid.ToString("D");

            if (value is Enum)
                return value.ToString();

            if (value is IEnumerable<IFieldable> || value is IFieldable)
                return "__ignored";

            if (value is IDictionary dictionary)
            {
                var @object = new DynamicJsonValue();

                foreach (var key in dictionary.Keys)
                {
                    var keyAsString = KeyAsString(key: ToBlittableSupportedType(root, key, flattenArrays, forIndexing, recursiveLevel: recursiveLevel + 1, engine: engine, context: context));
                    @object[keyAsString] = ToBlittableSupportedType(root, dictionary[key], flattenArrays, forIndexing, recursiveLevel: recursiveLevel + 1, engine: engine, context: context);
                }

                return @object;
            }

            if (value is IDictionary<object, object> dDictionary)
            {
                var @object = new DynamicJsonValue();

                foreach (var key in dDictionary.Keys)
                {
                    var keyAsString = KeyAsString(key: ToBlittableSupportedType(root, key, flattenArrays, forIndexing, recursiveLevel: recursiveLevel + 1, engine: engine, context: context));
                    @object[keyAsString] = ToBlittableSupportedType(root, dDictionary[key], flattenArrays, forIndexing, recursiveLevel: recursiveLevel + 1, engine: engine, context: context);
                }

                return @object;
            }

            if (value is char[] chars)
                return new string(chars);

            if (value is IEnumerable<char> charEnumerable)
                return new string(charEnumerable.ToArray());

            if (value is byte[] bytes)
                return System.Convert.ToBase64String(bytes);

            if (value is IEnumerable enumerable)
            {
                if (ShouldTreatAsEnumerable(enumerable))
                    return EnumerableToJsonArray(flattenArrays ? Flatten(enumerable) : enumerable, root, flattenArrays, forIndexing, recursiveLevel, engine, context);
            }

            var inner = new DynamicJsonValue();
            var accessor = GetPropertyAccessor(value);

            foreach (var property in accessor.GetPropertiesInOrder(value))
            {
                var propertyValue = property.Value;
                if (propertyValue is IEnumerable<object> propertyValueAsEnumerable && ShouldTreatAsEnumerable(propertyValue))
                {
                    inner[property.Key] = EnumerableToJsonArray(propertyValueAsEnumerable, root, flattenArrays, forIndexing, recursiveLevel, engine, context);
                    continue;
                }

                inner[property.Key] = ToBlittableSupportedType(root, propertyValue, flattenArrays, forIndexing, recursiveLevel + 1, engine, context);
            }

            return inner;
        }

        public static string KeyAsString(object key)
        {
            string kvpKeyAsString;
            switch (key)
            {
                case null:
                    kvpKeyAsString = Constants.Documents.Indexing.Fields.NullValue;
                    break;
                case LazyStringValue lsv:
                    kvpKeyAsString = lsv.Size == 0 ? Constants.Documents.Indexing.Fields.EmptyString : lsv;
                    break;
                case LazyCompressedStringValue lcsv:
                    kvpKeyAsString = lcsv.ToLazyStringValue();
                    break;
                case DateTime dateTime:
                    kvpKeyAsString = dateTime.GetDefaultRavenFormat();
                    break;
                case DateTimeOffset dateTimeOffset:
                    kvpKeyAsString = dateTimeOffset.UtcDateTime.GetDefaultRavenFormat(isUtc: true);
                    break;
                case TimeSpan timeSpan:
                    kvpKeyAsString = timeSpan.ToString("c", CultureInfo.InvariantCulture);
                    break;
                default:
                    kvpKeyAsString = key.ToString();
                    break;
            }

            return kvpKeyAsString;
        }

        private static void ThrowInvalidObject(JsValue jsValue)
        {
            throw new InvalidOperationException("Invalid type " + jsValue);
        }

        private static IEnumerable<object> EnumerateArray(object root, ArrayInstance arr, bool flattenArrays, bool forIndexing, int recursiveLevel, Engine engine, JsonOperationContext context)
        {
            foreach (var (key, val) in arr.GetOwnPropertiesWithoutLength())
            {
                yield return ToBlittableSupportedType(root, val.Value, flattenArrays, forIndexing, recursiveLevel, engine, context);
            }
        }

        private static DynamicJsonArray EnumerableToJsonArray(IEnumerable propertyEnumerable, object root, bool flattenArrays, bool forIndexing, int recursiveLevel, Engine engine, JsonOperationContext context)
        {
            var dja = new DynamicJsonArray();

            foreach (var x in propertyEnumerable)
            {
                dja.Add(ToBlittableSupportedType(root, x, flattenArrays, forIndexing, recursiveLevel + 1, engine, context));
            }

            return dja;
        }

        private static void NestingLevelTooDeep(object value)
        {
            throw new SerializationNestedLevelTooDeepException(
                                $"Reached nesting level of {MaxAllowedRecursiveLevelForType} for type {value.GetType().Name}, recursive types that exceed the allowed nesting level are not supported.");
        }

        public static int MaxAllowedRecursiveLevelForType { get; } = 100;

        internal static IEnumerable<object> Flatten(IEnumerable items)
        {
            foreach (var item in items)
            {
                if (item is IEnumerable enumerable && ShouldTreatAsEnumerable(enumerable))
                {
                    foreach (var nestedItem in Flatten(enumerable))
                    {
                        yield return nestedItem;
                    }

                    continue;
                }

                yield return item;
            }
        }

        public static dynamic ToDynamicType(object value)
        {
            if (value == null)
                return DynamicNullObject.ExplicitNull;

            if (value is DynamicNullObject)
                return value;

            if (value is IDynamicMetaObjectProvider)
                return value;

            if (value is int || value is long || value is double || value is decimal || value is float || value is short || value is byte)
                return value;

            if (value is string s && TryConvertStringValue(s, out object result))
                return result;

            LazyStringValue lazyString = null;
            if (value is LazyCompressedStringValue lazyCompressedStringValue)
                lazyString = lazyCompressedStringValue.ToLazyStringValue();
            else if (value is LazyStringValue lsv)
                lazyString = lsv;

            if (lazyString != null)
                return ConvertLazyStringValue(lazyString);

            if (value is BlittableJsonReaderObject jsonObject)
            {
                if (jsonObject.TryGetWithoutThrowingOnError(ValuesPropertyName, out BlittableJsonReaderArray ja1))
                    return new DynamicArray(ja1);

                return new DynamicBlittableJson(jsonObject);
            }

            if (value is BlittableJsonReaderArray ja2)
                return new DynamicArray(ja2);

            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe object ConvertLazyStringValue(LazyStringValue value)
        {
            var buffer = value.Buffer;
            var size = value.Size;

            var result = LazyStringParser.TryParseDateTime(buffer, size, out DateTime dt, out DateTimeOffset dto);
            if (result == LazyStringParser.Result.DateTime)
                return dt;
            if (result == LazyStringParser.Result.DateTimeOffset)
                return dto;

            if (LazyStringParser.TryParseTimeSpan(buffer, size, out TimeSpan ts))
                return ts;

            return value; // ensure that the decompressed lazy string value is returned
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static object TryConvertBlittableJsonReaderObject(BlittableJsonReaderObject blittable)
        {
            if (blittable.TryGet(TypePropertyName, out string type) == false)
                return blittable;

            if (type == null)
                return blittable;

            if (blittable.TryGet(ValuesPropertyName, out BlittableJsonReaderArray array))
                return array;

            if (blittable.TryGet(ValuePropertyName, out object value))
                return value;

            return blittable;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe bool TryConvertStringValue(string value, out object output)
        {
            output = null;

            fixed (char* str = value)
            {
                var result = LazyStringParser.TryParseDateTime(str, value.Length, out DateTime dt, out DateTimeOffset dto);
                if (result == LazyStringParser.Result.DateTime)
                    output = dt;
                if (result == LazyStringParser.Result.DateTimeOffset)
                    output = dto;

                if (LazyStringParser.TryParseTimeSpan(str, value.Length, out var ts))
                    output = ts;
            }

            return output != null;
        }

        public static object ConvertForIndexing(object value)
        {
            if (value == null)
                return null;

            if (value is int || value is long || value is double || value is decimal || value is float || value is short || value is byte)
                return value;

            if (value is string s && TryConvertStringValue(s, out object result))
                return result;

            LazyStringValue lazyString = null;
            if (value is LazyCompressedStringValue lazyCompressedStringValue)
                lazyString = lazyCompressedStringValue.ToLazyStringValue();
            else if (value is LazyStringValue lsv)
                lazyString = lsv;

            if (lazyString != null)
                return ConvertLazyStringValue(lazyString);

            if (value is BlittableJsonReaderObject bjo)
                return TryConvertBlittableJsonReaderObject(bjo);

            return value;
        }

        public static T Convert<T>(object value, bool cast)
        {
            if (value == null || value is DynamicNullObject)
                return default;

            if (cast)
            {
                // HACK
                return (T)value;
            }

            if (value is T t)
                return t;

            var targetType = typeof(T);

            if (targetType.IsGenericType && targetType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                targetType = Nullable.GetUnderlyingType(targetType);
            }

            if (targetType == typeof(Guid))
            {
                return (T)(object)new Guid(value.ToString());
            }

            if (targetType == typeof(string))
            {
                return (T)(object)value.ToString();
            }

            if (targetType == typeof(DateTime))
            {
                if (value is DateTimeOffset dto)
                    return (T)(object)dto.DateTime;

                var s = value as string;
                if (s == null)
                {
                    var lzv = value as LazyStringValue;
                    if (lzv != null)
                        s = lzv;
                }

                if (s != null)
                {
                    if (DateTime.TryParseExact(s, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                        DateTimeStyles.RoundtripKind, out var dateTime))
                        return (T)(object)dateTime;

                    dateTime = RavenDateTimeExtensions.ParseDateMicrosoft(s);
                    return (T)(object)dateTime;
                }
            }

            if (targetType == typeof(DateTimeOffset))
            {
                var s = value as string ?? value as LazyStringValue;

                if (s != null)
                {
                    if (DateTimeOffset.TryParseExact(s, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                        DateTimeStyles.RoundtripKind, out var dateTimeOffset))
                        return (T)(object)dateTimeOffset;

                    return default;
                }
            }

            var lsv = value as LazyStringValue;
            if (lsv != null)
                value = (string)lsv;

            try
            {
                return (T)System.Convert.ChangeType(value, targetType, CultureInfo.InvariantCulture);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException(string.Format("Unable to find suitable conversion for {0} since it is not predefined ", value), e);
            }
        }

        public static IPropertyAccessor GetPropertyAccessor(object value)
        {
            var type = value.GetType();

            if (value is Dictionary<string, object>) // don't use cache when using dictionaries
                return PropertyAccessor.Create(type, value);

            return PropertyAccessorCache.GetOrAdd(type, x => PropertyAccessor.Create(type, value));
        }

        public static IPropertyAccessor GetPropertyAccessorForMapReduceOutput(object value, Dictionary<string, CompiledIndexField> groupByFields)
        {
            var type = value.GetType();

            if (type == typeof(ObjectInstance)) // We don't cache JS types
                return PropertyAccessor.CreateMapReduceOutputAccessor(type, value, groupByFields, true);

            if (value is Dictionary<string, object>) // don't use cache when using dictionaries
                return PropertyAccessor.Create(type, value);

            return PropertyAccessorForMapReduceOutputCache.GetOrAdd(type, x => PropertyAccessor.CreateMapReduceOutputAccessor(type, value, groupByFields));
        }

        public static bool ShouldTreatAsEnumerable(object item)
        {
            if (item == null || item is DynamicNullObject)
                return false;

            if (item is DynamicBlittableJson)
                return false;

            if (item is string || item is LazyStringValue)
                return false;

            if (item is IDictionary)
                return false;

            return true;
        }
    }
}
