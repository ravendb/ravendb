using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Object;
using Jint.Runtime.Interop;
using Lucene.Net.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch;
using Raven.Server.Exceptions;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Extensions;

namespace Raven.Server.Utils
{
    internal static class TypeConverter
    {
        private const string TypePropertyName = "$type";

        private const string ValuesPropertyName = "$values";

        private static readonly string TypeList = typeof(List<>).FullName;

        private static readonly ConcurrentDictionary<Type, IPropertyAccessor> PropertyAccessorCache = new ConcurrentDictionary<Type, IPropertyAccessor>();

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

            if (value is IEnumerable enumerable)
            {
                if (ShouldTreatAsEnumerable(enumerable))
                {
                    var isSupportedObjects = true;
                    var objectEnumerable = value as IEnumerable<object>;
                    if (objectEnumerable == null)
                        EnumerableNullException();

                    foreach (var x in objectEnumerable)
                    {
                        isSupportedObjects &= IsSupportedType(x);
                    }

                    return isSupportedObjects;
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

        public static object ToBlittableSupportedType(object value, bool flattenArrays = false, Engine engine = null, JsonOperationContext context = null)
        {
            return ToBlittableSupportedType(value, value, flattenArrays, 0, engine, context);
        }

        private static object ToBlittableSupportedType(object root, object value, bool flattenArrays, int recursiveLevel, Engine engine, JsonOperationContext context)
        {
            if (recursiveLevel > MaxAllowedRecursiveLevelForType)
                NestingLevelTooDeep(root);

            if (value is JsValue js)
            {
                if (js.IsNull() || js.IsUndefined())
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
                    return new DynamicJsonArray(flattenArrays ? Flatten(EnumerateArray(arr)) : EnumerateArray(arr));
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
                    @object[key.ToString()] = ToBlittableSupportedType(root, dictionary[key], flattenArrays, recursiveLevel: recursiveLevel + 1, engine: engine, context: context);

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
                {
                    var objectEnumerable = value as IEnumerable<object>;
                    return EnumerableToJsonArray(flattenArrays ? Flatten(objectEnumerable) : objectEnumerable, root, flattenArrays, recursiveLevel, engine, context);
                }
            }

            var inner = new DynamicJsonValue();
            var accessor = GetPropertyAccessor(value);

            foreach (var property in accessor.GetPropertiesInOrder(value))
            {
                var propertyValue = property.Value;
                if (propertyValue is IEnumerable<object> propertyValueAsEnumerable && ShouldTreatAsEnumerable(propertyValue))
                {
                    inner[property.Key] = EnumerableToJsonArray(propertyValueAsEnumerable, root, flattenArrays, recursiveLevel, engine, context);
                    continue;
                }

                inner[property.Key] = ToBlittableSupportedType(root, propertyValue, flattenArrays, recursiveLevel + 1, engine, context);
            }

            return inner;
        }

        private static void ThrowInvalidObject(JsValue jsValue)
        {
            throw new InvalidOperationException("Invalid type " + jsValue);
        }

        private static IEnumerable<object> EnumerateArray(ArrayInstance arr)
        {
            foreach (var (key, val) in arr.GetOwnProperties())
            {
                if (key == "length")
                    continue;

                yield return ToBlittableSupportedType(val.Value);
            }
        }

        private static DynamicJsonArray EnumerableToJsonArray(IEnumerable<object> propertyEnumerable, object root, bool flattenArrays, int recursiveLevel, Engine engine, JsonOperationContext context)
        {
            if (propertyEnumerable == null)
                EnumerableNullException();

            var dja = new DynamicJsonArray();

            foreach (var x in propertyEnumerable)
            {
                dja.Add(ToBlittableSupportedType(root, x, flattenArrays, recursiveLevel + 1, engine, context));
            }

            return dja;
        }

        private static void NestingLevelTooDeep(object value)
        {
            throw new SerializationNestedLevelTooDeepException(
                                $"Reached nesting level of {MaxAllowedRecursiveLevelForType} for type {value.GetType().Name}, recursive types that exceed the allowed nesting level are not supported.");
        }

        public static int MaxAllowedRecursiveLevelForType { get; } = 100;

        private static IEnumerable<object> Flatten(IEnumerable items)
        {
            if (items == null)
                EnumerableNullException();

            foreach (var item in items)
            {
                if (item is IEnumerable enumerable && ShouldTreatAsEnumerable(enumerable))
                {
                    foreach (var nestedItem in Flatten(enumerable))
                    {
                        yield return nestedItem;
                    }

                    yield break;
                }

                yield return item;
            }
        }

        private static void EnumerableNullException()
        {
            throw new InvalidOperationException("Cannot enumerate on null.");
        }

        public static dynamic ToDynamicType(object value)
        {
            if (value is DynamicNullObject)
                return value;

            if (value == null)
                return DynamicNullObject.ExplicitNull;

            BlittableJsonReaderArray jsonArray;
            if (value is BlittableJsonReaderObject jsonObject)
            {
                if (jsonObject.TryGetWithoutThrowingOnError("$values", out jsonArray))
                    return new DynamicArray(jsonArray);

                return new DynamicBlittableJson(jsonObject);
            }

            jsonArray = value as BlittableJsonReaderArray;
            if (jsonArray != null)
                return new DynamicArray(jsonArray);

            return ConvertForIndexing(value);
        }

        public static unsafe object ConvertForIndexing(object value)
        {
            if (value == null)
                return null;

            if (value is BlittableJsonReaderObject blittableJsonObject)
            {
                if (blittableJsonObject.TryGet(TypePropertyName, out string type) == false)
                    return blittableJsonObject;

                if (type == null)
                    return blittableJsonObject;

                if (type.StartsWith(TypeList) == false)
                    return blittableJsonObject;

                if (blittableJsonObject.TryGet(ValuesPropertyName, out BlittableJsonReaderArray values))
                    return values;

                throw new NotSupportedException($"Detected list type '{type}' but could not extract '{values}'.");
            }

            var lazyString = value as LazyStringValue;
            if (lazyString == null)
            {
                if (value is LazyCompressedStringValue lazyCompressedStringValue)
                    lazyString = lazyCompressedStringValue.ToLazyStringValue();
            }

            if (lazyString != null)
            {
                var result = LazyStringParser.TryParseDateTime(lazyString.Buffer, lazyString.Size, out DateTime dt, out DateTimeOffset dto);
                if (result == LazyStringParser.Result.DateTime)
                    return dt;
                if (result == LazyStringParser.Result.DateTimeOffset)
                    return dto;

                if (LazyStringParser.TryParseTimeSpan(lazyString.Buffer, lazyString.Size, out TimeSpan ts))
                    return ts;

                return lazyString; // ensure that the decompressed lazy string value is returned
            }

            if (value is string s)
            {
                fixed (char* str = s)
                {
                    var result = LazyStringParser.TryParseDateTime(str, s.Length, out DateTime dt, out DateTimeOffset dto);
                    if (result == LazyStringParser.Result.DateTime)
                        return dt;
                    if (result == LazyStringParser.Result.DateTimeOffset)
                        return dto;

                    if (LazyStringParser.TryParseTimeSpan(str, s.Length, out var ts))
                        return ts;
                }
            }

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

            if (targetType.GetTypeInfo().IsGenericType && targetType.GetGenericTypeDefinition() == typeof(Nullable<>))
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

        public static IPropertyAccessor GetPropertyAccessorForMapReduceOutput(object value, HashSet<CompiledIndexField> groupByFields)
        {
            var type = value.GetType();
            
            if (type == typeof(ObjectInstance)) // We don't cache JS types
                return PropertyAccessor.CreateMapReduceOutputAccessor(type, value, groupByFields, true);

            if (value is Dictionary<string, object>) // don't use cache when using dictionaries
                return PropertyAccessor.Create(type, value);

            return PropertyAccessorCache.GetOrAdd(type, x => PropertyAccessor.CreateMapReduceOutputAccessor(type, value, groupByFields));
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
