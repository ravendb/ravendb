using System;
using System.IO;
using System.Text;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Runtime.Interop;
using Lucene.Net.Documents;
using Raven.Client;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.JavaScript.Jint;
using Raven.Server.Documents.Indexes.Static.JavaScript.V8;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Exceptions;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Sync;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Config.Categories;
using Raven.Server.Extensions.Jint;
using V8.Net;

namespace Raven.Server.Utils
{
    public static class TypeConverter
    {
        private const string TypePropertyName = "$type";

        private static readonly StringSegment ValuesPropertyName = new StringSegment("$values");

        private static readonly StringSegment ValuePropertyName = new StringSegment("$value");

        private static readonly ConcurrentDictionary<Type, IPropertyAccessor> PropertyAccessorCache = new ConcurrentDictionary<Type, IPropertyAccessor>();

        private static readonly ConcurrentDictionary<Type, IPropertyAccessor> PropertyAccessorForMapReduceOutputCache = new ConcurrentDictionary<Type, IPropertyAccessor>();

        private static readonly TypeCache<bool> _isSupportedTypeCache = new (512);

        public static JavaScriptEngineType GetJsEngineType(IJavaScriptOptions jsOptions)
        { 
            return GetJsEngineType(jsOptions?.EngineType); 
        }
        
        public static JavaScriptEngineType GetJsEngineType(JavaScriptEngineType? jsEngineType)
        { 
            return jsEngineType ?? JavaScriptEngineType.Jint; 
        }
        
        private static bool IsSupportedTypeInternal(object value)
        {
            if (value is DynamicNullObject)
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

        private static bool UnlikelyIsSupportedType(object value)
        {
            bool result = IsSupportedTypeInternal(value);
            _isSupportedTypeCache.Put(value.GetType(), result);
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsSupportedType(object value)
        {
            if (value == null)
                return true;

            if (_isSupportedTypeCache.TryGet(value.GetType(), out bool result))
                return result;

            return UnlikelyIsSupportedType(value);
        }

        public static object ToBlittableSupportedType(object value, bool flattenArrays = false, bool forIndexing = false, 
            IJsEngineHandle engine = null, JsonOperationContext context = null, bool isRoot = true)
        {
            return ToBlittableSupportedType(value, value, flattenArrays, forIndexing, 0, engine, context, out _, isRoot: isRoot);
        }

        public static object ToBlittableSupportedType(object value, out BlittableSupportedReturnType returnType, bool flattenArrays = false, bool forIndexing = false, 
            IJsEngineHandle engine = null, JsonOperationContext context = null)
        {
            return ToBlittableSupportedType(value, value, flattenArrays, forIndexing, 0, engine, context, out returnType);
        }

        public enum BlittableSupportedReturnType : int
        {
            Null = 0,
            Same = 1,
            Javascript = 2,
            Runtime = 3,
            Ignored = 4,
            Dictionary = 5,
            GenericDictionary = 6,
            Enumerable = 7,
            String = 8
        }

        private static readonly TypeCache<BlittableSupportedReturnType> _supportedTypeCache = new (512);

        private static BlittableSupportedReturnType DoBlittableSupportedTypeInternal(Type type, object value)
        {
            if (type == typeof(DynamicNullObject))
                return BlittableSupportedReturnType.Null;

            if (type == typeof(LazyStringValue) || type == typeof(LazyCompressedStringValue) || type == typeof(LazyNumberValue) ||
                type == typeof(string) || type == typeof(bool) || type == typeof(int) || type == typeof(long) ||
                type == typeof(double) || type == typeof(decimal) || type == typeof(float) || type == typeof(short) ||
                type == typeof(byte) ||
                type == typeof(DateTime) || type == typeof(DateTimeOffset) || type == typeof(TimeSpan) ||
                type == typeof(DateOnly) || type == typeof(TimeOnly))
                return BlittableSupportedReturnType.Same;

            if (value is JsHandle || value is JsValue || value is InternalHandle)
                return BlittableSupportedReturnType.Javascript;

            if (value is IEnumerable<IFieldable> || value is IFieldable)
                return BlittableSupportedReturnType.Ignored;

            if (value is IDictionary)
                return BlittableSupportedReturnType.Dictionary;

            if (value is IDictionary<object, object>)
                return BlittableSupportedReturnType.GenericDictionary;

            if (value is Enum)
                return BlittableSupportedReturnType.String;

            if (type == typeof(char[]) || type == typeof(byte[]) || value is IEnumerable<char> || value is IEnumerable)
                return BlittableSupportedReturnType.Enumerable;

            return BlittableSupportedReturnType.Runtime;
        }

        private static object ToBlittableSupportedType(object root, object value, bool flattenArrays, bool forIndexing, int recursiveLevel, 
            IJsEngineHandle engine, JsonOperationContext context, out BlittableSupportedReturnType blittableSupportedReturnType, bool isRoot = true)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            if (recursiveLevel > MaxAllowedRecursiveLevelForType)
                NestingLevelTooDeep(root);

            if (value == null)
            {
                blittableSupportedReturnType = BlittableSupportedReturnType.Null;
                return null;
            }
            
            // We cache the return type.
            var type = value.GetType();
            if (!_supportedTypeCache.TryGet(type, out BlittableSupportedReturnType returnType))
            {
                returnType = DoBlittableSupportedTypeInternal(type, value);
                _supportedTypeCache.Put(type, returnType);
            }

            blittableSupportedReturnType = returnType;

            if (returnType == BlittableSupportedReturnType.Same)
                return value;

            switch (returnType)
            {
                case BlittableSupportedReturnType.Null: return null;
                case BlittableSupportedReturnType.Ignored: return "__ignored";
                case BlittableSupportedReturnType.String: return value.ToString();
                case BlittableSupportedReturnType.Javascript:
                {
                    JsValue valueJint = null;
                    InternalHandle valueV8 = InternalHandle.Empty;
                    if (value is JsHandle jsHandle)
                    {
                        var jsEngineType = jsHandle.EngineType;
                        switch (jsEngineType)
                        {
                            case JavaScriptEngineType.Jint:
                                valueJint = jsHandle.Jint.Item;
                                break;
                            case JavaScriptEngineType.V8:
                                valueV8 = jsHandle.V8.Item;
                                break;
                            default:
                                throw new NotSupportedException($"Not supported JS engine kind '{jsEngineType}'.");
                        }
                    }
                    else if (value is JsValue)
                        valueJint = (JsValue)value;
                    else if (value is InternalHandle)
                        valueV8 = (InternalHandle)value;

                    if (valueJint != null)
                    {
                        if (valueJint.IsNull())
                        {
                            if (forIndexing && valueJint is DynamicJsNullJint dynamicJsNull)
                                return dynamicJsNull.IsExplicitNull ? DynamicNullObject.ExplicitNull : DynamicNullObject.Null;

                            return null;
                        }

                        if (valueJint.IsUndefined())
                            return null;
                        if (valueJint.IsString())
                            return valueJint.AsString();
                        if (valueJint.IsBoolean())
                            return valueJint.AsBoolean();
                        if (valueJint.IsNumber())
                            return valueJint.AsNumber();
                        if (valueJint.IsDate())
                            return valueJint.AsDate().ToDateTime();
                        //object wrapper is an object so it must come before the object
                        if (valueJint is ObjectWrapper ow)
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

                            ThrowInvalidObject(new JsHandle(valueJint));
                        }
                        //Array is an object in Jint
                        else if (valueJint.IsArray())
                        {
                            var arr = valueJint.AsArray();
                            var convertedArray = EnumerateArray(root, arr, flattenArrays, forIndexing, recursiveLevel + 1, engine, context);
                            return new DynamicJsonArray(flattenArrays ? Flatten(convertedArray) : convertedArray);
                        }
                        else if (valueJint.IsObject())
                        {
                            return JsBlittableBridgeJint.Translate(context, (Engine)engine, valueJint.AsObject(), isRoot: isRoot);
                        }
                        ThrowInvalidObject(new JsHandle(valueJint));
                    }
                    else if (!valueV8.IsEmpty)
                    {
                        if (valueV8.IsNull)
                        {
                            if (forIndexing && valueV8.IsObject && valueV8.Object is DynamicJsNullV8 dynamicJsNull)
                                return dynamicJsNull.IsExplicitNull ? DynamicNullObject.ExplicitNull : DynamicNullObject.Null;

                            return null;
                        }

                        if (valueV8.IsUndefined)
                            return null;
                        if (valueV8.IsStringEx)
                            return valueV8.AsString;
                        if (valueV8.IsRegExp)              // added, check
                            return valueV8.AsString;
                        if (valueV8.IsBoolean)
                            return valueV8.AsBoolean;
                        if (valueV8.IsInt32)               // added, check
                            return valueV8.AsInt32;
                        if (valueV8.IsNumberEx)
                            return valueV8.AsDouble;
                        if (valueV8.IsDate)
                            return valueV8.AsDate;
                        if (valueV8.IsArray)
                        {
                            var convertedArray = EnumerateArray(root, valueV8, flattenArrays, forIndexing, recursiveLevel + 1, engine, context);
                            return new DynamicJsonArray(flattenArrays ? Flatten(convertedArray) : convertedArray);
                        }                
                        if (valueV8.IsObject)
                        {
                            object boundObject = valueV8.BoundObject;
                            if (boundObject != null)
                            {
                                switch (boundObject)
                                {
                                    case LazyStringValue lsv:
                                        return lsv;
                                    case LazyCompressedStringValue lcsv:
                                        return lcsv;
                                    case LazyNumberValue lnv:
                                        return lnv; //should be already blittable supported type.
                                }
                            }
                            return JsBlittableBridgeV8.Translate(context, (V8Engine)engine, valueV8, isRoot: isRoot);
                        }
                        ThrowInvalidObject(new JsHandle(valueV8));
                        return null;
                    }

                    throw new InvalidOperationException("Invalid object type " + value.GetType());
                }
                case BlittableSupportedReturnType.Enumerable:
                {
                    if (type == typeof(char[]))
                        return new string((char[])value);
                    if (type == typeof(byte[]))
                        return System.Convert.ToBase64String((byte[])value);

                    if (value is IEnumerable<char> charEnumerable)
                        return new string(charEnumerable.ToArray());

                    var enumerable = (IEnumerable)value;
                    if (ShouldTreatAsEnumerable(enumerable))
                        return EnumerableToJsonArray(flattenArrays ? Flatten(enumerable) : enumerable, root, flattenArrays, forIndexing, recursiveLevel, engine, context);

                    break;
                }
                case BlittableSupportedReturnType.Dictionary:
                {
                    var @object = new DynamicJsonValue();

                    var dictionary = (IDictionary)value;
                    foreach (var key in dictionary.Keys)
                    {
                        var keyAsString = KeyAsString(key: ToBlittableSupportedType(root, key, flattenArrays, forIndexing, recursiveLevel: recursiveLevel + 1, engine: engine, context: context, out _));
                        @object[keyAsString] = ToBlittableSupportedType(root, dictionary[key], flattenArrays, forIndexing, recursiveLevel: recursiveLevel + 1, engine: engine, context: context, out _);
                    }

                    return @object;
                }
                case BlittableSupportedReturnType.GenericDictionary:
                {
                    var @object = new DynamicJsonValue();

                    var dDictionary = (IDictionary<object, object>)value;
                    foreach (var key in dDictionary.Keys)
                    {
                        var keyAsString = KeyAsString(key: ToBlittableSupportedType(root, key, flattenArrays, forIndexing, recursiveLevel: recursiveLevel + 1, engine: engine, context: context, out _));
                        @object[keyAsString] = ToBlittableSupportedType(root, dDictionary[key], flattenArrays, forIndexing, recursiveLevel: recursiveLevel + 1, engine: engine, context: context, out _);
                    }

                    return @object;
                }
            }

            if (value is DynamicBlittableJson dynamicDocument)
                return dynamicDocument.BlittableJson;

            if (value is Guid guid)
                return guid.ToString("D");

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

                inner[property.Key] = ToBlittableSupportedType(root, propertyValue, flattenArrays, forIndexing, recursiveLevel + 1, engine, context, out _);
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

        private static void ThrowInvalidObject(JsHandle jsValue)
        {
            throw new InvalidOperationException("Invalid type " + jsValue.ValueType);
        }

        private static IEnumerable<object> EnumerateArray(object root, ArrayInstance arr, bool flattenArrays, bool forIndexing, int recursiveLevel, IJsEngineHandle engine, JsonOperationContext context)
        {
            foreach (var (key, val) in arr.GetOwnPropertiesWithoutLength())
            {
                yield return ToBlittableSupportedType(root, val.Value, flattenArrays, forIndexing, recursiveLevel, engine, context, out _);
            }
        }

        private static IEnumerable<object> EnumerateArray(object root, InternalHandle jsArr, bool flattenArrays, bool forIndexing, int recursiveLevel, IJsEngineHandle engine, JsonOperationContext context)
        {
            for (int i = 0; i < jsArr.ArrayLength; i++)
            {
                using (var value = jsArr.GetProperty(i))
                {
                    yield return ToBlittableSupportedType(root, value, flattenArrays, forIndexing, recursiveLevel, engine, context, out _);
                }
            }
        }

        private static DynamicJsonArray EnumerableToJsonArray(IEnumerable propertyEnumerable, object root, bool flattenArrays, bool forIndexing, int recursiveLevel, IJsEngineHandle engine, JsonOperationContext context)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            
            var dja = new DynamicJsonArray();

            foreach (var x in propertyEnumerable)
            {
                dja.Add(ToBlittableSupportedType(root, x, flattenArrays, forIndexing, recursiveLevel + 1, engine, context, out _));
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
            RuntimeHelpers.EnsureSufficientExecutionStack();
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

        private static readonly Type[] DynamicRules = new[]
        {
            typeof(string), typeof(LazyCompressedStringValue), 
            typeof(LazyStringValue), typeof(BlittableJsonReaderObject),
            typeof(BlittableJsonReaderArray)
        };

        private enum DynamicRuleTypeLocation : int
        {
            String = 0,
            LazyCompressedStringValue = 1,
            LazyStringValue = 2,
            BlittableJsonReaderObject = 3,
            BlittableJsonReaderArray = 4,
        }

        public static dynamic ToDynamicType(object value)
        {
            if (value == null)
                return DynamicNullObject.ExplicitNull;

            Type objectType = value.GetType();
            if (objectType == DynamicRules[(int)DynamicRuleTypeLocation.String])
            {
                if (TryConvertStringValue((string)value, out object result))
                    return result;

                return value;
            }

            LazyStringValue lazyString = null;
            if (objectType == DynamicRules[(int)DynamicRuleTypeLocation.LazyCompressedStringValue])
                lazyString = ((LazyCompressedStringValue)value).ToLazyStringValue();
            else if (objectType == DynamicRules[(int)DynamicRuleTypeLocation.LazyStringValue])
                lazyString = (LazyStringValue) value;
            if (lazyString != null)
                return ConvertLazyStringValue(lazyString);

            if (objectType == DynamicRules[(int)DynamicRuleTypeLocation.BlittableJsonReaderObject])
            {
                var jsonObject = (BlittableJsonReaderObject)value;
                if (jsonObject.TryGetWithoutThrowingOnError(ValuesPropertyName, out BlittableJsonReaderArray ja1))
                    return new DynamicArray(ja1);

                return new DynamicBlittableJson(jsonObject);
            }

            if (objectType == DynamicRules[(int)DynamicRuleTypeLocation.BlittableJsonReaderArray])
                return new DynamicArray((BlittableJsonReaderArray)value);

            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe object ConvertLazyStringValue(LazyStringValue value)
        {
            var buffer = value.Buffer;
            var size = value.Size;

            var result = LazyStringParser.TryParseDateTime(buffer, size, out DateTime dt, out DateTimeOffset dto, properlyParseThreeDigitsMilliseconds: true);
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
        internal static unsafe bool TryConvertStringValue(string value, out object output)
        {
            output = null;

            fixed (char* str = value)
            {
                var result = LazyStringParser.TryParseDateTime(str, value.Length, out DateTime dt, out DateTimeOffset dto, properlyParseThreeDigitsMilliseconds: true);

                output = result switch
                {
                    LazyStringParser.Result.DateTime => dt,
                    LazyStringParser.Result.DateTimeOffset => dto,
                    _ => null
                };
                
                

                if (LazyStringParser.TryParseTimeSpan(str, value.Length, out var ts))
                    output = ts;
                
            }

            return output != null;
        }

        public static object ConvertForIndexing(object value)
        {
            if (value == null)
                return null;

            Type objectType = value.GetType();
            if (objectType == DynamicRules[(int)DynamicRuleTypeLocation.String] && TryConvertStringValue((string)value, out object result))
                return result;

            LazyStringValue lazyString = null;
            if (objectType == DynamicRules[(int)DynamicRuleTypeLocation.LazyCompressedStringValue])
                lazyString = ((LazyCompressedStringValue)value).ToLazyStringValue();
            else if (objectType == DynamicRules[(int)DynamicRuleTypeLocation.LazyStringValue])
                lazyString = (LazyStringValue)value;

            if (lazyString != null)
                return ConvertLazyStringValue(lazyString);

            if (objectType == DynamicRules[(int)DynamicRuleTypeLocation.BlittableJsonReaderObject])
                return TryConvertBlittableJsonReaderObject((BlittableJsonReaderObject) value);

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
            if (value is JsHandle) // We don't cache JS types
                return PropertyAccessor.CreateMapReduceOutputAccessorJs(groupByFields);

            var type = value.GetType();
            if (value is Dictionary<string, object>) // don't use cache when using dictionaries
                return PropertyAccessor.Create(type, value);

            return PropertyAccessorForMapReduceOutputCache.GetOrAdd(type, x => PropertyAccessor.CreateMapReduceOutputAccessor(value, groupByFields, type: type));
        }


        private static readonly TypeCache<bool> _treatAsEnumerableCache = new(512);

        public static bool ShouldTreatAsEnumerable(object item)
        {
            if (item == null)
                return false;

            if (_treatAsEnumerableCache.TryGet(item.GetType(), out bool result))
                return result;

            return UnlikelyShouldTreatAsEnumerable(item);
        }

        private static bool UnlikelyShouldTreatAsEnumerable(object item)
        {
            bool result;
            if (item is DynamicNullObject || item is DynamicBlittableJson || item is string || item is LazyStringValue || item is IDictionary)
                result = false;
            else 
                result = true;

            _treatAsEnumerableCache.Put(item.GetType(), result);

            return result;
        }
        
        public static string ConvertResultToString(ScriptRunnerResult resultBase)
        {
            var result = (ScriptRunnerResult)resultBase;
            var ms = new MemoryStream();
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var writer = new BlittableJsonTextWriter(ctx, ms))
            {
                writer.WriteStartObject();

                writer.WritePropertyName("Result");

                if (result.IsNull)
                {
                    writer.WriteNull();
                }
                else if (result.RawJsValue.IsBoolean)
                {
                    writer.WriteBool(result.RawJsValue.AsBoolean);
                }
                else if (result.RawJsValue.IsStringEx)
                {
                    writer.WriteString(result.RawJsValue.AsString);
                }
                else if (result.RawJsValue.IsDate)
                {
                    var date = result.RawJsValue.AsDate;
                    writer.WriteString(date.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite));
                }
                else if (result.RawJsValue.IsInt32)
                {
                    writer.WriteInteger(result.RawJsValue.AsInt32);
                }
                else if (result.RawJsValue.IsNumberEx)
                {
                    writer.WriteDouble(result.RawJsValue.AsDouble);
                }
                else
                {
                    writer.WriteObject(result.TranslateToObject(ctx));
                }

                writer.WriteEndObject();
                writer.Flush();
            }

            var str = Encoding.UTF8.GetString(ms.ToArray());
            return str;
        }
    }
}
