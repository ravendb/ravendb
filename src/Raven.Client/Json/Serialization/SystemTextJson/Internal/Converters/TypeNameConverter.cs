using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Raven.Client.Util;
using Sparrow.Extensions;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal static class TypeNameConverter
    {
        internal const string TypePropertyName = "$type";

        internal const string ValuesPropertyName = "$values";

        private static readonly ConcurrentDictionary<Type, string> _typeNames = new();

        private static readonly ConcurrentDictionary<string, Type> _types = new();

        private static readonly ConcurrentDictionary<Type, byte[]> _typeNamePrefixes = new();

        private static readonly ConcurrentDictionary<Type, DictionaryKey> _dictionaryKeys = new();

        private static readonly ConcurrentDictionary<Type, TypeShape> _shapes = new();

        private static readonly ConcurrentDictionary<Type, DictionaryShape> _dictionaryShapes = new();

        private static readonly ConcurrentDictionary<Type, Func<IList>> _listFactories = new();

        private static readonly ConcurrentDictionary<Type, Func<IList, object>> _collectionFactories = new();

        private static readonly ConditionalWeakTable<JsonSerializerOptions, ConcurrentDictionary<Type, bool>> _writtenAsIs = new();

        private static readonly ConditionalWeakTable<JsonSerializerOptions, ConcurrentDictionary<Type, JsonTypeInfo>> _typeInfos = new();

        private static readonly ConcurrentDictionary<Type, TypedValue> _typedValues = new();

        private static readonly ConcurrentDictionary<(Type KeyType, Type ValueType), DictionaryEntries> _dictionaryEntries = new();

        [ThreadStatic]
        private static HashSet<object> _writingWithTypeName;

        [ThreadStatic]
        private static object _writingTypeNameFor;

        internal static readonly JsonConverter<string> TypeNameProperty = new TypeNamePropertyConverter();

        internal static object TypeNameFor(object target) => ReferenceEquals(target, _writingTypeNameFor) ? GetTypeName(target.GetType()) : null;

        private static bool HasTypeNameProperty(JsonTypeInfo typeInfo) =>
            typeInfo.Kind == JsonTypeInfoKind.Object && typeInfo.Properties.Count > 0 && ReferenceEquals(typeInfo.Properties[0].CustomConverter, TypeNameProperty);

        private sealed class TypeNamePropertyConverter : JsonConverter<string>
        {
            public override string Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) => reader.GetString();

            public override void Write(Utf8JsonWriter writer, string value, JsonSerializerOptions options) => writer.WriteStringValue(value);
        }

        private static readonly ConcurrentDictionary<Type, MethodInfo> _immutableFactories = new();

        private static readonly Dictionary<string, string> ImmutableInterfaceImplementations = new(StringComparer.Ordinal)
        {
            ["System.Collections.Immutable.IImmutableList`1"] = "System.Collections.Immutable.ImmutableList`1",
            ["System.Collections.Immutable.IImmutableQueue`1"] = "System.Collections.Immutable.ImmutableQueue`1",
            ["System.Collections.Immutable.IImmutableStack`1"] = "System.Collections.Immutable.ImmutableStack`1",
            ["System.Collections.Immutable.IImmutableSet`1"] = "System.Collections.Immutable.ImmutableHashSet`1",
            ["System.Collections.Immutable.IImmutableDictionary`2"] = "System.Collections.Immutable.ImmutableDictionary`2"
        };

        private static readonly Dictionary<string, string> ImmutableFactories = new(StringComparer.Ordinal)
        {
            ["System.Collections.Immutable.ImmutableArray`1"] = "System.Collections.Immutable.ImmutableArray",
            ["System.Collections.Immutable.ImmutableList`1"] = "System.Collections.Immutable.ImmutableList",
            ["System.Collections.Immutable.ImmutableQueue`1"] = "System.Collections.Immutable.ImmutableQueue",
            ["System.Collections.Immutable.ImmutableStack`1"] = "System.Collections.Immutable.ImmutableStack",
            ["System.Collections.Immutable.ImmutableHashSet`1"] = "System.Collections.Immutable.ImmutableHashSet",
            ["System.Collections.Immutable.ImmutableSortedSet`1"] = "System.Collections.Immutable.ImmutableSortedSet",
            ["System.Collections.Immutable.ImmutableDictionary`2"] = "System.Collections.Immutable.ImmutableDictionary",
            ["System.Collections.Immutable.ImmutableSortedDictionary`2"] = "System.Collections.Immutable.ImmutableSortedDictionary"
        };

        public static JsonConverter TryCreate(Type declaredType, TypeNameHandling typeNameHandling, JsonSerializerOptions options)
        {
            if (typeNameHandling == TypeNameHandling.None || declaredType == typeof(object) || IsCandidate(declaredType, options) == false)
                return null;

            Type converterType = typeof(DeclaredTypeConverter<>).MakeGenericType(declaredType);
            return (JsonConverter)Activator.CreateInstance(converterType, [typeNameHandling == TypeNameHandling.Objects]);
        }

        private static bool IsCandidate(Type declaredType, JsonSerializerOptions options)
        {
            if (declaredType.IsValueType || declaredType == typeof(string) || declaredType.IsPointer || declaredType.IsByRef)
                return false;

            if (IsSparrowType(declaredType) || HasConverter(declaredType, options))
                return false;

            if (TryGetDictionaryTypes(declaredType, out _, out Type valueType))
                return IsOpen(declaredType) || IsItemCandidate(valueType, options);

            if (typeof(IEnumerable).IsAssignableFrom(declaredType))
                return IsItemCandidate(GetItemType(declaredType), options);

            return declaredType.IsSealed == false;
        }

        private static bool IsItemCandidate(Type itemType, JsonSerializerOptions options)
        {
            if (itemType == typeof(object))
                return true;

            if (itemType.IsValueType || itemType == typeof(string) || itemType.IsSealed)
                return false;

            return IsSparrowType(itemType) == false && HasConverter(itemType, options) == false;
        }

        private static bool IsOpen(Type type) => type.IsInterface || type.IsAbstract || type.IsSealed == false;

        private static bool IsSparrowType(Type type) =>
            type.Namespace != null && (type.Namespace == "Sparrow" || type.Namespace.StartsWith("Sparrow.", StringComparison.Ordinal));

        private static bool HasConverter(Type type, JsonSerializerOptions options)
        {
            for (int i = 0; i < options.Converters.Count; i++)
            {
                if (options.Converters[i].CanConvert(type))
                    return true;
            }

            return false;
        }

        private static JsonTypeInfo GetTypeInfo(Type type, JsonSerializerOptions options)
        {
            ConcurrentDictionary<Type, JsonTypeInfo> cache = _typeInfos.GetValue(options, static _ => new ConcurrentDictionary<Type, JsonTypeInfo>());
            return cache.TryGetValue(type, out JsonTypeInfo typeInfo) ? typeInfo : cache.GetOrAdd(type, options.GetTypeInfo(type));
        }

        private static TypedValue GetTypedValue(Type type)
        {
            return _typedValues.GetOrAdd(type, static t => (TypedValue)Activator.CreateInstance(typeof(TypedValue<>).MakeGenericType(t)));
        }

        private static void WriteTyped(Utf8JsonWriter writer, object value, Type type, JsonSerializerOptions options)
        {
            JsonTypeInfo typeInfo = GetTypeInfo(type, options);
            if (GetTypedValue(type).TryWrite(writer, value, typeInfo, options) == false)
                JsonSerializer.Serialize(writer, value, typeInfo);
        }

        private static object ReadTyped(ref Utf8JsonReader reader, Type type, JsonSerializerOptions options)
        {
            JsonTypeInfo typeInfo = GetTypeInfo(type, options);
            if (reader.TokenType != JsonTokenType.Null && GetTypedValue(type).TryRead(ref reader, typeInfo, options, out object value))
                return value;

            return JsonSerializer.Deserialize(ref reader, typeInfo);
        }

        private abstract class TypedValue
        {
            public abstract bool TryWrite(Utf8JsonWriter writer, object value, JsonTypeInfo typeInfo, JsonSerializerOptions options);

            public abstract bool TryRead(ref Utf8JsonReader reader, JsonTypeInfo typeInfo, JsonSerializerOptions options, out object value);
        }

        private sealed class TypedValue<T> : TypedValue
        {
            public override bool TryWrite(Utf8JsonWriter writer, object value, JsonTypeInfo typeInfo, JsonSerializerOptions options)
            {
                if (typeInfo.Converter is not JsonConverter<T> converter)
                    return false;

                converter.Write(writer, (T)value, options);
                return true;
            }

            public override bool TryRead(ref Utf8JsonReader reader, JsonTypeInfo typeInfo, JsonSerializerOptions options, out object value)
            {
                if (typeInfo.Converter is not JsonConverter<T> converter)
                {
                    value = null;
                    return false;
                }

                value = converter.Read(ref reader, typeof(T), options);
                return true;
            }
        }

        private abstract class DictionaryEntries
        {
            public abstract bool TryWrite(Utf8JsonWriter writer, object dictionary, in ItemWriter values, JsonSerializerOptions options);
        }

        private sealed class DictionaryEntries<TKey, TValue> : DictionaryEntries
        {
            public override bool TryWrite(Utf8JsonWriter writer, object dictionary, in ItemWriter values, JsonSerializerOptions options)
            {
                if (dictionary is not IEnumerable<KeyValuePair<TKey, TValue>> pairs)
                    return false;

                foreach (KeyValuePair<TKey, TValue> pair in pairs)
                {
                    if (typeof(TKey) == typeof(string))
                        writer.WritePropertyName((string)(object)pair.Key);
                    else
                        WriteKey(writer, pair.Key, typeof(TKey), options);

                    values.Write(writer, pair.Value, options);
                }

                return true;
            }
        }

        private static bool IsWrittenAsIs(Type type, JsonSerializerOptions options)
        {
            ConcurrentDictionary<Type, bool> cache = _writtenAsIs.GetValue(options, static _ => new ConcurrentDictionary<Type, bool>());
            return cache.TryGetValue(type, out bool writtenAsIs) ? writtenAsIs : cache.GetOrAdd(type, ComputeWrittenAsIs(type, options));
        }

        private static bool ComputeWrittenAsIs(Type type, JsonSerializerOptions options)
        {
            Type underlying = Nullable.GetUnderlyingType(type) ?? type;
            if (underlying.IsPrimitive || underlying.IsEnum || underlying == typeof(string) || underlying == typeof(decimal) ||
                underlying == typeof(DateTime) || underlying == typeof(DateTimeOffset) || underlying == typeof(TimeSpan) ||
                underlying == typeof(Guid) || underlying == typeof(Uri) || underlying == typeof(byte[]) ||
                underlying == typeof(JsonElement) || typeof(JsonNode).IsAssignableFrom(underlying) || typeof(JsonDocument).IsAssignableFrom(underlying) ||
                underlying == typeof(JsonDynamicObject) || underlying == typeof(JsonDynamicArray) || underlying == typeof(JsonDynamicValue))
                return true;

#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
            if (underlying == typeof(DateOnly) || underlying == typeof(TimeOnly))
                return true;
#endif

            return IsSparrowType(underlying) || HasConverter(underlying, options);
        }

        private static TypeShape GetShape(Type type) => _shapes.GetOrAdd(type, static t => new TypeShape(t));

        private sealed class TypeShape
        {
            public readonly bool IsDictionary;

            public readonly Type KeyType;

            public readonly Type ValueType;

            public readonly Type ItemType;

            public readonly Type CreatedType;

            public readonly bool IsPlainObject;

            public TypeShape(Type type)
            {
                IsDictionary = ComputeDictionaryTypes(type, out KeyType, out ValueType);
                ItemType = ComputeItemType(type);
                CreatedType = ComputeCreatedType(type);
                IsPlainObject = type != typeof(object) && type.IsInterface == false && type.IsAbstract == false && IsDictionary == false &&
                                typeof(IEnumerable).IsAssignableFrom(type) == false;
            }
        }

        private readonly struct ItemWriter
        {
            private readonly Type _itemType;

            private readonly Type _runtimeType;

            private readonly TypedValue _typed;

            private readonly JsonTypeInfo _typeInfo;

            private readonly bool _alwaysWriteObjectTypeName;

            public ItemWriter(Type itemType, bool alwaysWriteObjectTypeName, JsonSerializerOptions options)
            {
                _itemType = itemType;
                _runtimeType = Nullable.GetUnderlyingType(itemType) ?? itemType;
                _alwaysWriteObjectTypeName = alwaysWriteObjectTypeName;

                bool exact = _runtimeType.IsSealed || _runtimeType.IsValueType;
                bool direct = exact ? IsWrittenAsIs(_runtimeType, options) : alwaysWriteObjectTypeName == false && GetShape(itemType).IsPlainObject;
                _typed = direct ? GetTypedValue(itemType) : null;
                _typeInfo = direct ? GetTypeInfo(itemType, options) : null;
            }

            public void Write(Utf8JsonWriter writer, object item, JsonSerializerOptions options)
            {
                if (item == null)
                    writer.WriteNullValue();
                else if (_typed != null && item.GetType() == _runtimeType && _typed.TryWrite(writer, item, _typeInfo, options))
                    return;
                else
                    WriteValue(writer, item, _itemType, _alwaysWriteObjectTypeName, options);
            }
        }

        private readonly struct ItemReader
        {
            private readonly Type _itemType;

            private readonly TypedValue _typed;

            private readonly JsonTypeInfo _typeInfo;

            private readonly bool _isPlainObject;

            public ItemReader(Type itemType, JsonSerializerOptions options)
            {
                _itemType = itemType;
                _typed = GetTypedValue(itemType);
                _typeInfo = GetTypeInfo(itemType, options);
                _isPlainObject = GetShape(itemType).IsPlainObject;
            }

            public object Read(ref Utf8JsonReader reader, JsonSerializerOptions options)
            {
                switch (reader.TokenType)
                {
                    case JsonTokenType.Null:
                        return null;
                    case JsonTokenType.StartArray:
                        return TypeNameConverter.Read(ref reader, _itemType, options);
                    case JsonTokenType.StartObject:
                        if (_isPlainObject == false || TryPeekTypeName(reader, out _))
                            return TypeNameConverter.Read(ref reader, _itemType, options);
                        break;
                }

                return _typed.TryRead(ref reader, _typeInfo, options, out object value) ? value : TypeNameConverter.Read(ref reader, _itemType, options);
            }
        }

        private static bool TryGetDictionaryTypes(Type type, out Type keyType, out Type valueType)
        {
            TypeShape shape = GetShape(type);
            keyType = shape.KeyType;
            valueType = shape.ValueType;
            return shape.IsDictionary;
        }

        private static Type GetItemType(Type type) => GetShape(type).ItemType;

        private static Type GetCreatedType(Type declaredType) => GetShape(declaredType).CreatedType;

        private static bool ComputeDictionaryTypes(Type type, out Type keyType, out Type valueType)
        {
            Type dictionaryInterface = type.IsGenericType && (type.GetGenericTypeDefinition() == typeof(IDictionary<,>) || type.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>))
                ? type
                : type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>)) ??
                  type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>));

            if (dictionaryInterface == null)
            {
                if (typeof(IDictionary).IsAssignableFrom(type))
                {
                    keyType = valueType = typeof(object);
                    return true;
                }

                keyType = valueType = null;
                return false;
            }

            Type[] arguments = dictionaryInterface.GetGenericArguments();
            keyType = arguments[0];
            valueType = arguments[1];
            return true;
        }

        private static Type ComputeItemType(Type type)
        {
            if (type.IsArray)
                return type.GetElementType();

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                return type.GetGenericArguments()[0];

            Type enumerableInterface = type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
            return enumerableInterface?.GetGenericArguments()[0] ?? typeof(object);
        }

        private static string GetTypeName(Type type)
        {
            return _typeNames.GetOrAdd(type, static t =>
            {
                RavenSerializationBinder.Instance.ValidateType(t);
                return t.GetTypeNameForSerialization();
            });
        }

        private static Type ResolveType(string typeName, Type expectedType)
        {
            Type type = _types.GetOrAdd(typeName, static name => Type.GetType(name, throwOnError: true));
            RavenSerializationBinder.Instance.ValidateType(type);

            if (expectedType.IsAssignableFrom(type) == false)
                throw new JsonException($"Type specified in JSON '{type.AssemblyQualifiedName}' is not compatible with '{expectedType.AssemblyQualifiedName}'.");

            return type;
        }

        internal static object ReadItem(ref Utf8JsonReader reader, Type declaredType, TypeNameHandling typeNameHandling, JsonSerializerOptions options)
        {
            return typeNameHandling == TypeNameHandling.None
                ? JsonSerializer.Deserialize(ref reader, GetTypeInfo(declaredType, options))
                : Read(ref reader, declaredType, options);
        }

        internal static void WriteItem(Utf8JsonWriter writer, object item, Type declaredType, TypeNameHandling typeNameHandling, JsonSerializerOptions options)
        {
            if (item == null)
                writer.WriteNullValue();
            else if (typeNameHandling == TypeNameHandling.None)
                JsonSerializer.Serialize(writer, item, GetTypeInfo(item.GetType(), options));
            else
                WriteValue(writer, item, declaredType, typeNameHandling == TypeNameHandling.Objects, options);
        }

        internal static void WriteValue(Utf8JsonWriter writer, object value, Type declaredType, bool alwaysWriteObjectTypeName, JsonSerializerOptions options)
        {
            if (value == null)
            {
                writer.WriteNullValue();
                return;
            }

            Type runtimeType = value.GetType();
            if (IsWrittenAsIs(runtimeType, options))
            {
                WriteTyped(writer, value, runtimeType, options);
                return;
            }

            Type createdType = GetCreatedType(declaredType);
            if (createdType.IsArray && runtimeType.IsArray)
                runtimeType = createdType;

            bool writeTypeName = alwaysWriteObjectTypeName || runtimeType != createdType;

            if (TryGetDictionaryTypes(runtimeType, out Type keyType, out Type valueType))
            {
                if (value is IDictionary dictionary)
                    WriteDictionary(writer, dictionary, runtimeType, keyType, valueType, writeTypeName, alwaysWriteObjectTypeName, options);
                else
                    WriteObject(writer, value, runtimeType, writeTypeName, options);

                return;
            }

            if (value is IEnumerable enumerable)
            {
                WriteCollection(writer, enumerable, runtimeType, createdType, alwaysWriteObjectTypeName, options);
                return;
            }

            WriteObject(writer, value, runtimeType, writeTypeName, options);
        }

        private static void WriteDictionary(Utf8JsonWriter writer, IDictionary dictionary, Type runtimeType, Type keyType, Type valueType, bool writeTypeName, bool alwaysWriteObjectTypeName, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            if (writeTypeName)
                writer.WriteString(TypePropertyName, GetTypeName(runtimeType));

            var values = new ItemWriter(valueType, alwaysWriteObjectTypeName, options);
            DictionaryEntries entries = _dictionaryEntries.GetOrAdd((keyType, valueType), static key => (DictionaryEntries)Activator.CreateInstance(typeof(DictionaryEntries<,>).MakeGenericType(key.KeyType, key.ValueType)));
            if (entries.TryWrite(writer, dictionary, values, options) == false)
            {
                foreach (DictionaryEntry entry in dictionary)
                {
                    WriteKey(writer, entry.Key, keyType, options);
                    values.Write(writer, entry.Value, options);
                }
            }

            writer.WriteEndObject();
        }

        private static void WriteKey(Utf8JsonWriter writer, object key, Type keyType, JsonSerializerOptions options)
        {
            if (key is string text)
                writer.WritePropertyName(text);
            else if (keyType == typeof(object))
                writer.WritePropertyName(Convert.ToString(key, CultureInfo.InvariantCulture));
            else
                GetDictionaryKey(keyType).Write(writer, key, options);
        }

        private static object ReadKey(ref Utf8JsonReader reader, Type keyType, JsonSerializerOptions options)
        {
            if (keyType == typeof(string) || keyType == typeof(object))
                return reader.GetString();

            return GetDictionaryKey(keyType).Read(ref reader, options);
        }

        private static DictionaryKey GetDictionaryKey(Type keyType)
        {
            return _dictionaryKeys.GetOrAdd(keyType, static t => (DictionaryKey)Activator.CreateInstance(typeof(DictionaryKey<>).MakeGenericType(t)));
        }

        private abstract class DictionaryKey
        {
            public abstract void Write(Utf8JsonWriter writer, object key, JsonSerializerOptions options);

            public abstract object Read(ref Utf8JsonReader reader, JsonSerializerOptions options);
        }

        private sealed class DictionaryKey<TKey> : DictionaryKey
        {
            public override void Write(Utf8JsonWriter writer, object key, JsonSerializerOptions options) =>
                GetConverter(options).WriteAsPropertyName(writer, (TKey)key, options);

            public override object Read(ref Utf8JsonReader reader, JsonSerializerOptions options) =>
                GetConverter(options).ReadAsPropertyName(ref reader, typeof(TKey), options);

            private static JsonConverter<TKey> GetConverter(JsonSerializerOptions options) => (JsonConverter<TKey>)options.GetConverter(typeof(TKey));
        }

        private static Type ComputeCreatedType(Type declaredType)
        {
            if (declaredType.IsInterface == false)
                return declaredType;

            if (declaredType.IsGenericType)
            {
                Type definition = declaredType.GetGenericTypeDefinition();
                Type[] arguments = declaredType.GetGenericArguments();

                if (definition == typeof(IDictionary<,>) || definition == typeof(IReadOnlyDictionary<,>))
                    return typeof(Dictionary<,>).MakeGenericType(arguments);

                if (definition == typeof(ISet<>))
                    return typeof(HashSet<>).MakeGenericType(arguments);

                if (definition == typeof(IEnumerable<>) || definition == typeof(ICollection<>) || definition == typeof(IList<>) ||
                    definition == typeof(IReadOnlyCollection<>) || definition == typeof(IReadOnlyList<>))
                    return typeof(List<>).MakeGenericType(arguments);

                return declaredType;
            }

            if (declaredType == typeof(IDictionary))
                return typeof(Dictionary<object, object>);

            if (declaredType == typeof(IEnumerable) || declaredType == typeof(ICollection) || declaredType == typeof(IList))
                return typeof(List<object>);

            return declaredType;
        }

        private static void WriteCollection(Utf8JsonWriter writer, IEnumerable enumerable, Type runtimeType, Type createdType, bool alwaysWriteObjectTypeName, JsonSerializerOptions options)
        {
            Type itemType = GetItemType(runtimeType);
            bool wrapValues = itemType == typeof(object) && runtimeType != createdType;

            if (wrapValues)
            {
                writer.WriteStartObject();
                writer.WriteString(TypePropertyName, GetTypeName(runtimeType));
                writer.WritePropertyName(ValuesPropertyName);
            }

            var items = new ItemWriter(itemType, alwaysWriteObjectTypeName, options);
            writer.WriteStartArray();
            foreach (object item in enumerable)
                items.Write(writer, item, options);
            writer.WriteEndArray();

            if (wrapValues)
                writer.WriteEndObject();
        }

        private static void WriteObject(Utf8JsonWriter writer, object value, Type runtimeType, bool writeTypeName, JsonSerializerOptions options)
        {
            if (writeTypeName == false)
            {
                JsonSerializer.Serialize(writer, value, GetTypeInfo(runtimeType, options));
                return;
            }

            HashSet<object> writing = _writingWithTypeName ??= new HashSet<object>(ObjectReferenceEqualityComparer<object>.Default);
            if (writing.Add(value) == false)
                throw new JsonException($"Self referencing loop detected for type '{runtimeType.FullName}'.");

            if (HasTypeNameProperty(RavenJsonTypeInfoResolver.GetObjectTypeInfo(options, runtimeType)))
            {
                RavenSerializationBinder.Instance.ValidateType(runtimeType);
                object previous = _writingTypeNameFor;
                _writingTypeNameFor = value;
                try
                {
                    JsonSerializer.Serialize(writer, value, GetTypeInfo(runtimeType, options));
                }
                finally
                {
                    _writingTypeNameFor = previous;
                    writing.Remove(value);
                }

                return;
            }

            byte[] prefix = GetTypeNamePrefix(runtimeType);
            using (var buffer = new PooledByteBufferWriter(prefix.Length + 256))
            {
                buffer.Write(prefix);
                try
                {
                    using (var valueWriter = new Utf8JsonWriter(buffer, SystemTextJsonSerializationConventions.WriterOptions))
                        JsonSerializer.Serialize(valueWriter, value, GetTypeInfo(runtimeType, options));
                }
                finally
                {
                    writing.Remove(value);
                }

                Span<byte> json = buffer.WrittenSpan;
                Span<byte> body = json.Slice(prefix.Length);
                if (body.Length < 2 || body[0] != (byte)'{')
                {
                    writer.WriteRawValue(body, skipInputValidation: true);
                    return;
                }

                RavenSerializationBinder.Instance.ValidateType(runtimeType);

                if (body.Length == 2)
                {
                    body[0] = (byte)'}';
                    writer.WriteRawValue(json.Slice(0, prefix.Length + 1), skipInputValidation: true);
                    return;
                }

                body[0] = (byte)',';
                writer.WriteRawValue(json, skipInputValidation: true);
            }
        }

        private static byte[] GetTypeNamePrefix(Type type)
        {
            return _typeNamePrefixes.GetOrAdd(type, static t =>
            {
                using var stream = new MemoryStream();
                using (var typeWriter = new Utf8JsonWriter(stream, SystemTextJsonSerializationConventions.MetadataWriterOptions))
                {
                    typeWriter.WriteStartObject();
                    typeWriter.WriteString(TypePropertyName, t.GetTypeNameForSerialization());
                }

                return stream.ToArray();
            });
        }

        private static object Read(ref Utf8JsonReader reader, Type declaredType, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null)
                return null;

            if (reader.TokenType == JsonTokenType.StartObject)
            {
                if (TryReadTypedObject(ref reader, declaredType, options, out object typed))
                    return typed;

                if (TryGetDictionaryTypes(declaredType, out Type keyType, out Type valueType))
                    return ReadDictionary(ref reader, declaredType, keyType, valueType, options);

                return JsonSerializer.Deserialize(ref reader, GetTypeInfo(declaredType, options));
            }

            if (reader.TokenType == JsonTokenType.StartArray && declaredType != typeof(object))
                return ReadCollection(ref reader, declaredType, GetItemType(declaredType), options);

            return ReadTyped(ref reader, declaredType, options);
        }

        internal static bool TryReadTypedObject(ref Utf8JsonReader reader, Type declaredType, JsonSerializerOptions options, out object value)
        {
            if (reader.TokenType != JsonTokenType.StartObject || TryPeekTypeName(reader, out string typeName) == false)
            {
                value = null;
                return false;
            }

            Type type = ResolveType(typeName, declaredType);

            if (TryGetDictionaryTypes(type, out Type keyType, out Type valueType))
                value = ReadDictionary(ref reader, type, keyType, valueType, options);
            else if (type != typeof(string) && typeof(IEnumerable).IsAssignableFrom(type))
                value = ReadWrappedCollection(ref reader, type, options);
            else
            {
                value = JsonSerializer.Deserialize(ref reader, GetTypeInfo(type, options));
                RemoveTypeNameFromExtensionData(value, options);
            }

            return true;
        }

        private static void RemoveTypeNameFromExtensionData(object value, JsonSerializerOptions options)
        {
            if (value == null)
                return;

            JsonTypeInfo typeInfo = RavenJsonTypeInfoResolver.GetObjectTypeInfo(options, value.GetType());
            if (typeInfo.Kind != JsonTypeInfoKind.Object)
                return;

            foreach (JsonPropertyInfo property in typeInfo.Properties)
            {
                if (property.IsExtensionData == false || property.Get == null)
                    continue;

                switch (property.Get(value))
                {
                    case IDictionary<string, object> objects:
                        objects.Remove(TypePropertyName);
                        break;
                    case IDictionary<string, JsonElement> elements:
                        elements.Remove(TypePropertyName);
                        break;
                    case JsonObject node:
                        node.Remove(TypePropertyName);
                        break;
                }
            }
        }

        private sealed class DictionaryShape
        {
            public readonly Func<object> Create;

            public readonly PropertyInfo Indexer;

            public readonly Func<object, object> Finish;

            public DictionaryShape(Type dictionaryType, Type keyType, Type valueType)
            {
                Type createdType = GetCreatedType(dictionaryType);
                Type bufferType = typeof(Dictionary<,>).MakeGenericType(keyType, valueType);
                ConstructorInfo dictionaryConstructor = null;
                MethodInfo immutableFactory = GetImmutableFactory(createdType);

                if (immutableFactory != null || createdType.IsInterface || createdType.IsAbstract)
                {
                    createdType = bufferType;
                }
                else if (createdType.IsValueType == false && createdType.GetConstructor(Type.EmptyTypes) == null)
                {
                    dictionaryConstructor = createdType.GetConstructors()
                        .FirstOrDefault(c => c.GetParameters() is { Length: 1 } parameters && parameters[0].ParameterType.IsAssignableFrom(bufferType));
                    if (dictionaryConstructor != null)
                        createdType = bufferType;
                }

                Type instanceType = createdType;
                Create = instanceType.IsValueType == false && instanceType.GetConstructor(Type.EmptyTypes) is { } constructor
                    ? Expression.Lambda<Func<object>>(Expression.New(constructor)).Compile()
                    : () => Activator.CreateInstance(instanceType);
                Indexer = typeof(IDictionary).IsAssignableFrom(instanceType) ? null : typeof(IDictionary<,>).MakeGenericType(keyType, valueType).GetProperty("Item");

                if (immutableFactory != null)
                    Finish = dictionary => immutableFactory.Invoke(null, [dictionary]);
                else if (dictionaryConstructor != null)
                    Finish = dictionary => dictionaryConstructor.Invoke([dictionary]);
            }
        }

        private static object ReadDictionary(ref Utf8JsonReader reader, Type dictionaryType, Type keyType, Type valueType, JsonSerializerOptions options)
        {
            DictionaryShape shape = _dictionaryShapes.GetOrAdd(dictionaryType, t => new DictionaryShape(t, keyType, valueType));
            object dictionary = shape.Create();
            PropertyInfo indexer = shape.Indexer;

            var values = new ItemReader(valueType, options);
            bool leading = true;
            while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
            {
                bool isTypeName = leading && reader.ValueTextEquals(TypePropertyName);
                leading = false;

                object key = isTypeName ? null : ReadKey(ref reader, keyType, options);
                reader.Read();

                if (isTypeName)
                    continue;

                object value = values.Read(ref reader, options);
                if (dictionary is IDictionary nonGeneric)
                    nonGeneric[key] = value;
                else
                    indexer.SetValue(dictionary, value, [key]);
            }

            return shape.Finish == null ? dictionary : shape.Finish(dictionary);
        }

        private static MethodInfo GetImmutableFactory(Type type)
        {
            return _immutableFactories.GetOrAdd(type, static t =>
            {
                if (t.IsGenericType == false)
                    return null;

                string definitionName = t.GetGenericTypeDefinition().FullName;
                if (ImmutableInterfaceImplementations.TryGetValue(definitionName, out string implementationName))
                    definitionName = implementationName;

                if (ImmutableFactories.TryGetValue(definitionName, out string factoryName) == false)
                    return null;

                Type[] arguments = t.GetGenericArguments();
                MethodInfo createRange = t.Assembly.GetType(factoryName)?
                    .GetMethods(BindingFlags.Public | BindingFlags.Static)
                    .FirstOrDefault(m => m.Name == "CreateRange" && m.IsGenericMethodDefinition && m.GetGenericArguments().Length == arguments.Length && m.GetParameters().Length == 1);

                return createRange?.MakeGenericMethod(arguments);
            });
        }

        private static object ReadWrappedCollection(ref Utf8JsonReader reader, Type collectionType, JsonSerializerOptions options)
        {
            reader.Read();
            reader.Read();
            reader.Read();
            if (reader.TokenType != JsonTokenType.PropertyName || reader.ValueTextEquals(ValuesPropertyName) == false)
                throw new JsonException($"Expected '{ValuesPropertyName}' after '{TypePropertyName}' for collection type '{collectionType.FullName}'.");

            reader.Read();
            object collection = ReadCollection(ref reader, collectionType, GetItemType(collectionType), options);
            reader.Read();
            return collection;
        }

        private static object ReadCollection(ref Utf8JsonReader reader, Type collectionType, Type itemType, JsonSerializerOptions options)
        {
            IList items = _listFactories.GetOrAdd(itemType, static t => Expression.Lambda<Func<IList>>(Expression.New(typeof(List<>).MakeGenericType(t))).Compile())();

            var itemReader = new ItemReader(itemType, options);
            while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                items.Add(itemReader.Read(ref reader, options));

            return _collectionFactories.GetOrAdd(collectionType, t => CreateCollectionFactory(t, itemType))(items);
        }

        private static Func<IList, object> CreateCollectionFactory(Type collectionType, Type itemType)
        {
            if (collectionType.IsArray)
            {
                return items =>
                {
                    Array array = Array.CreateInstance(itemType, items.Count);
                    items.CopyTo(array, 0);
                    return array;
                };
            }

            Type listType = typeof(List<>).MakeGenericType(itemType);
            if (collectionType.IsAssignableFrom(listType))
                return static items => items;

            MethodInfo immutableFactory = GetImmutableFactory(collectionType);
            if (immutableFactory != null)
                return items => immutableFactory.Invoke(null, [items]);

            if (collectionType.IsInterface || collectionType.IsAbstract)
            {
                Type setType = typeof(HashSet<>).MakeGenericType(itemType);
                if (collectionType.IsAssignableFrom(setType))
                    return items => Activator.CreateInstance(setType, items);

                return _ => throw new JsonException($"Cannot create an instance of collection type '{collectionType.FullName}'.");
            }

            ConstructorInfo enumerableConstructor = collectionType.GetConstructors()
                .FirstOrDefault(c => c.GetParameters() is { Length: 1 } parameters && parameters[0].ParameterType.IsAssignableFrom(listType));
            if (enumerableConstructor != null)
                return items => enumerableConstructor.Invoke([items]);

            if (typeof(IList).IsAssignableFrom(collectionType))
            {
                return items =>
                {
                    var list = (IList)Activator.CreateInstance(collectionType);
                    foreach (object item in items)
                        list.Add(item);
                    return list;
                };
            }

            Type collectionInterface = typeof(ICollection<>).MakeGenericType(itemType);
            if (collectionInterface.IsAssignableFrom(collectionType) == false)
                return _ => throw new JsonException($"Cannot populate collection type '{collectionType.FullName}'.");

            MethodInfo add = collectionInterface.GetMethod("Add");
            return items =>
            {
                object collection = Activator.CreateInstance(collectionType);
                foreach (object item in items)
                    add.Invoke(collection, [item]);
                return collection;
            };
        }

        internal static bool TryPeekTypeName(Utf8JsonReader reader, out string typeName)
        {
            typeName = null;
            if (reader.Read() == false || reader.TokenType != JsonTokenType.PropertyName || reader.ValueTextEquals(TypePropertyName) == false)
                return false;

            if (reader.Read() == false || reader.TokenType != JsonTokenType.String)
                return false;

            typeName = reader.GetString();
            return true;
        }

        private sealed class DeclaredTypeConverter<T>(bool alwaysWriteObjectTypeName) : JsonConverter<T>
        {
            public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
                (T)TypeNameConverter.Read(ref reader, typeof(T), options);

            public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options) =>
                TypeNameConverter.WriteValue(writer, value, typeof(T), alwaysWriteObjectTypeName, options);
        }
    }
}
