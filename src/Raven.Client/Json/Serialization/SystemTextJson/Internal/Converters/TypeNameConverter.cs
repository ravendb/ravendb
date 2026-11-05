using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
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

        private static readonly ConcurrentDictionary<Type, DictionaryKey> _dictionaryKeys = new();

        [ThreadStatic]
        private static HashSet<object> _writingWithTypeName;

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

            Type converterType = typeNameHandling == TypeNameHandling.Objects
                ? typeof(AlwaysConverter<>).MakeGenericType(declaredType)
                : typeof(AutoConverter<>).MakeGenericType(declaredType);

            return (JsonConverter)Activator.CreateInstance(converterType);
        }

        private static bool IsCandidate(Type declaredType, JsonSerializerOptions options)
        {
            if (declaredType == typeof(object))
                return true;

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

        private static bool IsWrittenAsIs(Type type, JsonSerializerOptions options)
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

        private static bool TryGetDictionaryTypes(Type type, out Type keyType, out Type valueType)
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

        private static Type GetItemType(Type type)
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
                JsonSerializer.Serialize(writer, value, runtimeType, options);
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

            foreach (DictionaryEntry entry in dictionary)
            {
                WriteKey(writer, entry.Key, keyType, options);
                WriteValue(writer, entry.Value, valueType, alwaysWriteObjectTypeName, options);
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

        private static Type GetCreatedType(Type declaredType)
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

            writer.WriteStartArray();
            foreach (object item in enumerable)
                WriteValue(writer, item, itemType, alwaysWriteObjectTypeName, options);
            writer.WriteEndArray();

            if (wrapValues)
                writer.WriteEndObject();
        }

        private static void WriteObject(Utf8JsonWriter writer, object value, Type runtimeType, bool writeTypeName, JsonSerializerOptions options)
        {
            if (writeTypeName == false)
            {
                JsonSerializer.Serialize(writer, value, runtimeType, options);
                return;
            }

            HashSet<object> writing = _writingWithTypeName ??= new HashSet<object>(ObjectReferenceEqualityComparer<object>.Default);
            if (writing.Add(value) == false)
                throw new JsonException($"Self referencing loop detected for type '{runtimeType.FullName}'.");

            byte[] valueJson;
            try
            {
                using (var valueStream = new MemoryStream())
                {
                    using (var valueWriter = new Utf8JsonWriter(valueStream, SystemTextJsonSerializationConventions.WriterOptions))
                        JsonSerializer.Serialize(valueWriter, value, runtimeType, options);

                    valueJson = valueStream.ToArray();
                }
            }
            finally
            {
                writing.Remove(value);
            }

            if (valueJson.Length < 2 || valueJson[0] != (byte)'{')
            {
                writer.WriteRawValue(valueJson, skipInputValidation: true);
                return;
            }

            using (var stream = new MemoryStream())
            {
                using (var typeWriter = new Utf8JsonWriter(stream, SystemTextJsonSerializationConventions.MetadataWriterOptions))
                {
                    typeWriter.WriteStartObject();
                    typeWriter.WriteString(TypePropertyName, GetTypeName(runtimeType));
                }

                if (valueJson.Length > 2)
                {
                    stream.WriteByte((byte)',');
                    stream.Write(valueJson, 1, valueJson.Length - 1);
                }
                else
                {
                    stream.WriteByte((byte)'}');
                }

                writer.WriteRawValue(new ReadOnlySpan<byte>(stream.GetBuffer(), 0, (int)stream.Length), skipInputValidation: true);
            }
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

                return JsonSerializer.Deserialize(ref reader, declaredType, options);
            }

            if (reader.TokenType == JsonTokenType.StartArray && declaredType != typeof(object))
                return ReadCollection(ref reader, declaredType, GetItemType(declaredType), options);

            return JsonSerializer.Deserialize(ref reader, declaredType, options);
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
                value = JsonSerializer.Deserialize(ref reader, type, options);
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

        private static object ReadDictionary(ref Utf8JsonReader reader, Type dictionaryType, Type keyType, Type valueType, JsonSerializerOptions options)
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

            object dictionary = Activator.CreateInstance(createdType);

            PropertyInfo indexer = dictionary is IDictionary ? null : typeof(IDictionary<,>).MakeGenericType(keyType, valueType).GetProperty("Item");

            bool leading = true;
            while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
            {
                bool isTypeName = leading && reader.ValueTextEquals(TypePropertyName);
                leading = false;

                object key = isTypeName ? null : ReadKey(ref reader, keyType, options);
                reader.Read();

                if (isTypeName)
                    continue;

                object value = Read(ref reader, valueType, options);
                if (dictionary is IDictionary nonGeneric)
                    nonGeneric[key] = value;
                else
                    indexer.SetValue(dictionary, value, [key]);
            }

            if (immutableFactory != null)
                return immutableFactory.Invoke(null, [dictionary]);

            return dictionaryConstructor == null ? dictionary : dictionaryConstructor.Invoke([dictionary]);
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
            var items = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(itemType));

            while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                items.Add(Read(ref reader, itemType, options));

            return CreateCollection(collectionType, itemType, items);
        }

        private static object CreateCollection(Type collectionType, Type itemType, IList items)
        {
            if (collectionType.IsArray)
            {
                Array array = Array.CreateInstance(itemType, items.Count);
                items.CopyTo(array, 0);
                return array;
            }

            if (collectionType.IsAssignableFrom(items.GetType()))
                return items;

            MethodInfo immutableFactory = GetImmutableFactory(collectionType);
            if (immutableFactory != null)
                return immutableFactory.Invoke(null, [items]);

            if (collectionType.IsInterface || collectionType.IsAbstract)
            {
                Type setType = typeof(HashSet<>).MakeGenericType(itemType);
                if (collectionType.IsAssignableFrom(setType))
                    return Activator.CreateInstance(setType, items);

                throw new JsonException($"Cannot create an instance of collection type '{collectionType.FullName}'.");
            }

            ConstructorInfo enumerableConstructor = collectionType.GetConstructors()
                .FirstOrDefault(c => c.GetParameters() is { Length: 1 } parameters && parameters[0].ParameterType.IsAssignableFrom(items.GetType()));
            if (enumerableConstructor != null)
                return enumerableConstructor.Invoke([items]);

            object collection = Activator.CreateInstance(collectionType);
            if (collection is IList list)
            {
                foreach (object item in items)
                    list.Add(item);
                return collection;
            }

            Type collectionInterface = typeof(ICollection<>).MakeGenericType(itemType);
            if (collectionInterface.IsAssignableFrom(collectionType) == false)
                throw new JsonException($"Cannot populate collection type '{collectionType.FullName}'.");

            MethodInfo add = collectionInterface.GetMethod("Add");
            foreach (object item in items)
                add.Invoke(collection, [item]);

            return collection;
        }

        private static bool TryPeekTypeName(Utf8JsonReader reader, out string typeName)
        {
            typeName = null;
            if (reader.Read() == false || reader.TokenType != JsonTokenType.PropertyName || reader.ValueTextEquals(TypePropertyName) == false)
                return false;

            if (reader.Read() == false || reader.TokenType != JsonTokenType.String)
                return false;

            typeName = reader.GetString();
            return true;
        }

        private sealed class AutoConverter<T> : JsonConverter<T>
        {
            public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
                (T)TypeNameConverter.Read(ref reader, typeof(T), options);

            public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options) =>
                TypeNameConverter.WriteValue(writer, value, typeof(T), alwaysWriteObjectTypeName: false, options);
        }

        private sealed class AlwaysConverter<T> : JsonConverter<T>
        {
            public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
                (T)TypeNameConverter.Read(ref reader, typeof(T), options);

            public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options) =>
                TypeNameConverter.WriteValue(writer, value, typeof(T), alwaysWriteObjectTypeName: true, options);
        }
    }
}
