using System;
using System.Buffers;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Text.Json.Serialization.Metadata;
using Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal;

internal abstract class BlittableJsonConverterWithMissingProperties : BlittableJsonConverterBase
{
    private static readonly byte[] MetadataKey = Encoding.UTF8.GetBytes(Constants.Documents.Metadata.Key);

    private static readonly byte[] TypePropertyName = Encoding.UTF8.GetBytes(TypeNameConverter.TypePropertyName);

    private static readonly byte[] ValuesPropertyName = Encoding.UTF8.GetBytes(TypeNameConverter.ValuesPropertyName);

    private static readonly ConcurrentDictionary<Type, ItemOrder> ItemOrders = new();

    public Dictionary<object, Dictionary<object, object>> MissingProperties { get; set; }

    protected BlittableJsonConverterWithMissingProperties(ISerializationConventions conventions) : base(conventions)
    {
    }

    protected void CaptureMissingProperties(object entity, Type entityType, BlittableJsonReaderObject json)
    {
        if (CaptureObject(entity, entityType, json.AsView()) && MissingProperties.ContainsKey(entity) == false)
            MissingProperties[entity] = new Dictionary<object, object>();
    }

    protected BlittableJsonReaderObject RestoreMissingProperties(object entity, BlittableJsonReaderObject document, JsonOperationContext context)
    {
        if (MissingProperties == null || MissingProperties.ContainsKey(entity) == false || RestoreObject(entity, document) == false)
            return document;

        using (document)
            return context.ReadObject(document, "restore/missingProperties");
    }

    private bool CaptureObject(object instance, Type type, in BlittableJsonReaderObject.ObjectView json)
    {
        MissingPropertiesShape shape = Conventions.GetMissingPropertiesShape(type);
        if (shape.IsObject == false)
            return false;

        bool captured = false;

        for (int i = 0; i < json.Count; i++)
        {
            ReadOnlySpan<byte> name = json.GetPropertyName(i, out BlittableJsonToken token, out int position);
            if (name.SequenceEqual(MetadataKey) || name.SequenceEqual(TypePropertyName))
                continue;

            if (shape.TryGetMember(name, out JsonPropertyInfo property, out bool canHoldMissingProperties) == false)
            {
                string propertyName = GetString(name);
                if (shape.Properties.TryGetValue(propertyName, out property) == false)
                {
                    if (shape.OwnsUnknownProperties == false)
                        captured |= RegisterMissingProperties(instance, propertyName, json.Owner.GetObject(token, position, out _));

                    continue;
                }

                canHoldMissingProperties = MissingPropertiesShape.CanHoldMissingProperties(property);
            }

            if (canHoldMissingProperties && IsContainer(token))
                captured |= CaptureValue(property.Get(instance), json.Owner, token, position);
        }

        return captured;
    }

    private static unsafe string GetString(ReadOnlySpan<byte> utf8)
    {
        if (utf8.IsEmpty)
            return string.Empty;

        fixed (byte* bytes = utf8)
            return Encoding.UTF8.GetString(bytes, utf8.Length);
    }

    private static unsafe int GetPropertyIndex(in BlittableJsonReaderObject.ObjectView json, string name)
    {
        const int StackBufferSize = 256;

        int maxSize = Encoding.UTF8.GetMaxByteCount(name.Length);
        byte[] rented = maxSize > StackBufferSize ? ArrayPool<byte>.Shared.Rent(maxSize) : null;
        Span<byte> buffer = rented ?? stackalloc byte[StackBufferSize];

        try
        {
            int size;
            fixed (char* chars = name)
            fixed (byte* bytes = buffer)
                size = Encoding.UTF8.GetBytes(chars, name.Length, bytes, buffer.Length);

            return json.GetPropertyIndex(buffer.Slice(0, size));
        }
        finally
        {
            if (rented != null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static bool IsContainer(BlittableJsonToken token)
    {
        return (token & BlittableJsonReaderBase.TypesMask) is BlittableJsonToken.StartObject or BlittableJsonToken.StartArray or BlittableJsonToken.EmbeddedBlittable;
    }

    private static bool CanHoldMissingProperties(object value)
    {
        return value != null && value is not string && value.GetType().IsValueType == false;
    }

    private bool CaptureValue(object value, BlittableJsonReaderObject owner, BlittableJsonToken token, int position)
    {
        if (value == null || value.GetType().IsValueType)
            return false;

        BlittableJsonReaderObject.ObjectView json;
        switch (token & BlittableJsonReaderBase.TypesMask)
        {
            case BlittableJsonToken.StartArray:
                return value is IEnumerable and not string && CaptureItems((IEnumerable)value, owner.GetArrayView(token, position));
            case BlittableJsonToken.StartObject:
                json = owner.GetObjectView(token, position);
                break;
            case BlittableJsonToken.EmbeddedBlittable:
                json = ((BlittableJsonReaderObject)owner.GetObject(token, position, out _)).AsView();
                break;
            default:
                return false;
        }

        if (value is IDictionary dictionary)
        {
            bool captured = false;
            foreach (DictionaryEntry entry in dictionary)
            {
                if (entry.Key is not string key || CanHoldMissingProperties(entry.Value) == false)
                    continue;

                int index = GetPropertyIndex(json, key);
                if (index == -1)
                    continue;

                json.GetPropertyName(index, out BlittableJsonToken entryToken, out int entryPosition);
                if (IsContainer(entryToken))
                    captured |= CaptureValue(entry.Value, json.Owner, entryToken, entryPosition);
            }

            return captured;
        }

        if (value is IEnumerable items and not string)
        {
            int index = json.GetPropertyIndex(ValuesPropertyName);
            if (index == -1)
                return false;

            json.GetPropertyName(index, out BlittableJsonToken valuesToken, out int valuesPosition);
            return (valuesToken & BlittableJsonReaderBase.TypesMask) == BlittableJsonToken.StartArray &&
                   CaptureItems(items, json.Owner.GetArrayView(valuesToken, valuesPosition));
        }

        return CaptureObject(value, value.GetType(), json);
    }

    private bool CaptureItems(IEnumerable items, in BlittableJsonReaderObject.ArrayView json)
    {
        Type type = items.GetType();
        if (ItemOrders.TryGetValue(type, out ItemOrder order) == false)
            order = ItemOrders.GetOrAdd(type, GetItemOrder(type));

        switch (order)
        {
            case ItemOrder.Unknown:
                return false;
            case ItemOrder.Reversed when items is not ICollection { Count: var count } || count != json.Count:
                return false;
            case ItemOrder.DocumentWhenSameCount when CountItems(items) != json.Count:
                return false;
        }

        bool captured = false;
        int index = 0;
        foreach (object item in items)
        {
            if (index >= json.Count)
                break;

            int jsonIndex = order == ItemOrder.Reversed ? json.Count - 1 - index : index;
            index++;

            BlittableJsonToken token = json.GetItem(jsonIndex, out int position);
            if (CanHoldMissingProperties(item) && IsContainer(token))
                captured |= CaptureValue(item, json.Owner, token, position);
        }

        return captured;
    }

    private static int CountItems(IEnumerable items)
    {
        int count = 0;
        foreach (object _ in items)
            count++;

        return count;
    }

    private static ItemOrder GetItemOrder(Type type)
    {
        if (type.IsArray || typeof(IList).IsAssignableFrom(type))
            return ItemOrder.Document;

        if (type == typeof(Stack))
            return ItemOrder.Reversed;

        if (type.IsGenericType)
        {
            Type definition = type.GetGenericTypeDefinition();
            if (definition == typeof(Stack<>) || definition == typeof(ConcurrentStack<>) || definition.FullName == "System.Collections.Immutable.ImmutableStack`1")
                return ItemOrder.Reversed;

            if (definition == typeof(HashSet<>))
                return ItemOrder.DocumentWhenSameCount;

            if (definition == typeof(ConcurrentBag<>))
                return ItemOrder.Unknown;
        }

        foreach (Type implemented in type.GetInterfaces())
        {
            if (implemented.IsGenericType && implemented.GetGenericTypeDefinition() == typeof(ISet<>))
                return ItemOrder.Unknown;
        }

        return ItemOrder.Document;
    }

    private enum ItemOrder
    {
        Document,
        DocumentWhenSameCount,
        Reversed,
        Unknown
    }

    private bool RestoreObject(object instance, BlittableJsonReaderObject json)
    {
        bool modified = false;
        if (MissingProperties.TryGetValue(instance, out Dictionary<object, object> missingProperties))
        {
            foreach (KeyValuePair<object, object> missingProperty in missingProperties)
            {
                var name = (string)missingProperty.Key;
                if (json.GetPropertyIndex(name) != -1)
                    continue;

                json.Modifications ??= new DynamicJsonValue(json);
                json.Modifications[name] = missingProperty.Value;
                modified = true;
            }
        }

        Type type = instance.GetType();
        if (IsObject(type) == false)
            return modified;

        JsonPropertyMap knownProperties = Conventions.GetJsonProperties(type);
        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

        for (int i = 0; i < json.Count; i++)
        {
            json.GetPropertyByIndex(i, ref propertyDetails);
            string propertyName = propertyDetails.Name;
            if (knownProperties.TryGetValue(propertyName, out JsonPropertyInfo property) == false || CanHoldMissingProperties(property, propertyDetails.Value) == false)
                continue;

            if (RestoreValue(property.Get(instance), propertyDetails.Value, out object restored) == false)
                continue;

            json.Modifications ??= new DynamicJsonValue(json);
            json.Modifications[propertyName] = restored;
            modified = true;
        }

        return modified;
    }

    private bool RestoreValue(object value, object json, out object restored)
    {
        restored = json;
        if (value == null || value.GetType().IsValueType)
            return false;

        switch (json)
        {
            case BlittableJsonReaderObject jsonObject when value is IDictionary dictionary:
                bool modified = false;
                foreach (DictionaryEntry entry in dictionary)
                {
                    if (entry.Key is not string key || jsonObject.TryGetMember(key, out object entryJson) == false || RestoreValue(entry.Value, entryJson, out object restoredEntry) == false)
                        continue;

                    jsonObject.Modifications ??= new DynamicJsonValue(jsonObject);
                    jsonObject.Modifications[key] = restoredEntry;
                    modified = true;
                }

                return modified;
            case BlittableJsonReaderObject jsonObject when value is IEnumerable items and not string:
                if (jsonObject.TryGet(TypeNameConverter.ValuesPropertyName, out BlittableJsonReaderArray values) == false || RestoreItems(items, values, out DynamicJsonArray restoredValues) == false)
                    return false;

                jsonObject.Modifications ??= new DynamicJsonValue(jsonObject);
                jsonObject.Modifications[TypeNameConverter.ValuesPropertyName] = restoredValues;
                return true;
            case BlittableJsonReaderObject jsonObject:
                return RestoreObject(value, jsonObject);
            case BlittableJsonReaderArray jsonArray when value is IEnumerable items and not string:
                if (RestoreItems(items, jsonArray, out DynamicJsonArray restoredItems) == false)
                    return false;

                restored = restoredItems;
                return true;
            default:
                return false;
        }
    }

    private bool RestoreItems(IEnumerable items, BlittableJsonReaderArray json, out DynamicJsonArray restored)
    {
        var values = new object[json.Length];
        bool modified = false;
        int index = 0;
        foreach (object item in items)
        {
            if (index >= json.Length)
                break;

            modified |= RestoreValue(item, json[index], out values[index]);
            index++;
        }

        for (; index < json.Length; index++)
            values[index] = json[index];

        restored = modified ? new DynamicJsonArray(values) : null;
        return modified;
    }

    private static bool CanHoldMissingProperties(JsonPropertyInfo property, object json)
    {
        return property.Get != null &&
               property.PropertyType.IsValueType == false &&
               property.PropertyType != typeof(string) &&
               json is BlittableJsonReaderObject or BlittableJsonReaderArray;
    }

    private bool IsObject(Type type)
    {
        return RavenJsonTypeInfoResolver.GetObjectTypeInfo(Conventions.Options, type).Kind == JsonTypeInfoKind.Object;
    }

    private bool RegisterMissingProperties(object o, string id, object value)
    {
        if (Conventions.Conventions.PreserveDocumentPropertiesNotFoundOnModel == false ||
            id == Constants.Documents.Metadata.Key)
            return false;

        MissingProperties ??= new Dictionary<object, Dictionary<object, object>>(ObjectReferenceEqualityComparer<object>.Default);

        if (MissingProperties.TryGetValue(o, out var dictionary) == false)
        {
            MissingProperties[o] = dictionary = new Dictionary<object, object>();
        }

        dictionary[id] = value;
        return true;
    }
}
