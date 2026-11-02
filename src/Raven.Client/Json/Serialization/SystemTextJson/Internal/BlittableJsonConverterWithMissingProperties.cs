using System;
using System.Collections;
using System.Collections.Generic;
using System.Text.Json.Serialization.Metadata;
using Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal;

internal abstract class BlittableJsonConverterWithMissingProperties : BlittableJsonConverterBase
{
    public Dictionary<object, Dictionary<object, object>> MissingProperties { get; set; }

    protected BlittableJsonConverterWithMissingProperties(ISerializationConventions conventions) : base(conventions)
    {
    }

    protected void CaptureMissingProperties(object entity, Type entityType, BlittableJsonReaderObject json)
    {
        if (CaptureObject(entity, entityType, json) && MissingProperties.ContainsKey(entity) == false)
            MissingProperties[entity] = new Dictionary<object, object>();
    }

    protected BlittableJsonReaderObject RestoreMissingProperties(object entity, BlittableJsonReaderObject document, JsonOperationContext context)
    {
        if (MissingProperties == null || MissingProperties.ContainsKey(entity) == false || RestoreObject(entity, document) == false)
            return document;

        using (document)
            return context.ReadObject(document, "restore/missingProperties");
    }

    private bool CaptureObject(object instance, Type type, BlittableJsonReaderObject json)
    {
        JsonTypeInfo typeInfo = Conventions.Options.GetTypeInfo(type);
        if (typeInfo.Kind != JsonTypeInfoKind.Object)
            return false;

        bool ownsUnknownProperties = RavenJsonTypeInfoResolver.OwnsUnknownProperties(typeInfo);
        Dictionary<string, JsonPropertyInfo> knownProperties = Conventions.GetJsonProperties(type);
        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
        bool captured = false;

        foreach (string propertyName in json.GetPropertyNames())
        {
            if (propertyName == Constants.Documents.Metadata.Key || propertyName == TypeNameConverter.TypePropertyName)
                continue;

            json.GetPropertyByIndex(json.GetPropertyIndex(propertyName), ref propertyDetails);

            if (knownProperties.TryGetValue(propertyName, out JsonPropertyInfo property) == false)
            {
                if (ownsUnknownProperties == false)
                    captured |= RegisterMissingProperties(instance, propertyName, propertyDetails.Value);

                continue;
            }

            if (property.Get != null)
                captured |= CaptureValue(property.Get(instance), propertyDetails.Value);
        }

        return captured;
    }

    private bool CaptureValue(object value, object json)
    {
        if (value == null || value.GetType().IsValueType)
            return false;

        switch (json)
        {
            case BlittableJsonReaderObject jsonObject when value is IDictionary dictionary:
                bool captured = false;
                foreach (DictionaryEntry entry in dictionary)
                {
                    if (entry.Key is string key && jsonObject.TryGetMember(key, out object entryJson))
                        captured |= CaptureValue(entry.Value, entryJson);
                }

                return captured;
            case BlittableJsonReaderObject jsonObject when value is IEnumerable items and not string:
                return jsonObject.TryGet(TypeNameConverter.ValuesPropertyName, out BlittableJsonReaderArray values) && CaptureItems(items, values);
            case BlittableJsonReaderObject jsonObject:
                return CaptureObject(value, value.GetType(), jsonObject);
            case BlittableJsonReaderArray jsonArray when value is IEnumerable items and not string:
                return CaptureItems(items, jsonArray);
            default:
                return false;
        }
    }

    private bool CaptureItems(IEnumerable items, BlittableJsonReaderArray json)
    {
        bool captured = false;
        int index = 0;
        foreach (object item in items)
        {
            if (index >= json.Length)
                break;

            captured |= CaptureValue(item, json[index++]);
        }

        return captured;
    }

    private bool RestoreObject(object instance, BlittableJsonReaderObject json)
    {
        bool modified = false;
        if (MissingProperties.TryGetValue(instance, out Dictionary<object, object> missingProperties) && missingProperties.Count > 0)
        {
            json.Modifications ??= new DynamicJsonValue(json);
            foreach (KeyValuePair<object, object> missingProperty in missingProperties)
                json.Modifications[(string)missingProperty.Key] = missingProperty.Value;

            modified = true;
        }

        Type type = instance.GetType();
        if (IsObject(type) == false)
            return modified;

        Dictionary<string, JsonPropertyInfo> knownProperties = Conventions.GetJsonProperties(type);
        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

        foreach (string propertyName in json.GetPropertyNames())
        {
            if (knownProperties.TryGetValue(propertyName, out JsonPropertyInfo property) == false || property.Get == null)
                continue;

            json.GetPropertyByIndex(json.GetPropertyIndex(propertyName), ref propertyDetails);
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

    private bool IsObject(Type type)
    {
        return Conventions.Options.GetTypeInfo(type).Kind == JsonTypeInfoKind.Object;
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
