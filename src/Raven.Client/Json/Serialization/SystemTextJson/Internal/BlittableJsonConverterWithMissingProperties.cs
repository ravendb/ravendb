using System;
using System.Collections.Generic;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal;

internal abstract class BlittableJsonConverterWithMissingProperties : BlittableJsonConverterBase
{
    public Dictionary<object, Dictionary<object, object>> MissingProperties { get; set; }

    protected BlittableJsonConverterWithMissingProperties(ISerializationConventions conventions) : base(conventions)
    {
    }

    protected void CaptureMissingProperties(object entity, Type entityType, BlittableJsonReaderObject json)
    {
        var knownProperties = Conventions.GetJsonProperties(entityType);
        var propertyNames = json.GetPropertyNames();

        for (int i = 0; i < propertyNames.Length; i++)
        {
            var propName = propertyNames[i];

            if (propName == Constants.Documents.Metadata.Key)
                continue;

            if (knownProperties.ContainsKey(propName))
                continue;

            var propIndex = json.GetPropertyIndex(propName);
            var propDetails = new BlittableJsonReaderObject.PropertyDetails();
            json.GetPropertyByIndex(propIndex, ref propDetails);

            RegisterMissingProperties(entity, propName, propDetails.Value);
        }
    }

    private void RegisterMissingProperties(object o, string id, object value)
    {
        if (Conventions.Conventions.PreserveDocumentPropertiesNotFoundOnModel == false ||
            id == Constants.Documents.Metadata.Key)
            return;

        MissingProperties ??= new Dictionary<object, Dictionary<object, object>>(ObjectReferenceEqualityComparer<object>.Default);

        if (MissingProperties.TryGetValue(o, out var dictionary) == false)
        {
            MissingProperties[o] = dictionary = new Dictionary<object, object>();
        }

        dictionary[id] = value;
    }
}
