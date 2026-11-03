using System.Collections.Generic;
using Raven.Client.Util;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal;

internal abstract class BlittableJsonConverterWithMissingProperties : BlittableJsonConverterBase
{
    public Dictionary<object, Dictionary<object, object>> MissingProperties { get; set; }

    protected BlittableJsonConverterWithMissingProperties(ISerializationConventions conventions) : base(conventions)
    {
    }

    protected void RegisterMissingProperties(object o, string id, object value)
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
