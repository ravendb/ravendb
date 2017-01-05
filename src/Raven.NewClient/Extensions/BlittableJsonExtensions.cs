using System;
using Raven.NewClient.Abstractions.Data;
using Sparrow.Json;

namespace Raven.NewClient.Extensions
{
    public static class BlittableJsonExtensions
    {
        public static BlittableJsonReaderObject GetMetadata(this BlittableJsonReaderObject document)
        {
            object metadataObj;
            if (document.TryGetMember(Constants.Metadata.Key, out metadataObj) == false)
                throw new InvalidOperationException($"Document does not contain '{Constants.Metadata.Key}' field.");

            var metadata = metadataObj as BlittableJsonReaderObject;
            if (metadata == null)
                throw new InvalidOperationException($"Field {Constants.Metadata.Key} is null or not an JSON object.");

            return metadata;
        }

        public static string GetId(this BlittableJsonReaderObject metadata)
        {
            string id;
            if (metadata.TryGet(Constants.Metadata.Id, out id) == false)
                throw new InvalidOperationException($"Metadata does not contain '{Constants.Metadata.Id}' field.");

            return id;
        }
    }
}