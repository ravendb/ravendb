using System;
using Raven.NewClient.Abstractions.Data;
using Sparrow.Json;

namespace Raven.NewClient.Extensions
{
    public static class BlittableJsonExtensions
    {
        public static BlittableJsonReaderObject GetMetadata(this BlittableJsonReaderObject document)
        {
            BlittableJsonReaderObject metadata;
            if (document.TryGet(Constants.Metadata.Key, out metadata) == false || metadata == null)
                throw new InvalidOperationException($"Document does not contain '{Constants.Metadata.Key}' field.");

            return metadata;
        }

        public static string GetId(this BlittableJsonReaderObject metadata)
        {
            string id;
            if (metadata.TryGet(Constants.Metadata.Id, out id) == false)
                throw new InvalidOperationException($"Metadata does not contain '{Constants.Metadata.Id}' field.");

            return id;
        }

        public static bool TryGetId(this BlittableJsonReaderObject metadata, out string id)
        {
            return metadata.TryGet(Constants.Metadata.Id, out id) && id != null;
        }
    }
}