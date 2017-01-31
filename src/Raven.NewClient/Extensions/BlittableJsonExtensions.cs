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

        public static long GetEtag(this BlittableJsonReaderObject metadata)
        {
            long etag;
            if (metadata.TryGet(Constants.Metadata.Etag, out etag) == false)
                InvalidMissingEtag();

            return etag;
        }

        private static void InvalidMissingEtag()
        {
            throw new InvalidOperationException($"Metadata does not contain '{Constants.Metadata.Etag}' field.");
        }

        public static bool TryGetEtag(this BlittableJsonReaderObject metadata, out long etag)
        {
            object etagAsObject;
            if (metadata.TryGetMember(Constants.Metadata.Etag, out etagAsObject) == false)
            {
                etag = 0;
                return false;
            }

            if (etagAsObject is long)
            {
                etag = (long)etagAsObject;
                return true;
            }

            return long.TryParse(etagAsObject.ToString(), out etag);
        }
    }
}