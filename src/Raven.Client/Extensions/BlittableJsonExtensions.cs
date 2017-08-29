using System;
using Sparrow.Json;

namespace Raven.Client.Extensions
{
    internal static class BlittableJsonExtensions
    {
        public static BlittableJsonReaderObject GetMetadata(this BlittableJsonReaderObject document)
        {
            BlittableJsonReaderObject metadata;
            if (document.TryGet(Constants.Documents.Metadata.Key, out metadata) == false || metadata == null)
                throw new InvalidOperationException($"Document does not contain '{Constants.Documents.Metadata.Key}' field.");

            return metadata;
        }

        public static string GetId(this BlittableJsonReaderObject metadata)
        {
            string id;
            if (metadata.TryGet(Constants.Documents.Metadata.Id, out id) == false)
                throw new InvalidOperationException($"Metadata does not contain '{Constants.Documents.Metadata.Id}' field.");

            return id;
        }

        public static bool TryGetId(this BlittableJsonReaderObject metadata, out string id)
        {
            return metadata.TryGet(Constants.Documents.Metadata.Id, out id) && id != null;
        }

        public static bool TryGetConflict(this BlittableJsonReaderObject metadata, out bool conflict)
        {
            return metadata.TryGet(Constants.Documents.Metadata.Conflict, out conflict);
        }

        public static string GetChangeVector(BlittableJsonReaderObject metadata)
        {
            if (metadata.TryGet(Constants.Documents.Metadata.ChangeVector, out string changeVector) == false)
                InvalidMissingChangeVector();

            return changeVector;
        }

        public static DateTime GetLastModified(this BlittableJsonReaderObject metadata)
        {
            DateTime lastModified;
            if (metadata.TryGet(Constants.Documents.Metadata.LastModified, out lastModified) == false)
                InvalidMissingLastModified();

            return lastModified;
        }

        public static bool TryGetChangeVector(this BlittableJsonReaderObject metadata, out string changeVector)
        {
            object changeVectorAsObject;
            if (metadata.TryGetMember(Constants.Documents.Metadata.ChangeVector, out changeVectorAsObject) == false)
            {
                changeVector = null;
                return false;
            }

            changeVector = changeVectorAsObject as string;
            return true;
        }

        private static void InvalidMissingChangeVector()
        {
            throw new InvalidOperationException($"Metadata does not contain '{Constants.Documents.Metadata.ChangeVector}' field.");
        }

        private static void InvalidMissingLastModified()
        {
            throw new InvalidOperationException($"Metadata does not contain '{Constants.Documents.Metadata.LastModified}' field.");
        }
    }
}
