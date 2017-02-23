using System;
using System.IO;
using System.Text;
using Raven.Client.Documents.Replication.Messages;
using Sparrow.Json;

namespace Raven.Client.Extensions
{
    internal static class BlittableJsonExtensions
    {
        public static ChangeVectorEntry[] ToVector(this BlittableJsonReaderArray vectorJson)
        {
            var result = new ChangeVectorEntry[vectorJson.Length];
            int iter = 0;
            foreach (BlittableJsonReaderObject entryJson in vectorJson)
            {
                if (!entryJson.TryGet(nameof(ChangeVectorEntry.DbId), out result[iter].DbId))
                    throw new InvalidDataException("Tried to find " + nameof(ChangeVectorEntry.DbId) + " property in change vector, but didn't find.");
                if (!entryJson.TryGet(nameof(ChangeVectorEntry.Etag), out result[iter].Etag))
                    throw new InvalidDataException("Tried to find "+ nameof(ChangeVectorEntry.Etag) + " property in change vector, but didn't find.");

                iter++;
            }
            return result;
        }

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

        public static long GetEtag(this BlittableJsonReaderObject metadata)
        {
            long etag;
            if (metadata.TryGet(Constants.Documents.Metadata.Etag, out etag) == false)
                InvalidMissingEtag();

            return etag;
        }

        public static DateTime GetLastModified(this BlittableJsonReaderObject metadata)
        {
            DateTime lastModified;
            if (metadata.TryGet(Constants.Documents.Metadata.LastModified, out lastModified) == false)
                InvalidMissingLastModified();

            return lastModified;
        }

        public static bool TryGetEtag(this BlittableJsonReaderObject metadata, out long etag)
        {
            object etagAsObject;
            if (metadata.TryGetMember(Constants.Documents.Metadata.Etag, out etagAsObject) == false)
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

        private static void InvalidMissingEtag()
        {
            throw new InvalidOperationException($"Metadata does not contain '{Constants.Documents.Metadata.Etag}' field.");
        }

        private static void InvalidMissingLastModified()
        {
            throw new InvalidOperationException($"Metadata does not contain '{Constants.Documents.Metadata.LastModified}' field.");
        }
    }
}