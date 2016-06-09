using System;
using System.IO;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Server.Documents.Replication;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server
{
    public static class BlittableExtensions
    {
        public static void PrepareForStorage(this BlittableJsonReaderObject doc)
        {
            DynamicJsonValue mutableMetadata;
            BlittableJsonReaderObject metadata;
            if (doc.TryGet(Constants.Metadata, out metadata))
            {
                metadata.Modifications = mutableMetadata = new DynamicJsonValue(metadata);
            }
            else
            {
                doc.Modifications = new DynamicJsonValue(doc)
                {
                    [Constants.Metadata] = mutableMetadata = new DynamicJsonValue()
                };
            }

            mutableMetadata["Raven-Last-Modified"] = SystemTime.UtcNow.GetDefaultRavenFormat(isUtc: true);
        }


        public static string GetIdFromMetadata(this BlittableJsonReaderObject document)
        {
            string id;
            BlittableJsonReaderObject metadata;
            if (!document.TryGet(Constants.Metadata, out metadata) ||
                !metadata.TryGet(Constants.MetadataDocId, out id))
                return null;
            return id;
        }

        public static long GetEtag(this BlittableJsonReaderObject document)
        {
            long etag;
            BlittableJsonReaderObject metadata;
            if (!document.TryGet(Constants.Metadata, out metadata) ||
                !metadata.TryGet(Constants.MetadataEtagField, out etag))
                    return 0;
            return etag;
        }

        /// <summary>
        /// Extract enumerable of change vector from document's metadata
        /// </summary>
        /// <exception cref="InvalidDataException">Invalid data is encountered in the change vector.</exception>        
        public static ChangeVectorEntry[] EnumerateChangeVector(this BlittableJsonReaderObject document)
        {
            //TODO: do not forget to investigate a bug in here
            //(last result in the vector key seems corrupted)
            BlittableJsonReaderObject metadata;
            BlittableJsonReaderArray changeVector;
            if (document.TryGet(Constants.Metadata, out metadata) == false ||
                metadata.TryGet(Constants.Replication.DocumentChangeVector,
                out changeVector) == false)
            {
                return new ChangeVectorEntry[0];
            }

            var results = new ChangeVectorEntry[changeVector.Length];

            for (int inx = 0; inx < changeVector.Length; inx++)
            {
                if (changeVector[inx] == null)
                    throw new InvalidDataException("Encountered invalid data in change vector. Expected BlittableJsonReaderObject, but found null");

                var vectorEntry = changeVector[inx] as BlittableJsonReaderObject;
                if(vectorEntry == null)
                    throw new InvalidDataException($"Encountered invalid data in change vector. Expected BlittableJsonReaderObject, but found {changeVector[inx].GetType()}");

                var key = vectorEntry.GetPropertyByIndex(0);
                if(key.Item3 != BlittableJsonToken.String)
                    throw new InvalidDataException($"Encountered invalid data in extracting document change vector. Expected a string, but found {key.Item3}");

                var val = vectorEntry.GetPropertyByIndex(1);
                if(val.Item3 != BlittableJsonToken.Integer)
                    throw new InvalidDataException($"Encountered invalid data in extracting document change vector. Expected a number, but found {key.Item3}");

                results[inx] = new ChangeVectorEntry
                {
                    DbId = Guid.Parse(key.Item2.ToString()),
                    Etag = (long)val.Item2
                };
            }

            return results;
        }
    }
}
