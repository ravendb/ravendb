using System;
using System.IO;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Client.Replication.Messages;
using Raven.Server.Documents.Replication;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server
{
    public static class BlittableExtensions
    {
        public static DateTime GetLastModified(this BlittableJsonReaderObject doc)
        {
            //"Raven-Last-Modified"
            BlittableJsonReaderObject metadata;
            if (!doc.TryGet(Constants.Metadata.Key, out metadata))
            {
                throw new InvalidDataException("A document missing @metadata section, cannot fetch 'last modified' value.");
            }

            string lastModifiedStringValue;
            if (!metadata.TryGet("Raven-Last-Modified", out lastModifiedStringValue))
            {
                throw new InvalidDataException("A document missing 'Raven-Last-Modified' in @metadata section, cannot fetch it's value.");
            }

            DateTime result;
            if (!DateTime.TryParse(lastModifiedStringValue, out result))
            {
                throw new InvalidDataException("Failed to parse 'Raven-Last-Modified' field from @metadata section. The value was: " + lastModifiedStringValue);
            }

            return result;
        }

        public static void PrepareForStorage(this BlittableJsonReaderObject doc)
        {
            DynamicJsonValue mutableMetadata;
            BlittableJsonReaderObject metadata;
            if (doc.TryGet(Constants.Metadata.Key, out metadata))
            {
                metadata.Modifications = mutableMetadata = new DynamicJsonValue(metadata);
            }
            else
            {
                doc.Modifications = new DynamicJsonValue(doc)
                {
                    [Constants.Metadata.Key] = mutableMetadata = new DynamicJsonValue()
                };
            }
            
            mutableMetadata["Raven-Last-Modified"] = SystemTime.UtcNow.GetDefaultRavenFormat(isUtc: true);
        }
    }
}
