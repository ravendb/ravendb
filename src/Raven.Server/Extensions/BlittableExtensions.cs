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
