using System;
using System.Collections.Generic;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Streaming
{
    public static class ShardResultConverter
    {
        public static Document BlittableToDocumentConverter(BlittableJsonReaderObject json)
        {
            var metadata = json.GetMetadata();
            return new Document
            {
                Data = json, 
                LowerId = metadata.GetIdAsLazyString(),
                LastModified = metadata.GetLastModified()
            };
        }

        public static Document BlittableToRevisionConverter(BlittableJsonReaderObject json)
        {
            var revision = new Document();

            if(json.TryGet(nameof(Document.Id), out revision.Id) == false)
                throw new InvalidOperationException("Revision does not contain 'Id' field.");

            if (json.TryGet(nameof(Document.ChangeVector), out revision.ChangeVector) == false)
                throw new InvalidOperationException($"Revision of \"{revision.Id}\" does not contain 'ChangeVector' fields.");

            if (json.TryGet(nameof(Document.Etag), out revision.Etag) == false)
                throw new InvalidOperationException($"Revision of \"{revision.Id}\" and change vector '{revision.ChangeVector}' does not contain 'Etag' fields.");


            if (json.TryGet(nameof(Document.LastModified), out revision.LastModified) == false)
                throw new InvalidOperationException($"Revision of \"{revision.Id}\" and change vector '{revision.ChangeVector}' does not contain 'LastModified' fields.");

            if (json.TryGet(nameof(Document.Flags), out revision.Flags) == false)
                throw new InvalidOperationException($"Revision of \"{revision.Id}\" and change vector '{revision.ChangeVector}' does not contain 'Flags' fields.");

            return revision;
        }

        public static List<string> BlittableToStringListConverter(BlittableJsonReaderArray json)
        {
            var list = new List<string>();
            foreach (var lsv in json)
            {
                list.Add(lsv.ToString());
            }
            return list;
        }
    }
}
