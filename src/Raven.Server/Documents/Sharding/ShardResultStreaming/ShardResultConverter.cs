using System.Collections.Generic;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.ShardResultStreaming
{
    public static class ShardResultConverter
    {
        public static Document BlittableToDocumentConverter(BlittableJsonReaderObject json)
        {
            var metadata = json.GetMetadata();
            return new Document
            {
                Data = json, 
                LowerId = metadata.GetIdAsLazyString()
            };
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
