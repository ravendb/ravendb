using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Database.Indexing
{
    public class IndexingBatch
    {
        public IndexingBatch(Etag highestEtagBeforeFiltering)
        {
            HighestEtagBeforeFiltering = highestEtagBeforeFiltering;
            Ids = new List<string>();
            Docs = new List<dynamic>();
            SkipDeleteFromIndex = new List<bool>();
            Collections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        public readonly List<string> Ids;
        public readonly List<dynamic> Docs;
        public readonly List<bool> SkipDeleteFromIndex;
        public DateTime? DateTime;
        public readonly Etag HighestEtagBeforeFiltering;
        public readonly HashSet<string> Collections;

        public void Add(JsonDocument doc, object asJson, bool skipDeleteFromIndex)
        {
            Ids.Add(doc.Key);
            Docs.Add(asJson);
            SkipDeleteFromIndex.Add(skipDeleteFromIndex);
            var entityName = doc.Metadata.Value<string>(Constants.RavenEntityName);
            if (entityName != null)
            {
                Collections.Add(entityName);
            }
        }
    }
}
