using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Replication.Messages;
using Raven.Server.Documents;
using Raven.Server.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Server.Utils
{
    public static class ReplicationUtils
    {
        public static ChangeVectorEntry[] MergeVectors(ChangeVectorEntry[] vectorA, ChangeVectorEntry[] vectorB)
        {
            var merged = new ChangeVectorEntry[Math.Max(vectorA.Length, vectorB.Length)];
            var inx = 0;
            foreach (var entryA in vectorA)
            {
                var etagA = entryA.Etag;
                var etagB = vectorB.FirstOrDefault(e => e.DbId == entryA.DbId).Etag;

                merged[inx++] = new ChangeVectorEntry
                {
                    DbId = entryA.DbId,
                    Etag = Math.Max(etagA,etagB)
                };
            }
            return merged;
        }

        public static DynamicJsonValue GetJsonForConflicts(string docId, IEnumerable<DocumentConflict> conflicts)
        {
            var conflictsArray = new DynamicJsonArray();
            foreach (var c in conflicts)
            {
                conflictsArray.Add(new DynamicJsonValue
                {
                    ["ChangeVector"] = c.ChangeVector.ToJson(),
                    ["Doc"] = c.Doc
                });
            }

            return new DynamicJsonValue
            {
                ["Message"] = "Conflict detected on " + docId + ", conflict must be resolved before the document will be accessible",
                ["DocId"] = docId,
                ["Conflics"] = conflictsArray
            };
        }

    }
}
