using System.Collections.Generic;
using Raven.Server.Documents;
using Raven.Server.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Server.Utils
{
    public static class ReplicationUtils
    {
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
