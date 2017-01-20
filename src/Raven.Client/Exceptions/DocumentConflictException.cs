using System.Collections.Generic;
using Raven.Client.Replication.Messages;
using Sparrow.Json.Parsing;

namespace Raven.Client.Exceptions
{
    public class DocumentConflictException : ConflictException
    {
        public string DocId { get; private set; }

        public IReadOnlyList<ChangeVectorEntry[]> Conflicts { get; }

        public DocumentConflictException(string docId, List<ChangeVectorEntry[]> conflicts)
            : base($"Conflict detected on {docId}, conflict must be resolved before the document will be accessible.")
        {
            DocId = docId;
            Conflicts = conflicts;
        }

        public DynamicJsonArray GetConflicts()
        {
            var result = new DynamicJsonArray();
            foreach (var entries in Conflicts)
            {
                var array = new DynamicJsonArray();
                foreach (var conflict in entries)
                {
                    array.Add(conflict.ToJson());
                }

                result.Add(array);
            }

            return result;
        }
    }
}