using System.Collections.Generic;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Exceptions;
using Raven.Client.Json.Converters;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Exceptions
{
    public class DocumentConflictException : ConflictException
    {
        public string DocId { get; private set; }

        public List<GetConflictsResult.Conflict> Conflicts { get; }

        private DocumentConflictException(string message, string docId, List<GetConflictsResult.Conflict> conflicts)
            : base(message)
        {
            DocId = docId;
            Conflicts = conflicts;
        }

        public DocumentConflictException(string docId, List<GetConflictsResult.Conflict> conflicts)
            : this($"Conflict detected on '{docId}', conflict must be resolved before the document will be accessible.", docId, conflicts)
        {
        }

        public DynamicJsonValue GetConflicts()
        {
            var array = new DynamicJsonArray();
            foreach (var conflict in Conflicts)
            {
                array.Add(new DynamicJsonValue
                {
                    [nameof(GetConflictsResult.Conflict.Key)] = conflict.Key,
                    [nameof(GetConflictsResult.Conflict.ChangeVector)] = conflict.ChangeVector.ToJson(),
                    [nameof(GetConflictsResult.Conflict.Doc)] = conflict.Doc
                });
            }

            return new DynamicJsonValue
            {
                [nameof(GetConflictsResult.Results)] = array
            }; 
        }

        public static DocumentConflictException From(string message)
        {
            return new DocumentConflictException(message, null, null);
        }

        public static DocumentConflictException From(BlittableJsonReaderObject json)
        {
            string message;
            json.TryGet("Message", out message);

            string docId;
            json.TryGet(nameof(DocId), out docId);

            var conflicts = new List<GetConflictsResult.Conflict>();
            BlittableJsonReaderObject conflictsObject;
            BlittableJsonReaderArray conflictsArray;
            if (json.TryGet(nameof(Conflicts), out conflictsObject) &&
                conflictsObject.TryGet("Results",out conflictsArray))
            {
                foreach (BlittableJsonReaderObject conflictObj in conflictsArray)
                {
                    var conflict = JsonDeserializationClient.DocumentConflict(conflictObj);
                    conflicts.Add(conflict);
                }
            }

            return new DocumentConflictException(message, docId, conflicts);
        }
    }
}