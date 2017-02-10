using System.Collections.Generic;
using Raven.NewClient.Client.Replication.Messages;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.NewClient.Client.Exceptions
{
    public class DocumentConflictException : ConflictException
    {
        public string DocId { get; private set; }

        public IReadOnlyList<ChangeVectorEntry[]> Conflicts { get; }

        private DocumentConflictException(string message, string docId, List<ChangeVectorEntry[]> conflicts)
            : base(message)
        {
            DocId = docId;
            Conflicts = conflicts;
        }

        public DocumentConflictException(string docId, List<ChangeVectorEntry[]> conflicts)
            : this($"Conflict detected on '{docId}', conflict must be resolved before the document will be accessible.", docId, conflicts)
        {
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

            var conflicts = new List<ChangeVectorEntry[]>();
            BlittableJsonReaderArray conflictsArray;
            if (json.TryGet(nameof(Conflicts), out conflictsArray))
            {
                foreach (BlittableJsonReaderArray changeVectorsArray in conflictsArray)
                {
                    var changeVectors = new ChangeVectorEntry[changeVectorsArray.Length];
                    for (int i = 0; i < changeVectorsArray.Length; i++)
                    {
                        var changeVectorJson = (BlittableJsonReaderObject)changeVectorsArray[i];
                        var changeVector = new ChangeVectorEntry();

                        changeVectorJson.TryGet(nameof(ChangeVectorEntry.DbId), out changeVector.DbId);
                        changeVectorJson.TryGet(nameof(ChangeVectorEntry.Etag), out changeVector.Etag);

                        changeVectors[i] = changeVector;
                    }

                    conflicts.Add(changeVectors);
                }
            }

            return new DocumentConflictException(message, docId, conflicts);
        }
    }
}