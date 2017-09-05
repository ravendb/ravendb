using System.IO;
using Raven.Client.Documents.Commands;
using Sparrow.Json;

namespace Raven.Client.Exceptions.Documents
{
    public class DocumentConflictException : ConflictException
    {
        public string DocId { get; }

        public long LargestEtag { get; }

        public DocumentConflictException(string message, string docId, long etag)
            : base(message)
        {
            DocId = docId;
            LargestEtag = etag;
        }

        public DocumentConflictException(GetConflictsResult conflicts)
            : this($"Conflict detected on '{conflicts.Id}', conflict must be resolved before the document will be accessible.", conflicts.Id, conflicts.LargestEtag)
        {
        }

        public static DocumentConflictException From(string message)
        {
            return new DocumentConflictException(message, null, 0);
        }

        public static DocumentConflictException From(BlittableJsonReaderObject json)
        {
            if (json.TryGet(nameof(DocId), out string docId) == false)
                throw new InvalidDataException("Expected to find property named " + nameof(DocId) + " in the exception received from the server, but didn't find it. This is probably a bug and should be reported.");

            if (json.TryGet(nameof(Message), out string message) == false)
                throw new InvalidDataException("Expected to find property named " + nameof(Message) + " in the exception received from the server, but didn't find it. This is probably a bug and should be reported.");

            if (json.TryGet(nameof(LargestEtag), out long etag) == false)
                throw new InvalidDataException("Expected to find property named " + nameof(LargestEtag) + " in the exception received from the server, but didn't find it. This is probably a bug and should be reported.");

            return new DocumentConflictException(message, docId, etag);
        }
    }
}
