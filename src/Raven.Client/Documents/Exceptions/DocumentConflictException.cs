using System.IO;
using Raven.Client.Documents.Commands;
using Raven.Client.Exceptions;
using Sparrow.Json;

namespace Raven.Client.Documents.Exceptions
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
            return new DocumentConflictException(message, null,0);
        }

        public static DocumentConflictException From(BlittableJsonReaderObject json)
        {
            string docId;
            if (!json.TryGet(nameof(DocId), out docId))
                throw new InvalidDataException("Expected to find property named " + nameof(DocId) + " in the exception received from the server, but didn't find it. This is probably a bug and should be reported.");

            string message;
            if (!json.TryGet(nameof(Message), out message))
                throw new InvalidDataException("Expected to find property named " + nameof(Message) + " in the exception received from the server, but didn't find it. This is probably a bug and should be reported.");

            long etag;
            if (!json.TryGet(nameof(LargestEtag), out etag))
                throw new InvalidDataException("Expected to find property named " + nameof(LargestEtag) + " in the exception received from the server, but didn't find it. This is probably a bug and should be reported.");
            return new DocumentConflictException(message,docId,etag);
        }
    }
}