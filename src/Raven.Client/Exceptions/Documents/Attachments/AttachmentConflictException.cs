namespace Raven.Client.Exceptions.Documents.Attachments
{
    public class AttachmentConflictException : ConflictException
    {
        public string DocId { get; }

        public AttachmentConflictException(string message, string docId) : base(message)
        {
            DocId = docId;
        }
    }
}
