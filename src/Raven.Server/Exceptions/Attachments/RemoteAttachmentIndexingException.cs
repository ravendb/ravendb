namespace Raven.Server.Exceptions.Attachments
{
    public sealed class RemoteAttachmentIndexingException : CriticalIndexingException
    {
        public RemoteAttachmentIndexingException(string message)
            : base(message, e: null)
        {
        }
    }
}
