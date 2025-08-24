namespace Raven.Server.Exceptions.Attachments
{
    public sealed class RetiredAttachmentIndexingException : CriticalIndexingException
    {
        public RetiredAttachmentIndexingException(string message)
            : base(message, e: null)
        {
        }
    }
}
