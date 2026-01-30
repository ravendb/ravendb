using System;

namespace Raven.Server.Exceptions.Attachments
{
    [Serializable]
    public sealed class UploadAttachmentException : Exception
    {
        public UploadAttachmentException()
        {
        }

        public UploadAttachmentException(string message) : base(message)
        {
        }

        public UploadAttachmentException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
