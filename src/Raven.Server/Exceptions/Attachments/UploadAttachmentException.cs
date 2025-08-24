using System;

namespace Raven.Server.Exceptions.Attachments
{
    [Serializable]
    internal sealed class UploadAttachmentException : Exception
    {
        public readonly string Identifier;
        public readonly string Key;

        public UploadAttachmentException()
        {
        }

        public UploadAttachmentException(string message) : base(message)
        {
        }

        public UploadAttachmentException(string identifier, string key, string message, Exception innerException) : base(message, innerException)
        {
            Identifier = identifier;
            Key = key;
        }
    }
}
