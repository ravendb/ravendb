using System;
using System.Runtime.Serialization;

namespace Raven.Server.Exceptions
{
    [Serializable]
    internal sealed class MissingAttachmentException : Exception
    {
        public MissingAttachmentException()
        {
        }

        public MissingAttachmentException(string message) : base(message)
        {
        }

        public MissingAttachmentException(string message, Exception innerException) : base(message, innerException)
        {
        }

        private MissingAttachmentException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
