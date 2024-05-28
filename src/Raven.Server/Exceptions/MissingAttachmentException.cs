using System;
using System.Runtime.Serialization;

namespace Raven.Server.Exceptions
{
    [Serializable]
    internal class MissingAttachmentException : Exception
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
    }
}
