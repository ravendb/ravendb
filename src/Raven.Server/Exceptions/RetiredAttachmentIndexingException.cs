using System;

namespace Raven.Server.Exceptions
{
    public sealed class RetiredAttachmentIndexingException : CriticalIndexingException
    {
        public RetiredAttachmentIndexingException(string message)
            : base(message, e: null)
        {
        }
    }
}
