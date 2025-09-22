using System;

namespace Voron.Exceptions
{
    public sealed class InvalidJournalFlushRequestException : Exception
    {

        public InvalidJournalFlushRequestException()
        {
        }

        public InvalidJournalFlushRequestException(string message) : base(message)
        {
        }

        public InvalidJournalFlushRequestException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}