using System;

namespace Voron.Exceptions
{
    public class InvalidJournalException : Exception
    {
        public long Number { get; }

        public InvalidJournalException(long number, string path) : base($"No such journal '{path}'")
        {
            Number = number;
        }

        public InvalidJournalException(long number) : base($"No such journal '{number}'")
        {
            Number = number;
        }
    }
}
