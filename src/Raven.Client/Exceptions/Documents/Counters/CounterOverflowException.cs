using System;

namespace Raven.Client.Exceptions.Documents.Counters
{
    public class CounterOverflowException : RavenException
    {
        public CounterOverflowException()
        {
        }

        public CounterOverflowException(string message) : base(message)
        {
        }

        public CounterOverflowException(string message, Exception inner) : base(message, inner)
        {
        }

        internal static void ThrowFor(string docId, string counterName, long value, long delta, OverflowException inner)
        {
            throw new CounterOverflowException($"Could not increment counter '{counterName}' from document '{docId}' with value '{value}' by '{delta}'.", inner);
        }

        internal static void ThrowFor(string docId, string counterName, OverflowException inner)
        {
            throw new CounterOverflowException($"Overflow detected in counter '{counterName}' from document '{docId}'.", inner);
        }
    }
}
