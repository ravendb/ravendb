using System;

namespace Raven.Server.Exceptions
{
    public class CounterOverflowException : Exception
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
    }
}
