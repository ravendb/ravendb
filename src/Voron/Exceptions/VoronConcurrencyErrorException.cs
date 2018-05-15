using System;

namespace Voron.Exceptions
{
    public class VoronConcurrencyErrorException : VoronErrorException
    {
        public VoronConcurrencyErrorException()
        {
        }

        public VoronConcurrencyErrorException(string message) : base(message)
        {
        }

        public VoronConcurrencyErrorException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
