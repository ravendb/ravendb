using System;

namespace Voron.Exceptions
{
    public class VoronErrorException : Exception
    {
        public VoronErrorException()
        {
        }

        public VoronErrorException(string message) : base(message)
        {
        }

        public VoronErrorException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
