using System;

namespace Raven.Server.Exceptions
{
    public class OverlappingGeneratedNewIdException : Exception
    {
        public OverlappingGeneratedNewIdException()
        {
        }

        public OverlappingGeneratedNewIdException(Exception e) : base(e.Message, e)
        {
        }

        public OverlappingGeneratedNewIdException(string message)
            : base(message)
        {
        }

        public OverlappingGeneratedNewIdException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
