using System;

namespace Raven.Server.Exceptions
{
    public class FeaturesAvailabilityException : Exception
    {
     
        public FeaturesAvailabilityException()
        {
        }

        public FeaturesAvailabilityException(string message) : base(message)
        {
        }

        public FeaturesAvailabilityException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
