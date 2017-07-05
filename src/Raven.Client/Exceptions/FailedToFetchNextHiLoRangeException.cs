using System;

namespace Raven.Client.Exceptions
{
    public class FailedToFetchNextHiLoRangeException  :Exception
    {
        public FailedToFetchNextHiLoRangeException()
        {
        }

        public FailedToFetchNextHiLoRangeException(string message) : base(message)
        {
        }

        public FailedToFetchNextHiLoRangeException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
