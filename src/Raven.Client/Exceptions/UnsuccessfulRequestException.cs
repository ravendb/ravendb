using System;

namespace Raven.Client.Exceptions
{
    public class UnsuccessfulRequestException : Exception
    {
        public UnsuccessfulRequestException(string msg)
            : base(msg + " Request to a server has failed.")
        {
        }

        public UnsuccessfulRequestException(string msg, Exception exception)
            : base(msg + " Request to a server has failed. Reason: " + exception.Message, exception)
        {
        }
    }
}
