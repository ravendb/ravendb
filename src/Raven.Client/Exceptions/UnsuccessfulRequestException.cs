using System;

namespace Raven.Client.Exceptions
{
    public class UnsuccessfulRequestException : Exception
    {
        public UnsuccessfulRequestException(string msg, ExceptionDispatcher.ExceptionSchema exceptionInfo)
            : base(msg + " Request to a server has failed. Reason: " + exceptionInfo.Message)
        {
            ExceptionInfo = exceptionInfo;
        }

        public ExceptionDispatcher.ExceptionSchema ExceptionInfo { get; }
    }
}
