using System;

namespace Raven.Server.Exceptions
{
    public class CriticalIndexingException : Exception
    {
        public CriticalIndexingException(Exception e)
            : base(e.Message, e)
        {
        }
    }
}