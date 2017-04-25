using System;
using System.Runtime.Serialization;

namespace Raven.Server.Exceptions
{
    public class IndexDisposingException : Exception
    {
     
        public IndexDisposingException()
        {
        }

        public IndexDisposingException(string message) : base(message)
        {
        }

        public IndexDisposingException(string message, Exception inner) : base(message, inner)
        {
        }
    }

    public class IndexOpenException : Exception
    {
        public IndexOpenException(string message, Exception e)
            : base(message, e)
        {
        }
    }
}