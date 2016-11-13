using System;

namespace Raven.Abstractions.Util 
{
    public class MalformedLineException : Exception 
    {
        public MalformedLineException()
        {
        }

        public MalformedLineException(string message) : base(message)
        {
        }

        public MalformedLineException(string message, Exception inner)
        {
        }
    }
}

