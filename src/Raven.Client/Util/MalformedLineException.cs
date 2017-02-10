using System;

namespace Raven.Client.Util 
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

