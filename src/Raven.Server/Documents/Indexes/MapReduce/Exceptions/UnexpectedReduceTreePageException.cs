using System;

namespace Raven.Server.Documents.Indexes.MapReduce.Exceptions
{
    public class UnexpectedReduceTreePageException : Exception
    {
        public UnexpectedReduceTreePageException()
        {
        }

        public UnexpectedReduceTreePageException(string message) : base(message)
        {
        }

        public UnexpectedReduceTreePageException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
