using System;

namespace Voron.Exceptions
{
    public class PageCompressedException : Exception
    {

        public PageCompressedException()
        {
        }

        public PageCompressedException(string message) : base(message)
        {
        }

        public PageCompressedException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
