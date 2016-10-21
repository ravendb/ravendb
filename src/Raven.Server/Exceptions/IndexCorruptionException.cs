using System;

namespace Raven.Server.Exceptions
{
    public class IndexCorruptionException : Exception
    {
        public IndexCorruptionException(Exception e)
            : base(e.Message, e)
        {
        }
    }
}