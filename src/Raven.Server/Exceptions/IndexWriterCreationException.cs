using System;

namespace Raven.Server.Exceptions
{
    public class IndexWriterCreationException : CriticalIndexingException
    {
        public IndexWriterCreationException(Exception e) : base(e)
        {
        }
    }
}