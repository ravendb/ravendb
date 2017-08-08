using System;

namespace Raven.Server.Exceptions
{
    public class IndexWriterCreationException : CriticalIndexingException
    {
        public string Field { get; }

        public IndexWriterCreationException(Exception e) : base(e)
        {
        }
        
        public IndexWriterCreationException(Exception e, string field) : base(e)
        {
            Field = field;
        }
    }
}