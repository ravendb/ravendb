using System;

namespace Raven.Server.Exceptions.ETL.QueueEtl
{
    public class QueueLoadException : Exception
    {
        public QueueLoadException()
        {
        }

        public QueueLoadException(string message)
            : base(message)
        {
        }

        public QueueLoadException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
