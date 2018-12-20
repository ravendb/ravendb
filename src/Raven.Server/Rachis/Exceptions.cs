using System;

namespace Raven.Server.Rachis
{
    public abstract class RachisException : Exception
    {
        protected RachisException()
        {
        }

        protected RachisException(string message) : base(message)
        {
        }

        protected RachisException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }

    public class RachisInvalidOperationException : RachisException
    {
        public RachisInvalidOperationException()
        {
        }

        public RachisInvalidOperationException(string message) : base(message)
        {
        }

        public RachisInvalidOperationException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public static void Throw(string msg)
        {
            throw new RachisInvalidOperationException(msg);
        }
    }

    public class RachisTopologyChangeException : RachisException
    {
        protected RachisTopologyChangeException()
        {
        }

        protected RachisTopologyChangeException(string message) : base(message)
        {
        }

        protected RachisTopologyChangeException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public static void Throw(string msg)
        {
            throw new RachisTopologyChangeException(msg);
        }
    }

    public class RachisConcurrencyException : RachisException
    {
        public RachisConcurrencyException()
        {
        }

        public RachisConcurrencyException(string message) : base(message)
        {
        }

        public RachisConcurrencyException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public static void Throw(string msg)
        {
            throw new RachisConcurrencyException(msg);
        }
    }

    public class RachisApplyException : RachisException
    {
        public RachisApplyException()
        {
        }

        public RachisApplyException(string message) : base(message)
        {
        }

        public RachisApplyException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public static void Throw(string msg)
        {
            throw new RachisConcurrencyException(msg);
        }
    }
}
