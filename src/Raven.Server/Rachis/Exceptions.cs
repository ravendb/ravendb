using System;
using System.Diagnostics.CodeAnalysis;

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

    public sealed class RachisInvalidOperationException : RachisException
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

        [DoesNotReturn]
        public static void Throw(string msg)
        {
            throw new RachisInvalidOperationException(msg);
        }
    }

    public sealed class RachisTopologyChangeException : RachisException
    {
        public RachisTopologyChangeException()
        {
        }

        public RachisTopologyChangeException(string message) : base(message)
        {
        }

        public RachisTopologyChangeException(string message, Exception innerException) : base(message, innerException)
        {
        }

        [DoesNotReturn]
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

        [DoesNotReturn]
        public static void Throw(string msg)
        {
            throw new RachisConcurrencyException(msg);
        }
    }

    public class TermValidationException : RachisConcurrencyException
    {
        public TermValidationException()
        {
        }

        public TermValidationException(string message) : base(message)
        {
        }

        public TermValidationException(string message, Exception innerException) : base(message, innerException)
        {
        }

        [DoesNotReturn]
        public static void Throw(string msg)
        {
            throw new TermValidationException(msg);
        }
    }

    public sealed class ParentStateChangedConcurrencyException : RachisConcurrencyException
    {
        public ParentStateChangedConcurrencyException()
        {
        }

        public ParentStateChangedConcurrencyException(string message) : base(message)
        {
        }

        public ParentStateChangedConcurrencyException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public new static void Throw(string msg)
        {
            throw new ParentStateChangedConcurrencyException(msg);
        }
    }

    public sealed class RachisApplyException : RachisException
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

        [DoesNotReturn]
        public static void Throw(string msg)
        {
            throw new RachisApplyException(msg);
        }
    }
}
