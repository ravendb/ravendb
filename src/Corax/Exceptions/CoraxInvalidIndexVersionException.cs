using System;

namespace Corax.Exceptions;

public class CoraxInvalidIndexVersionException : Exception
{
    public CoraxInvalidIndexVersionException()
    {
    }

    public CoraxInvalidIndexVersionException(string message)
        : base(message)
    {
    }
    
    public CoraxInvalidIndexVersionException(string message, Exception inner)
        : base(message, inner)
    {
    }
}
