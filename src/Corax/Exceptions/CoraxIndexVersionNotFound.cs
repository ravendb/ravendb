using System;

namespace Corax.Exceptions;

public class CoraxIndexVersionNotFound : Exception
{
    public CoraxIndexVersionNotFound()
    {
    }

    public CoraxIndexVersionNotFound(string message)
        : base(message)
    {
    }

    public CoraxIndexVersionNotFound(string message, Exception inner)
        : base(message, inner)
    {
    }
}
