using System;

namespace Raven.Client.Exceptions;

public class RavenTimeoutException : RavenException
{
    public RavenTimeoutException()
    {
    }

    public RavenTimeoutException(string message)
        : base(message)
    {
    }

    public RavenTimeoutException(string message, Exception inner)
        : base(message, inner)
    {
    }

    public bool FailImmediately;
}
