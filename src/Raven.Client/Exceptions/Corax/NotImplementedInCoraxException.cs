using System;

namespace Raven.Client.Exceptions.Corax;

public sealed class NotImplementedInCoraxException : RavenException
{
    public NotImplementedInCoraxException()
    {
    }

    public NotImplementedInCoraxException(string message)
        : base(message)
    {
    }

    public NotImplementedInCoraxException(string message, Exception e)
        : base(message, e)
    {
    }
}
