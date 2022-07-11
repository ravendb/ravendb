using System;

namespace Raven.Client.Exceptions.Corax;

public class NotImplementedInCoraxException : RavenException
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
