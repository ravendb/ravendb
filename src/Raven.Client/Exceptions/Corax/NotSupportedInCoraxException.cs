using System;

namespace Raven.Client.Exceptions.Corax;

public class NotSupportedInCoraxException : RavenException
{
    public NotSupportedInCoraxException()
    {
    }

    public NotSupportedInCoraxException(string message)
        : base(message)
    {
    }

    public NotSupportedInCoraxException(string message, Exception e)
        : base(message, e)
    {
    }
}
