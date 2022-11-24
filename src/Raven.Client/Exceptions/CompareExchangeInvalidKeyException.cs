using System;

namespace Raven.Client.Exceptions;

public class CompareExchangeInvalidKeyException : RavenException
{
    public CompareExchangeInvalidKeyException(string message)
        : base(message)
    {
    }
    public CompareExchangeInvalidKeyException(string message, Exception inner)
        : base(message, inner)
    {
    }
}
