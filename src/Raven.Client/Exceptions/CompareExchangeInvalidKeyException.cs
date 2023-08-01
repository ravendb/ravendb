using System;

namespace Raven.Client.Exceptions;

public sealed class CompareExchangeInvalidKeyException : RavenException
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
