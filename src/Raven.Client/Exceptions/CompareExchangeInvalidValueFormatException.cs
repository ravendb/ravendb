using System;

namespace Raven.Client.Exceptions;

public class CompareExchangeInvalidValueFormatException : RavenException
{
    public CompareExchangeInvalidValueFormatException(string message)
        : base(message)
    {
    }
    public CompareExchangeInvalidValueFormatException(string message, Exception inner)
        : base(message, inner)
    {
    }
}
