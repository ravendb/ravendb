using System;

namespace Raven.Client.Exceptions;

public sealed class ControlCharactersAreNotAllowedException : RavenException
{
    public ControlCharactersAreNotAllowedException(string message)
        : base(message)
    {
    }
    public ControlCharactersAreNotAllowedException(string message, Exception inner)
        : base(message, inner)
    {
    }
}
