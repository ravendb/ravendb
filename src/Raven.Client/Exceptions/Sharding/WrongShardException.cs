using System;

namespace Raven.Client.Exceptions.Sharding;

public class WrongShardException : RavenException
{
    public WrongShardException()
    {
    }

    public WrongShardException(string message)
        : base(message)
    {
    }

    public WrongShardException(string message, Exception inner)
        : base(message, inner)
    {
    }
}
