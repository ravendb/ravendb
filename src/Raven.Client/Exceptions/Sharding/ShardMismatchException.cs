using System;

namespace Raven.Client.Exceptions.Sharding;

public class ShardMismatchException : RavenException
{
    public ShardMismatchException()
    {
    }

    public ShardMismatchException(string message)
        : base(message)
    {
    }

    public ShardMismatchException(string message, Exception inner)
        : base(message, inner)
    {
    }
}
