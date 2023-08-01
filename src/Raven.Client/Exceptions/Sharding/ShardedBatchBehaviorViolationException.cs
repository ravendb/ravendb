using System;

namespace Raven.Client.Exceptions.Sharding;

public sealed class ShardedBatchBehaviorViolationException : RavenException
{
    public ShardedBatchBehaviorViolationException()
    {
    }

    public ShardedBatchBehaviorViolationException(string message)
        : base(message)
    {
    }

    public ShardedBatchBehaviorViolationException(string message, Exception e)
        : base(message, e)
    {
    }
}
