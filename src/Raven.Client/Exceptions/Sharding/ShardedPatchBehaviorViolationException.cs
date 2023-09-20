using System;

namespace Raven.Client.Exceptions.Sharding;

public sealed class ShardedPatchBehaviorViolationException : RavenException
{
    public ShardedPatchBehaviorViolationException()
    {
    }

    public ShardedPatchBehaviorViolationException(string message)
        : base(message)
    {
    }

    public ShardedPatchBehaviorViolationException(string message, Exception e)
        : base(message, e)
    {
    }
}
