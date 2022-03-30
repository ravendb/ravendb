using System;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;

namespace Raven.Server.Documents.Sharding.Operations;

public readonly struct ShardedDropSubscriptionConnectionOperation : IShardedOperation
{
    private readonly string _subscriptionName;
    private readonly string _workerId;

    public ShardedDropSubscriptionConnectionOperation(string subscriptionName, string workerId = null)
    {
        _subscriptionName = subscriptionName;
        _workerId = workerId;
    }

    public object Combine(Memory<object> results)
    {
        return null;
    }

    public RavenCommand<object> CreateCommandForShard(int shard)
    {
        if (string.IsNullOrEmpty(_workerId))
        {
            return new DropSubscriptionConnectionCommand(_subscriptionName);
        }

        return new DropSubscriptionConnectionCommand(_subscriptionName, _workerId);
    }
}
