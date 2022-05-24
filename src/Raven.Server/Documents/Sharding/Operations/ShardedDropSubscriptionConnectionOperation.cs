using System;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;

namespace Raven.Server.Documents.Sharding.Operations;

public readonly struct ShardedDropSubscriptionConnectionOperation : IShardedOperation
{
    private readonly HttpContext _httpContext;
    private readonly string _subscriptionName;
    private readonly long? _subscriptionTaskId;
    private readonly string _workerId;

    public ShardedDropSubscriptionConnectionOperation(HttpContext httpContext, string subscriptionName, long? subscriptionTaskId, string workerId = null)
    {
        _httpContext = httpContext;
        _subscriptionName = subscriptionName;
        _subscriptionTaskId = subscriptionTaskId;
        _workerId = workerId;
    }

    public HttpRequest HttpRequest => _httpContext.Request;

    public object Combine(Memory<object> results)
    {
        return null;
    }

    public RavenCommand<object> CreateCommandForShard(int shardNumber)
    {
        return new DropSubscriptionConnectionCommand(_subscriptionName, _subscriptionTaskId, _workerId);
    }
}
