using System;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;

namespace Raven.Server.Documents.Sharding.Operations;

public readonly struct ShardedDropSubscriptionConnectionOperation : IShardedOperation
{
    private readonly HttpContext _httpContext;
    private readonly string _subscriptionName;

    public ShardedDropSubscriptionConnectionOperation(HttpContext httpContext, string subscriptionName)
    {
        _httpContext = httpContext;
        _subscriptionName = subscriptionName;
    }

    public HttpRequest HttpRequest => _httpContext.Request;

    public object Combine(Memory<object> results)
    {
        return null;
    }

    public RavenCommand<object> CreateCommandForShard(int shardNumber) => new DropSubscriptionConnectionCommand(_subscriptionName);
}
