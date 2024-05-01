using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Commands;
using Raven.Server.Documents.Sharding.Handlers;

namespace Raven.Server.Documents.Sharding.Operations;

internal readonly struct ShardedWaitForIndexNotificationOperation : IShardedOperation
{
    private readonly ShardedDatabaseRequestHandler _handler;
    private readonly long _index;

    public ShardedWaitForIndexNotificationOperation(ShardedDatabaseRequestHandler handler, long index)
    {
        _handler = handler;
        _index = index;
    }

    public HttpRequest HttpRequest => _handler.HttpContext.Request;

    public RavenCommand<object> CreateCommandForShard(int shardNumber) => new WaitForIndexNotificationCommand(_index);
}
