using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;

namespace Raven.Server.Documents.Sharding.Operations;

public readonly struct WaitForIndexNotificationOnServerOperation : IShardedOperation
{
    private readonly long _index;
    public WaitForIndexNotificationOnServerOperation(long index)
    {
        _index = index;
    }

    public HttpRequest HttpRequest => null;
    public RavenCommand<object> CreateCommandForShard(int shardNumber) => new WaitForRaftIndexCommand(_index);
}
