using System;
using System.Threading.Tasks;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public class AdminShardedSortersHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/admin/sorters", "PUT")]
    public async Task Put()
    {
        throw new NotSupportedException("Custom sorting is not supported in sharding as of yet");
    }
}
