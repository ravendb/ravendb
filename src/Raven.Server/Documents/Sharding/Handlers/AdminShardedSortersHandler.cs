using System.Threading.Tasks;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public class AdminShardedSortersHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/admin/sorters", "PUT")]
    public async Task Put()
    {
        throw new NotSupportedInShardingException("Custom sorting is not supported in sharding as of yet");
    }

    [RavenShardedAction("/databases/*/admin/sorters", "DELETE")]
    public async Task Delete()
    {
        throw new NotSupportedInShardingException("Custom sorting is not supported in sharding as of yet");
    }
}
