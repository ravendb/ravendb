using System.Threading.Tasks;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers.Admin;

public class ShardedAdminSortersHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/admin/sorters", "PUT")]
    public Task Put()
    {
        throw new NotSupportedInShardingException("Custom sorting is not supported in sharding as of yet");
    }

    [RavenShardedAction("/databases/*/admin/sorters", "DELETE")]
    public Task Delete()
    {
        throw new NotSupportedInShardingException("Custom sorting is not supported in sharding as of yet");
    }
}
