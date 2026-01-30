using System.Threading.Tasks;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public sealed class ShardedRemoteAttachmentHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/admin/attachments/remote/config", "GET")]
    public Task GetRemoteConfig()
    {
        throw new NotSupportedInShardingException("Remote attachments does not support sharding");
    }

    [RavenShardedAction("/databases/*/admin/attachments/remote/config", "PUT")]
    public Task AddRemoteConfig()
    {
        throw new NotSupportedInShardingException("Remote attachments does not support sharding");
    }
}

