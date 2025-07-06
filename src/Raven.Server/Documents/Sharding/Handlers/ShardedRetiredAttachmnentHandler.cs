using System.Threading.Tasks;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public sealed class ShardedRetiredAttachmentHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/admin/attachments/retire/config", "GET")]
    public Task GetRetireConfig()
    {
        throw new NotSupportedInShardingException("Retired attachments does not support sharding");
    }

    [RavenShardedAction("/databases/*/admin/attachments/retire/config", "PUT")]
    public Task AddRetireConfig()
    {
        throw new NotSupportedInShardingException("Retired attachments does not support sharding");
    }
}

