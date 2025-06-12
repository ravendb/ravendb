using System.Threading.Tasks;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public sealed class ShardedAiAgentHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/admin/ai/ai-agent/add", "PUT")]
    public Task AddOrModifyAiAgent() => throw new NotSupportedInShardingException("AI Agent is currently not supported in sharding. Please use the non-sharded endpoint for this operation.");


    [RavenShardedAction("/databases/*/admin/ai/ai-agent/delete", "DELETE")]
    public Task DeleteAiAgent() => throw new NotSupportedInShardingException("AI Agent is currently not supported in sharding. Please use the non-sharded endpoint for this operation.");
}
