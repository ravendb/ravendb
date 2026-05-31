using System.Threading.Tasks;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers.AI.Agents
{
    public sealed class ShardedAiAgentHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/ai/agent", "PUT")]
        public Task AddOrModifyAiAgent()
        {
            throw new NotSupportedInShardingException("AI Agents for a sharded database are currently not supported");
        }


        [RavenShardedAction("/databases/*/admin/ai/agent", "DELETE")]
        public Task DeleteAiAgent()
        {
            throw new NotSupportedInShardingException("AI Agents for a sharded database are currently not supported");
        }

        [RavenShardedAction("/databases/*/admin/ai/agent", "GET")]
        public Task GetAiAgentConfiguration()
        {
            throw new NotSupportedInShardingException("AI Agents for a sharded database are currently not supported");
        }

        [RavenShardedAction("/databases/*/ai/agent", "POST")]
        public Task RunAiAgent()
        {
            throw new NotSupportedInShardingException("AI Agents for a sharded database are currently not supported");
        }

        [RavenShardedAction("/databases/*/ai/agent/test", "POST")]
        public Task AiAgentTest()
        {
            throw new NotSupportedInShardingException("AI Agents for a sharded database are currently not supported");
        }
        
        [RavenShardedAction("/databases/*/admin/ai/agent/generate-code", "GET")]
        public Task AiAgentGenerateCode()
        {
            throw new NotSupportedInShardingException("AI Agents for a sharded database are currently not supported");
        }

        [RavenShardedAction("/databases/*/ai/conversation/messages", "GET")]
        public Task GetConversationMessages()
        {
            throw new NotSupportedInShardingException("AI Agents for a sharded database are currently not supported");
        }
    }
}
