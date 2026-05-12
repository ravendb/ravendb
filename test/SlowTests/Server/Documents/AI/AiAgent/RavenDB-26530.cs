using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class RavenDB_26530 : ReplicationTestBase
{
    public RavenDB_26530(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanRouteConversationToEachSelectedNodeInClusterOf3(Options options, GenAiConfiguration config)
    {
        var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
        using var store = GetDocumentStore(new Options
        {
            Server = leader,
            ReplicationFactor = 3,
            Path = options?.Path,
            ModifyDatabaseRecord = options?.ModifyDatabaseRecord,
        });
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var agent = new AiAgentConfiguration("node-routing-agent", config.ConnectionStringName,
            "You are a brief assistant. Reply with a short greeting.");
        var agentId = (await store.AI.CreateAgentAsync(agent, SimpleAnswer.Instance)).Identifier;

        // Block document replication between nodes so every conversation document stays on
        // the node it was written to — its change vector then contains only that node's tag.
        using var r = await GetReplicationManagerAsync(store, store.Database, options.DatabaseMode, breakReplication: true, servers: nodes);

        foreach (var node in nodes)
        {
            var tag = node.ServerStore.NodeTag;

            var chat = store.AI.Conversation(agentId, $"chats/{tag}-",
                new AiConversationCreationOptions(),
                changeVector: null,
                nodeTag: tag);
            chat.SetUserPrompt("Say hello.");

            var result = await chat.RunAsync<SimpleAnswer>(CancellationToken.None);

            Assert.Equal(AiConversationResult.Done, result.Status);
            Assert.NotNull(result.Answer);
            Assert.NotNull(chat.Id);

            AssertChangeVectorHasOnlyTag(chat.ChangeVector, tag);
        }
        r.Mend();
    }

    private static void AssertChangeVectorHasOnlyTag(string changeVector, string expectedTag)
    {
        Assert.NotNull(changeVector);
        var entries = changeVector
            .Split(',')
            .Select(e => e.Trim())
            .Where(e => string.IsNullOrEmpty(e) == false)
            .ToList();

        Assert.Single(entries);
        Assert.StartsWith($"{expectedTag}:", entries[0]);
    }

    private class SimpleAnswer
    {
        public static SimpleAnswer Instance = new();
        public string Answer = "the answer to the user question";
    }
}
