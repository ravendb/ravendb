using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_25240 : RavenTestBase
    {
        public RavenDB_25240(ITestOutputHelper output) : base(output)
        {
        }
        public class OutputSchema
        {
            public static OutputSchema Instance = new();
            public string Answer = "free text answer";
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task RunAsyncReturnsAnswerUsageAndTime(Options options, GenAiConfiguration cfg)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(cfg.Connection));

            var agent = new AiAgentConfiguration(
                name: "echo assistant",
                connectionStringName: cfg.ConnectionStringName,
                systemPrompt:
                    "You are a concise assistant. Always respond with a short, plain sentence in the 'Answer' field."
            );
            var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

            var conv = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());
            conv.SetUserPrompt("Say hello briefly.");
            var result = await conv.RunAsync<OutputSchema>(CancellationToken.None);

            Assert.NotNull(result);
            Assert.NotNull(result.Answer);   
            Assert.False(string.IsNullOrWhiteSpace(result.Answer.Answer));

            Assert.NotNull(result.Usage);
            Assert.True(result.Usage.TotalTokens > 0);
            Assert.True(result.Elapsed != TimeSpan.Zero);
            Assert.Equal(AiConversationResult.Done, result.Status);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task StreamAsyncReturnsUsageTime(Options options, GenAiConfiguration cfg)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(cfg.Connection));

            var agent = new AiAgentConfiguration(
                "echo streaming",
                cfg.ConnectionStringName,
                "You respond briefly."
            );

            var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

            var streamed = "";
            var conv = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());
            conv.SetUserPrompt("Stream a short hello.");

            var r = await conv.StreamAsync<OutputSchema>(
                streamPropertyPath: (OutputSchema o) => o.Answer,
                streamedChunksCallback: chunk => { streamed += chunk; return Task.CompletedTask; },
                token: CancellationToken.None
            );

            Assert.False(string.IsNullOrWhiteSpace(streamed));

            Assert.NotNull(r.Usage);
            Assert.True(r.Usage.TotalTokens >= 0);
            Assert.True(r.Elapsed != TimeSpan.Zero);
            Assert.Equal(AiConversationResult.Done, r.Status);
        }
    }
}
