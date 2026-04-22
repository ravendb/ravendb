using System;
using System.IO;
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
    public class RavenDB_26426 : RavenTestBase
    {
        public RavenDB_26426(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanSendSameImageTwiceInSeparateTurns(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("image-agent", config.ConnectionStringName,
                "You are a helpful assistant that analyzes images.")
            {
                Identifier = "image-agent"
            };

            await store.AI.CreateAgentAsync(agent, new AiAgentBasics.OutputSchema());

            var chat = store.AI.Conversation(agent.Identifier, "chats/", new AiConversationCreationOptions());

            // First turn: send the image and ask what fruit it is
            AiAnswer<AiAgentBasics.OutputSchema> result1;
            await using (var banana1 = GetEmbeddedImgStream("banana.png"))
            {
                chat.AddAttachment("banana.png", banana1, "image/png");
                chat.SetUserPrompt("What fruit is in this image?");
                result1 = await chat.RunAsync<AiAgentBasics.OutputSchema>(CancellationToken.None);
            }

            Assert.Equal(AiConversationResult.Done, result1.Status);
            Assert.NotNull(result1.Answer);
            Assert.Contains("banana", result1.Answer.Answer, StringComparison.OrdinalIgnoreCase);

            // Second turn: re-send the same image via a fresh stream and ask a follow-up question
            AiAnswer<AiAgentBasics.OutputSchema> result2;
            await using (var banana2 = GetEmbeddedImgStream("banana.png"))
            {
                chat.AddAttachment("banana.png", banana2, "image/png");
                chat.SetUserPrompt("Look at this image again - what color is the fruit?");
                result2 = await chat.RunAsync<AiAgentBasics.OutputSchema>(CancellationToken.None);
            }

            Assert.Equal(AiConversationResult.Done, result2.Status);
            Assert.NotNull(result2.Answer);
            Assert.Contains("yellow", result2.Answer.Answer, StringComparison.OrdinalIgnoreCase);
        }


        private static Stream GetEmbeddedImgStream(string name)
        {
            var asm = typeof(RavenDB_24847).Assembly;
            var resourceName = "SlowTests.Data.RavenDB_24648." + name;

            var stream = asm.GetManifestResourceStream(resourceName);
            if (stream == null)
                throw new FileNotFoundException($"Embedded resource not found: {resourceName}");

            return stream;
        }
    }
}
