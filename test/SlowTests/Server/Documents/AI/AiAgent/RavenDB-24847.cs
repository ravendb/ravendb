using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions.Documents.Attachments;
using Raven.Server.Documents.Handlers.AI.Agents;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_24847 : RavenTestBase
    {
        public RavenDB_24847(ITestOutputHelper output) : base(output)
        {
        }

        public class OutputSchema
        {
            public string Answer { get; set; } = "answer form the llm";
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanAnalyzeImages(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("image-analyzer", config.ConnectionStringName,
                "You are my friend have a chat with me");
            agent.Identifier = "image-analyzer";

            await store.AI.CreateAgentAsync(agent, new OutputSchema());

            var chat = store.AI.Conversation(agent.Identifier, "chats/", new AiConversationCreationOptions(), debug: true);
            chat.SetUserPrompt("what are inside the images I sent you? what are their colors?");

            AiAnswer<OutputSchema> result;

            await using (var banana = GetEmbeddedImgStream("banana.png"))
            await using (var star = GetEmbeddedImgStream("star.png"))
            await using (var heart = GetEmbeddedImgStream("heart.png"))
            {
                chat.AddAttachment("banana.png", banana, "image/png");
                chat.AddAttachment("star.png", star, "image/png");
                chat.AddAttachment("heart.png", heart, "image/png");

                result = await chat.RunAsync<OutputSchema>(CancellationToken.None);
            }

            Assert.Equal(AiConversationResult.Done, result.Status);
            Assert.NotNull(result.Answer);
            Assert.NotNull(result.Answer.Answer);

            var traces = await LoadDebugTracesAsync(store, chat.Id);
            Assert.True(result.Answer.Answer.Contains("banana", StringComparison.OrdinalIgnoreCase),
                BuildAssertMessage("banana", result.Answer.Answer, traces));
            Assert.True(result.Answer.Answer.Contains("star", StringComparison.OrdinalIgnoreCase),
                BuildAssertMessage("star", result.Answer.Answer, traces));
            Assert.True(result.Answer.Answer.Contains("heart", StringComparison.OrdinalIgnoreCase),
                BuildAssertMessage("heart", result.Answer.Answer, traces));
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanAnalyzeImageFromCopiedAttachment(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("image-analyzer", config.ConnectionStringName,
                "You are my friend have a chat with me");
            agent.Identifier = "image-analyzer";
            agent.Parameters.Add(new AiAgentParameter("company", "The company ID"));

            await store.AI.CreateAgentAsync(agent, new OutputSchema());

            string sourceDocId = "docs/1";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new { Info = "Source Doc" }, sourceDocId);
                await session.SaveChangesAsync();
            }
            List<string> names = ["banana.png","star.png","heart.png"];
            foreach (var name in names)
            {
                using (var stream = GetEmbeddedImgStream(name))
                {
                    await store.Operations.SendAsync(new PutAttachmentOperation(sourceDocId, name, stream, "image/png"));
                }
            }
            var chat = store.AI.Conversation(agent.Identifier, "chats/", new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));

            chat.SetUserPrompt("What is in this image?");
            chat.CopyAttachmentFrom(sourceDocId, "heart.png");
            await chat.RunAsync<OutputSchema>(CancellationToken.None);
            chat.AddUserPrompt("what images do you have uploaded?");
            var result = await chat.RunAsync<OutputSchema>(CancellationToken.None);
            Assert.Contains("heart", result.Answer.Answer);
            Assert.Equal(AiConversationResult.Done, result.Status);
            Assert.NotNull(result.Answer);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CopiedAttachmentWithMissingAttachments(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("image-analyzer", config.ConnectionStringName,
                "You are my friend have a chat with me");
            agent.Identifier = "image-analyzer";
            agent.Parameters.Add(new AiAgentParameter("company", "The company ID"));

            await store.AI.CreateAgentAsync(agent, new OutputSchema());

            string sourceDocId = "docs/1";

            var chat = store.AI.Conversation(agent.Identifier, "chats/", new AiConversationCreationOptions().AddParameter("company", "companies/90-A"));

            chat.SetUserPrompt("What is in this image?");
            chat.CopyAttachmentFrom(sourceDocId, "hearts.png");
            await Assert.ThrowsAsync<AttachmentDoesNotExistException>(() => chat.RunAsync<OutputSchema>(CancellationToken.None));
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanHandleInternalAndExternalActionsCalledTogether(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("mixed-tool-agent", config.ConnectionStringName,
                "You are a helpful assistant. Before answering about an image, you should still try to describe the image using your vision capabilities.");

            agent.Identifier = "mixed-tool-agent";
            agent.Actions =
            [
                new AiAgentToolAction
                {
                    Name = "CheckImageMetadata",
                    Description = "Checks external metadata for an image file.",
                    ParametersSampleObject = "{\"filename\": \"string\"}"
                }
            ];

            await store.AI.CreateAgentAsync(agent, new OutputSchema());

            var chat = store.AI.Conversation(agent.Identifier, "chats/", new AiConversationCreationOptions());

            chat.SetUserPrompt("use the tools and describe the image and it's metadata");

            AiAnswer<OutputSchema> result;
            chat.Handle<object>("CheckImageMetadata", args => "{}");

            await using (var banana = GetEmbeddedImgStream("banana.png"))
            {
                chat.AddAttachment("banana.png", banana, "image/png");

                await chat.RunAsync<OutputSchema>(CancellationToken.None);
                chat.SetUserPrompt("load the attachment I sent you, which fruit is inside of it?");

                result = await chat.RunAsync<OutputSchema>(CancellationToken.None);
            }

            Assert.Equal(AiConversationResult.Done, result.Status);
            Assert.Contains("banana", result.Answer.Answer);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanSendAttachmentWithoutPromptAndRecallItLater(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("attachment-only", config.ConnectionStringName,
                "You are a helpful assistant. When the user asks about an image, describe it.")
            {
                Identifier = "attachment-only"
            };

            await store.AI.CreateAgentAsync(agent, new OutputSchema());

            var chat = store.AI.Conversation(agent.Identifier, "chats/", new AiConversationCreationOptions());

            await using (var banana = GetEmbeddedImgStream("banana.png"))
            {
                chat.AddAttachment("banana.png", banana, "image/png");

                await chat.RunAsync<OutputSchema>(CancellationToken.None);
                chat.SetUserPrompt("What is inside the image I previously uploaded?");
                var result = await chat.RunAsync<OutputSchema>(CancellationToken.None);

                Assert.Equal(AiConversationResult.Done, result.Status);
                Assert.NotNull(result.Answer);
            }
        }


        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanRecallAttachmentInSubsequentTurn(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("memory-agent", config.ConnectionStringName,
                "You are a helpful assistant. You can see attachments.")
            {
                Identifier = "memory-agent"
            };

            await store.AI.CreateAgentAsync(agent, new OutputSchema());

            var chat = store.AI.Conversation(agent.Identifier, "chats/memory", new AiConversationCreationOptions());
            var fileName = "banana.png";

            AiAnswer<OutputSchema> result1;

            chat.SetUserPrompt("I am sending you a file.");
            await using (var banana = GetEmbeddedImgStream(fileName))
            {
                chat.AddAttachment(fileName, banana, "image/png");
                result1 = await chat.RunAsync<OutputSchema>(CancellationToken.None);
            }

            Assert.Equal(AiConversationResult.Done, result1.Status);

            chat.SetUserPrompt("What does the image I sent you earlier contains?");

            var result2 = await chat.RunAsync<OutputSchema>(CancellationToken.None);
            Assert.Equal(AiConversationResult.Done, result2.Status);
            Assert.Contains("banana", result2.Answer.Answer.ToLowerInvariant());
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task MultipartUserPromptArrayWithAttachments(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("multipart-prompt", config.ConnectionStringName,
                "You are a helpful assistant.")
            {
                Identifier = "multipart-prompt"
            };

            await store.AI.CreateAgentAsync(agent, new OutputSchema());

            var chat = store.AI.Conversation(agent.Identifier, "chats/", new AiConversationCreationOptions(), debug: true);

            chat.AddUserPrompt(new[] { "Please describe it.", "can you give tags related to it?" });

            AiAnswer<OutputSchema> result1;

            await using (var banana = GetEmbeddedImgStream("banana.png"))
            {
                chat.AddAttachment("banana.png", banana, "image/png");
                result1 = await chat.RunAsync<OutputSchema>(CancellationToken.None);
            }

            Assert.Equal(AiConversationResult.Done, result1.Status);
            Assert.NotNull(result1.Answer);

            chat.SetUserPrompt("What is in the image I uploaded?");
            var result2 = await chat.RunAsync<OutputSchema>(CancellationToken.None);

            Assert.Equal(AiConversationResult.Done, result2.Status);
            Assert.NotNull(result2.Answer);
            Assert.NotNull(result2.Answer.Answer);

            var traces = await LoadDebugTracesAsync(store, chat.Id);
            Assert.True(result2.Answer.Answer.Contains("banana", StringComparison.OrdinalIgnoreCase),
                BuildAssertMessage("banana", result2.Answer.Answer, traces));
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

        internal static async Task<string> LoadDebugTracesAsync(IDocumentStore store, string conversationId)
        {
            using var session = store.OpenAsyncSession();
            var traces = (await session.Advanced.LoadStartingWithAsync<DebugTraceDoc>(
                $"{conversationId}/{AiDebugTrace.TraceSegment}/")).ToList();

            if (traces.Count == 0)
                return "(no debug traces persisted)";

            var sb = new StringBuilder();
            for (int i = 0; i < traces.Count; i++)
            {
                sb.AppendLine($"--- Trace #{i + 1} ---");
                sb.AppendLine($"RequestBody: {traces[i].RequestBody}");
                sb.AppendLine($"Response: {traces[i].Response}");
            }
            return sb.ToString();
        }

        internal static string BuildAssertMessage(string expected, string actual, string traces)
        {
            return $"Expected substring '{expected}' in answer.\nActual answer: {actual}\nDebug traces:\n{traces}";
        }

        internal class DebugTraceDoc
        {
            public string RequestBody { get; set; }
            public object Response { get; set; }
        }
    }
}
