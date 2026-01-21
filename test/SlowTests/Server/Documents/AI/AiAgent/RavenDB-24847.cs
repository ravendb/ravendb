using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

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
            public string UploadedAttachments { get; set; } = "the uploaded attachments you got from the user";
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanAnalyzeImageAndPersistStructuredData(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("image-analyzer", config.ConnectionStringName,
                "You are my friend have a chat with me");
            agent.Identifier = "image-analyzer";

            await store.AI.CreateAgentAsync(agent, new OutputSchema());

            var chat = store.AI.Conversation(agent.Identifier, "chats/", new AiConversationCreationOptions());
            List<string> names = ["banana.png", "star.png", "heart.png"];
            chat.SetUserPrompt("what are inside the images I sent you? what are their colors?");
            AiAnswer<OutputSchema> result = null;
            foreach (var name in names)
            {
                chat.AddAttachment(name, GetEmbeddedImgStream(name));
            }
            result = await chat.RunAsync<OutputSchema>(CancellationToken.None);
         
            Assert.Equal(AiConversationResult.Done, result?.Status);
            Assert.NotNull(result?.Answer);
            Assert.Contains("banana", result.Answer.Answer, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("star", result.Answer.Answer, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("heart", result.Answer.Answer, StringComparison.OrdinalIgnoreCase);
            
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

            agent.Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "ProductSearch",
                    Description = "semantic search the store product catalog",
                    Query = "from Products where vector.search(embedding.text(Name), $query)",
                    ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
                },
                new AiAgentToolQuery
                {
                    Name = "RecentOrder3",
                    Description = "Get the recent orders of the current user",
                    Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                    ParametersSampleObject = "{}"
                }
            ];
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
            chat.CopyAttachmentFrom("heart.png", sourceDocId);

            var result = await chat.RunAsync<OutputSchema>(CancellationToken.None);
            chat.AddUserPrompt("what images do you have uploaded?");
            result = await chat.RunAsync<OutputSchema>(CancellationToken.None);
            Assert.Contains("heart.png", result.Answer.UploadedAttachments);
            Assert.Equal(AiConversationResult.Done, result.Status);
            Assert.NotNull(result.Answer);
        }

        [RavenRetryTheory(RavenTestCategory.Ai, maxRetries:3, delayBetweenRetriesMs:10_000)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanHandleCustomToolFailureAndStillRetrieveAttachment(Options options, GenAiConfiguration config)
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
            var imgName = "banana.png";

            chat.SetUserPrompt("use the tool CheckImageMetadata once and tell me if the result is successful.");
            
            chat.AddAttachment(imgName, GetEmbeddedImgStream(imgName));
            
            chat.Handle<object>("CheckImageMetadata", args => "{}");

            var result = await chat.RunAsync<OutputSchema>(CancellationToken.None);
            chat.SetUserPrompt("load the attachment I sent you");
            result = await chat.RunAsync<OutputSchema>(CancellationToken.None);

            Assert.Equal(AiConversationResult.Done, result.Status);
            var attachmentsUploaded = result?.Answer.UploadedAttachments;
            Assert.Contains(attachmentsUploaded, imgName);

            WaitForUserToContinueTheTest(store, false);
            
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task AttachmentOnlySecondTurnWithPromptShouldAllowModelToUseAttachment(Options options, GenAiConfiguration config)
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

            chat.AddAttachment("banana.png", GetEmbeddedImgStream("banana.png"));

            await chat.RunAsync<OutputSchema>(CancellationToken.None);

            chat.SetUserPrompt("What is inside the image I previously uploaded?");
            var result = await chat.RunAsync<OutputSchema>(CancellationToken.None);

            Assert.Equal(AiConversationResult.Done, result.Status);
            Assert.NotNull(result.Answer);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task ActionRequiredWithoutClientHandlerShouldNotReturnActionRequiredForTheInternalTool(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("action-only", config.ConnectionStringName,
                "If the user asks you to check metadata, call the tool 'CheckImageMetadata' once and wait.")
            {
                Identifier = "action-only",
                Actions =
                [
                    new AiAgentToolAction
                    {
                        Name = "CheckImageMetadata",
                        Description = "Checks external metadata for an image file.",
                        ParametersSampleObject = "{\"filename\": \"string\"}"
                    }
                ]
            };

            await store.AI.CreateAgentAsync(agent, new OutputSchema());

            var chat = store.AI.Conversation(agent.Identifier, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("Check metadata for banana.png");

            chat.Handle<object>("CheckImageMetadata", (args) => AiConversationResult.Done);

            var result = await chat.RunAsync<OutputSchema>(CancellationToken.None);

            Assert.DoesNotContain(chat.RequiredActions(), x => x.Name.Equals("__RetrieveAttachment", StringComparison.OrdinalIgnoreCase));
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task UploadedAttachmentThenUserToolResponseShouldIgnoreRetrieveAttachmentResponse(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);

            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("ignore-internal-action-response", config.ConnectionStringName,
                "You are a helpful assistant.")
            {
                Identifier = "ignore-internal-action-response"
            };

            await store.AI.CreateAgentAsync(agent, new OutputSchema());

            var chat = store.AI.Conversation(agent.Identifier, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("I will upload an image. Describe it.");
            chat.AddAttachment("banana.png", GetEmbeddedImgStream("banana.png"));

            var result = await chat.RunAsync<OutputSchema>(CancellationToken.None);
            Assert.Equal(AiConversationResult.Done, result.Status);

            Assert.DoesNotContain(chat.RequiredActions(), x => x.Name.Equals("__RetrieveAttachment", StringComparison.OrdinalIgnoreCase));
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

            chat.SetUserPrompt("I am sending you a file.");
            chat.AddAttachment(fileName, GetEmbeddedImgStream(fileName));

            var result1 = await chat.RunAsync<OutputSchema>(CancellationToken.None);
            Assert.Equal(AiConversationResult.Done, result1.Status);

            chat.SetUserPrompt($"What is the filename of the image I sent you earlier?");

            var result2 = await chat.RunAsync<OutputSchema>(CancellationToken.None);
            Assert.Equal(AiConversationResult.Done, result2.Status);
            Assert.Contains("banana", result2.Answer.Answer.ToLowerInvariant());
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
