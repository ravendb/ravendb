using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Tests.Infrastructure.ConnectionString.AI;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class AnthropicClientApiTests : RavenTestBase
    {
        public AnthropicClientApiTests(ITestOutputHelper output) : base(output)
        {
        }

        private class AgentAnswer
        {
            public string Answer = "the answer to the user's question";
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Anthropic, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Agent_AddUserPrompt_MultiPart(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("assistant", config.ConnectionStringName, "You answer briefly.") { Identifier = "assistant" };
            var created = await store.AI.CreateAgentAsync(agent, new AgentAnswer());

            var chat = store.AI.Conversation(created.Identifier, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("I will ask two things.");
            chat.AddUserPrompt(new[] { "First, what is 2+2?", "Second, name a primary colour." });
            var r = await chat.RunAsync<AgentAnswer>(CancellationToken.None);

            Assert.Equal(AiConversationResult.Done, r.Status);
            Assert.False(string.IsNullOrEmpty(r.Answer.Answer));
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Anthropic, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Agent_Attachment_Image(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("vision", config.ConnectionStringName, "You describe images you are given.") { Identifier = "vision" };
            var created = await store.AI.CreateAgentAsync(agent, new AgentAnswer());

            var chat = store.AI.Conversation(created.Identifier, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("What fruit is in this image? Answer with one word.");
            await using (var img = GetImg("banana.png"))
            {
                chat.AddAttachment("banana.png", img, "image/png");
                var r = await chat.RunAsync<AgentAnswer>(CancellationToken.None);
                Assert.Equal(AiConversationResult.Done, r.Status);
                Assert.Contains("banana", r.Answer.Answer.ToLowerInvariant());
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Anthropic, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Agent_CopyAttachmentFrom_Document(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using (var session = store.OpenSession())
            {
                session.Store(new { }, "docs/1");
                session.SaveChanges();
            }
            await using (var img = GetImg("heart.png"))
                await store.Operations.SendAsync(new Raven.Client.Documents.Operations.Attachments.PutAttachmentOperation("docs/1", "heart.png", img, "image/png"));

            var agent = new AiAgentConfiguration("vision", config.ConnectionStringName, "You describe images you are given.") { Identifier = "vision" };
            var created = await store.AI.CreateAgentAsync(agent, new AgentAnswer());

            var chat = store.AI.Conversation(created.Identifier, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("What shape is in this image? Answer with one word.");
            chat.CopyAttachmentFrom("docs/1", "heart.png");
            var r = await chat.RunAsync<AgentAnswer>(CancellationToken.None);

            Assert.Equal(AiConversationResult.Done, r.Status);
            Assert.Contains("heart", r.Answer.Answer.ToLowerInvariant());
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Anthropic, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Agent_ActionTool_ManualLoop(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("assistant", config.ConnectionStringName,
                "You help the user. When asked about the weather you MUST call the GetWeather tool.") { Identifier = "assistant" };
            agent.Actions = [new AiAgentToolAction("GetWeather", "Get the current weather for a city") { ParametersSampleObject = "{\"city\":\"the city\"}" }];
            var created = await store.AI.CreateAgentAsync(agent, new AgentAnswer());

            var chat = store.AI.Conversation(created.Identifier, "chats/", new AiConversationCreationOptions());

            chat.OnUnhandledAction += _ => Task.CompletedTask;

            chat.SetUserPrompt("What is the weather in Paris? Use your tool.");
            var r = await chat.RunAsync<AgentAnswer>(CancellationToken.None);

            Assert.Equal(AiConversationResult.ActionRequired, r.Status);

            foreach (var action in chat.RequiredActions())
                chat.AddActionResponse(action.ToolId, "It is 22C and sunny.");

            r = await chat.RunAsync<AgentAnswer>(CancellationToken.None);

            Assert.Equal(AiConversationResult.Done, r.Status);
            Assert.False(string.IsNullOrEmpty(r.Answer.Answer));
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Anthropic, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Agent_QueryTool_AnswersFromRetrievedData_AndRemembersItNextTurn(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using (var session = store.OpenSession())
            {
                session.Store(new Product("Zyxwv Gizmo 3000", "Gadgets", "companies/1-A"));
                session.Store(new Product("Qwerty Widget 77", "Gadgets", "companies/2-A"));
                session.Store(new Product("Ordinary Stapler", "Office", "companies/1-A"));
                session.SaveChanges();
                session.Query<Product>().Where(p => p.Category == "Gadgets" && p.Company == "companies/1-A").ToList();
            }
            Indexes.WaitForIndexing(store);

            // the model supplies the category, the server binds the company
            var agent = new AiAgentConfiguration("shop", config.ConnectionStringName,
                "You are a shop assistant. You do not know the catalog from memory: whenever the user asks about products, call FindProducts " +
                "and answer only from what it returns, using exact product names.") { Identifier = "shop" };
            agent.Parameters.Add(new AiAgentParameter("company", "the current company id"));
            agent.Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "FindProducts",
                    Description = "Find the current company's products in a given category.",
                    Query = "from Products where Category = $category and Company = $company",
                    ParametersSampleObject = "{\"category\": \"the product category to look up\"}"
                }
            ];
            var created = await store.AI.CreateAgentAsync(agent, new AgentAnswer());

            var chat = store.AI.Conversation(created.Identifier, "chats/", new AiConversationCreationOptions().AddParameter("company", "companies/1-A"));
            chat.SetUserPrompt("What gadgets do we sell? Give their exact names.");
            var first = await chat.RunAsync<AgentAnswer>(CancellationToken.None);

            Assert.Equal(AiConversationResult.Done, first.Status);
            var firstAnswer = first.Answer.Answer.ToLowerInvariant();
            Assert.Contains("zyxwv", firstAnswer);      // only knowable from the query result
            Assert.DoesNotContain("qwerty", firstAnswer); // another company's product: the bound parameter was applied

            // the next request replays the history, tool call and tool result included
            chat.SetUserPrompt("Repeat the exact name of the product you just told me about.");
            var second = await chat.RunAsync<AgentAnswer>(CancellationToken.None);

            Assert.Equal(AiConversationResult.Done, second.Status);
            Assert.Contains("zyxwv", second.Answer.Answer.ToLowerInvariant());
        }

        // Reasoning on a model that binds thinking blocks, with a tool call and an attachment, across two turns: both turns
        // complete, and no thinking signature is stored on the conversation.
        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Anthropic, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Agent_WithReasoning_ToolCallsAndAnAttachment_WorkAcrossTurns(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            var connection = new AiConnectionString
            {
                Name = config.Connection.Name,
                ModelType = AiModelType.Chat,
                AnthropicSettings = new AnthropicSettings(config.Connection.AnthropicSettings.ApiKey, GenAiAnthropicConnectorForTesting.SupportingReasoningModel, reasoningEffort: "medium")
            };
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connection));

            using (var session = store.OpenSession())
            {
                session.Store(new Product("Zyxwv Gizmo 3000", "Gadgets", "companies/1-A"));
                session.Store(new Product("Ordinary Stapler", "Office", "companies/1-A"));
                session.SaveChanges();
                session.Query<Product>().Where(p => p.Company == "companies/1-A").ToList();
            }
            Indexes.WaitForIndexing(store);

            var agent = new AiAgentConfiguration("shop", config.ConnectionStringName,
                "You are a shop assistant. Think through the request, call FindProducts, and answer only from what it returns, using exact product names.") { Identifier = "shop" };
            agent.Parameters.Add(new AiAgentParameter("company", "the current company id"));
            agent.Queries = [new AiAgentToolQuery { Name = "FindProducts", Description = "Find the current company's products", Query = "from Products where Company = $company", ParametersSampleObject = "{}" }];
            var created = await store.AI.CreateAgentAsync(agent, new AgentAnswer());

            var chat = store.AI.Conversation(created.Identifier, "chats/", new AiConversationCreationOptions().AddParameter("company", "companies/1-A"));
            chat.SetUserPrompt("Which of our products would suit someone who loves gadgets? Give the exact name, and say which fruit is in the attached image.");
            await using (var img = GetImg("banana.png"))
            {
                chat.AddAttachment("banana.png", img, "image/png");
                var first = await chat.RunAsync<AgentAnswer>(CancellationToken.None);
                Assert.Equal(AiConversationResult.Done, first.Status);
                Assert.Contains("zyxwv", first.Answer.Answer.ToLowerInvariant());
            }

            chat.SetUserPrompt("Repeat the exact name of that product.");
            var second = await chat.RunAsync<AgentAnswer>(CancellationToken.None);
            Assert.Equal(AiConversationResult.Done, second.Status);
            Assert.Contains("zyxwv", second.Answer.Answer.ToLowerInvariant());

            var database = await GetDatabase(store.Database);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
                Assert.DoesNotContain("\"signature\"", database.DocumentsStorage.Get(ctx, chat.Id).Data.ToString());
        }

        // Haiku 4.5 caches only prompts of at least 4,096 tokens, hence the long system prompt.
        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Anthropic, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Agent_PromptCaching_SecondTurnReadsTheCache_UnlessDisabled(Options options, GenAiConfiguration config)
        {
            foreach (var enablePromptCache in new bool?[] { null, false })
            {
                using var store = GetDocumentStore(options);
                var settings = config.Connection.AnthropicSettings;
                var connection = new AiConnectionString
                {
                    Name = config.Connection.Name,
                    ModelType = AiModelType.Chat,
                    AnthropicSettings = new AnthropicSettings(settings.ApiKey, settings.Model) { EnablePromptCache = enablePromptCache }
                };
                store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connection));

                var rules = string.Join(" ", Enumerable.Range(0, 600).Select(i => $"Rule {i}: answer briefly and politely."));
                var agent = new AiAgentConfiguration("assistant", config.ConnectionStringName, "You are a helpful assistant. " + rules) { Identifier = "assistant" };
                var created = await store.AI.CreateAgentAsync(agent, new AgentAnswer());

                var chat = store.AI.Conversation(created.Identifier, "chats/", new AiConversationCreationOptions());
                chat.SetUserPrompt("Say hello.");
                var first = await chat.RunAsync<AgentAnswer>(CancellationToken.None);
                Assert.Equal(AiConversationResult.Done, first.Status);

                chat.SetUserPrompt("Say goodbye.");
                var second = await chat.RunAsync<AgentAnswer>(CancellationToken.None);
                Assert.Equal(AiConversationResult.Done, second.Status);

                if (enablePromptCache == false)
                    Assert.Equal(0, second.Usage.CachedTokens);
                else
                    Assert.True(second.Usage.CachedTokens > 0, $"expected a cache read on the second turn, usage: {second.Usage}");
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Anthropic, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Agent_SubAgent(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            // only the sub-agent knows the code word, so it can reach the answer only through the sub-agent call
            var sub = new AiAgentConfiguration("vault-agent", config.ConnectionStringName, "The secret code word is PELICAN-42. When asked for the code word, answer with it.") { Identifier = "vault-agent" };
            var subId = (await store.AI.CreateAgentAsync(sub, new AgentAnswer())).Identifier;

            var parent = new AiAgentConfiguration("host", config.ConnectionStringName, "You are a host. You don't know the code word; when the user asks for it, ask the vault sub-agent and repeat its answer.") { Identifier = "host" };
            parent.SubAgents = [new AiAgentToolSubAgent { Identifier = subId, Description = "Knows the secret code word. Ask it for the code word." }];
            var created = await store.AI.CreateAgentAsync(parent, new AgentAnswer());

            var chat = store.AI.Conversation(created.Identifier, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("What is the secret code word?");
            var r = await chat.RunAsync<AgentAnswer>(CancellationToken.None);

            Assert.Equal(AiConversationResult.Done, r.Status);
            Assert.Contains("pelican-42", r.Answer.Answer.ToLowerInvariant());
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Anthropic, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Agent_Summarization(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("assistant", config.ConnectionStringName, "You answer briefly.") { Identifier = "assistant" };
            agent.ChatTrimming = new AiAgentChatTrimmingConfiguration { Tokens = new AiAgentSummarizationByTokens { MaxTokensBeforeSummarization = 0 } }; // summarize every turn
            var created = await store.AI.CreateAgentAsync(agent, new AgentAnswer());

            var chat = store.AI.Conversation(created.Identifier, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("Name a fruit.");
            var first = await chat.RunAsync<AgentAnswer>(CancellationToken.None);
            Assert.Equal(AiConversationResult.Done, first.Status);

            chat.SetUserPrompt("Name a vegetable."); // forces a summarization completion call through Claude
            var second = await chat.RunAsync<AgentAnswer>(CancellationToken.None);
            Assert.Equal(AiConversationResult.Done, second.Status);

            // Summary-role messages only appear in the Full view.
            var messages = await store.AI.GetConversationMessagesAsync(new GetConversationMessagesOptions
            {
                ConversationId = chat.Id,
                DetailLevel = AiConversationDetailLevel.Full,
                PageSize = 50
            });
            Assert.Contains(messages.Messages, m => m.Role == AiMessageRole.Summary);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Anthropic, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Agent_OutputCapReached_FailsWithTooManyTokens(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            var settings = config.Connection.AnthropicSettings;
            var connection = new AiConnectionString
            {
                Name = config.Connection.Name,
                ModelType = AiModelType.Chat,
                AnthropicSettings = new AnthropicSettings(settings.ApiKey, settings.Model, maxOutputTokens: 16)
            };
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connection));

            var agent = new AiAgentConfiguration("writer", config.ConnectionStringName, "You write long stories.") { Identifier = "writer" };
            var created = await store.AI.CreateAgentAsync(agent, new AgentAnswer());

            var chat = store.AI.Conversation(created.Identifier, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("Write a 300-word story about a lighthouse.");
            var ex = await Assert.ThrowsAsync<AiException>(() => chat.RunAsync<AgentAnswer>(CancellationToken.None));

            Assert.Contains(nameof(TooManyTokensException), ex.ToString());
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Anthropic, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Agent_OutputCapReached_WhileStreaming_FailsWithTooManyTokens(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            var settings = config.Connection.AnthropicSettings;
            var connection = new AiConnectionString
            {
                Name = config.Connection.Name,
                ModelType = AiModelType.Chat,
                AnthropicSettings = new AnthropicSettings(settings.ApiKey, settings.Model, maxOutputTokens: 16)
            };
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connection));

            var agent = new AiAgentConfiguration("writer", config.ConnectionStringName, "You write long stories.") { Identifier = "writer" };
            var created = await store.AI.CreateAgentAsync(agent, new AgentAnswer());

            var chat = store.AI.Conversation(created.Identifier, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("Write a 300-word story about a lighthouse.");
            var ex = await Assert.ThrowsAsync<AiException>(() => chat.StreamAsync<AgentAnswer>(a => a.Answer, _ => Task.CompletedTask, CancellationToken.None));

            Assert.Contains(nameof(TooManyTokensException), ex.ToString());
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Anthropic, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Agent_InvalidApiKey_FailsWithTheAuthenticationError(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            var connection = new AiConnectionString
            {
                Name = config.Connection.Name,
                ModelType = AiModelType.Chat,
                AnthropicSettings = new AnthropicSettings("sk-ant-not-a-real-key", config.Connection.AnthropicSettings.Model)
            };
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connection));

            var agent = new AiAgentConfiguration("assistant", config.ConnectionStringName, "You answer briefly.") { Identifier = "assistant" };
            var created = await store.AI.CreateAgentAsync(agent, new AgentAnswer());

            var chat = store.AI.Conversation(created.Identifier, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("Say hi.");
            var ex = await Assert.ThrowsAsync<AiException>(() => chat.RunAsync<AgentAnswer>(CancellationToken.None));

            Assert.Contains("authentication_error", ex.ToString());
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Anthropic, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Agent_Attachment_Pdf(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var agent = new AiAgentConfiguration("reader", config.ConnectionStringName, "You read documents you are given.") { Identifier = "reader" };
            var created = await store.AI.CreateAgentAsync(agent, new AgentAnswer());

            var chat = store.AI.Conversation(created.Identifier, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("Which word is written in the attached PDF? Answer with that word only.");
            await using (var pdf = typeof(AnthropicClientApiTests).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_24644.Hibernating.pdf"))
            {
                Assert.NotNull(pdf);
                chat.AddAttachment("document.pdf", pdf, "application/pdf");
                var r = await chat.RunAsync<AgentAnswer>(CancellationToken.None);
                Assert.Equal(AiConversationResult.Done, r.Status);
                Assert.Contains("hibernating", r.Answer.Answer.ToLowerInvariant());
            }
        }

        private record Product(string Name, string Category, string Company);

        private static Stream GetImg(string name)
        {
            var resourceName = "SlowTests.Data.RavenDB_24648." + name;
            var stream = typeof(AnthropicClientApiTests).Assembly.GetManifestResourceStream(resourceName);
            if (stream == null)
                throw new FileNotFoundException($"Embedded resource not found: {resourceName}");
            return stream;
        }
    }
}
