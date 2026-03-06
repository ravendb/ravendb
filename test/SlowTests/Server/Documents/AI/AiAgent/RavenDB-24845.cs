using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_24845 : RavenTestBase
    {
        public RavenDB_24845(ITestOutputHelper output) : base(output)
        {
        }

        private class OutputSchema
        {
            public string Answer { get; set; } = "your answer";
            public static readonly OutputSchema Instance = new();
        }
        private class ConversationTestDto
        {
            public List<MessageDto> Messages { get; set; }
        }

        private class MessageDto
        {
            public string Role { get; set; }

            public JToken Content { get; set; }
        }

        public string[] ExpectedStrings = new[] { "single string", "second string", "third string" };

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanSetUserPromptWithSingleString(Options options, GenAiConfiguration configuration)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(configuration.Connection));
            var agent = new AiAgentConfiguration("Test Agent", configuration.ConnectionStringName, "test");
            var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());
            chat.SetUserPrompt("single string");
            var result = await chat.RunAsync<OutputSchema>();
            
            Assert.Equal(AiConversationResult.Done, result.Status);
            using (var session = store.OpenAsyncSession())
            {
                var conversationDoc = await session.LoadAsync<ConversationTestDto>(chat.Id);
                Assert.NotNull(conversationDoc);
                
                var lastMessage = conversationDoc.Messages.Last(m => m.Role =="user");
                Assert.Equal("user", lastMessage.Role);

                var contentArr = Assert.IsType<JArray>(lastMessage.Content);
                Assert.Single(contentArr);
                var firstPart = contentArr[0];
                Assert.Equal("text", firstPart["type"]?.ToString());
                Assert.Equal("single string", firstPart["text"]?.ToString());
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanSetUserPromptWithEmptyString(Options options, GenAiConfiguration configuration)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(configuration.Connection));
            var agent = new AiAgentConfiguration("Test Agent", configuration.ConnectionStringName, "test");
            var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());
            var e1 = Assert.Throws<ArgumentException>(() => chat.SetUserPrompt(string.Empty));
            Assert.Contains("cannot be null or empty", e1.Message);
            var e2 = Assert.Throws<ArgumentException>(() => chat.AddUserPrompt(string.Empty));
            Assert.Contains("cannot be null or empty", e2.Message);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanSetUserPromptWithMultipleStrings(Options options, GenAiConfiguration configuration)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(configuration.Connection));
            var agent = new AiAgentConfiguration("Test Agent", configuration.ConnectionStringName, "test");
            var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());
            chat.AddUserPrompt("single string", "second string", "third string");
            var result = await chat.RunAsync<OutputSchema>();
            Assert.Equal(AiConversationResult.Done, result.Status);
            using (var session = store.OpenAsyncSession())
            {
                var conversationDoc = await session.LoadAsync<ConversationTestDto>(chat.Id);
                Assert.NotNull(conversationDoc);

                var lastMessage = conversationDoc.Messages.Last(m => m.Role == "user");
                Assert.Equal("user", lastMessage.Role);

                var contentArr = Assert.IsType<JArray>(lastMessage.Content);
                Assert.Equal(3, contentArr.Count);

                for (int i = 0; i < contentArr.Count; i++)
                {
                    var part = contentArr[i];
                    Assert.Equal("text", part["type"]?.ToString());
                    Assert.Equal(ExpectedStrings[i], part["text"]?.ToString());
                }
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanSetUserPromptWithMultipleStringsAndNull(Options options, GenAiConfiguration configuration)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(configuration.Connection));
            var agent = new AiAgentConfiguration("Test Agent", configuration.ConnectionStringName, "test");
            var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());
            var e = Assert.Throws<ArgumentNullException>(() => chat.AddUserPrompt("single string", null, "third string"));
            Assert.Contains("Value cannot be null.", e.Message);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanSetUserPromptWithMultipleStringsAndEmptyString(Options options, GenAiConfiguration configuration)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(configuration.Connection));
            var agent = new AiAgentConfiguration("Test Agent", configuration.ConnectionStringName, "test");
            var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());
            var e = Assert.Throws<ArgumentException>(() => chat.AddUserPrompt("single string", "", "third string"));
            Assert.Contains("cannot be null or empty", e.Message);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanSetUserPromptWithEmptyArray(Options options, GenAiConfiguration configuration)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(configuration.Connection));
            var agent = new AiAgentConfiguration("Test Agent", configuration.ConnectionStringName, "test");
            var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());
            chat.AddUserPrompt(Array.Empty<string>());
            var e = await Assert.ThrowsAsync<RavenException>(() => chat.RunAsync<OutputSchema>());
            Assert.Contains("without a user prompt.", e.InnerException?.Message);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanSetUserPromptWithStringArray(Options options, GenAiConfiguration configuration)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(configuration.Connection));
            var agent = new AiAgentConfiguration("Test Agent", configuration.ConnectionStringName, "test");
            var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());
            chat.AddUserPrompt(ExpectedStrings);
            var result = await chat.RunAsync<OutputSchema>();

            Assert.Equal(AiConversationResult.Done, result.Status);
            using (var session = store.OpenAsyncSession())
            {
                var conversationDoc = await session.LoadAsync<ConversationTestDto>(chat.Id);
                Assert.NotNull(conversationDoc);

                var lastMessage = conversationDoc.Messages.Last(m => m.Role == "user");
                Assert.Equal("user", lastMessage.Role);

                var contentArr = Assert.IsType<JArray>(lastMessage.Content);
                Assert.Equal(3, contentArr.Count);
                for (int i = 0; i < contentArr.Count; i++)
                {
                    var part = contentArr[i];
                    Assert.Equal("text", part["type"]?.ToString());
                    Assert.Equal(ExpectedStrings[i], part["text"]?.ToString());
                }
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanSetUserPromptWithEmptyList(Options options, GenAiConfiguration configuration)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(configuration.Connection));
            var agent = new AiAgentConfiguration("Test Agent", configuration.ConnectionStringName, "test");
            var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());
            chat.AddUserPrompt(new List<string>());
            var e = await Assert.ThrowsAsync<RavenException>(() => chat.RunAsync<OutputSchema>());
            Assert.Contains("without a user prompt.", e.InnerException?.Message);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanSetUserPromptWithStringList(Options options, GenAiConfiguration configuration)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(configuration.Connection));
            var agent = new AiAgentConfiguration("Test Agent", configuration.ConnectionStringName, "test");
            var agentId = (await store.AI.CreateAgentAsync(agent, OutputSchema.Instance)).Identifier;

            var chat = store.AI.Conversation(agentId, "chats/", new AiConversationCreationOptions());
            var list = ExpectedStrings.ToList();
            chat.AddUserPrompt(list);
            var result = await chat.RunAsync<OutputSchema>();
            
            Assert.Equal(AiConversationResult.Done, result.Status);
            using (var session = store.OpenAsyncSession())
            {
                var conversationDoc = await session.LoadAsync<ConversationTestDto>(chat.Id);
                Assert.NotNull(conversationDoc);

                var lastMessage = conversationDoc.Messages.Last(m => m.Role == "user");
                Assert.Equal("user", lastMessage.Role);

                var contentArr = Assert.IsType<JArray>(lastMessage.Content);
                Assert.Equal(3, contentArr.Count);
                for (int i = 0; i < contentArr.Count; i++)
                {
                    var part = contentArr[i];
                    Assert.Equal("text", part["type"]?.ToString());
                    Assert.Equal(ExpectedStrings[i], part["text"]?.ToString());
                }
            }
        }
    }
}
