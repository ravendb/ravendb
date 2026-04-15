using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
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

        private static AiAgentConfiguration CreateTestAgent() => new("Test Agent", "fake-connection", "test")
        {
            SampleObject = "{\"Answer\":\"your answer\"}"
        };

        private static BlittableJsonReaderArray CreateMultiPartPrompt(JsonOperationContext context, params string[] parts)
        {
            var array = new DynamicJsonArray();
            foreach (var part in parts)
            {
                array.Add(new DynamicJsonValue
                {
                    ["type"] = "text",
                    ["text"] = part
                });
            }
            var blittable = context.ReadObject(new DynamicJsonValue { ["prompt"] = array }, "prompt");
            blittable.TryGet("prompt", out BlittableJsonReaderArray result);
            return result;
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task CanSetUserPromptWithSingleString()
        {
            using var store = GetDocumentStore();

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var handler = new MockLlmConversationHandler(Server.ServerStore, database) { Authentication = null };
                // The client API always wraps single strings into [{type:"text", text:"..."}] format
                handler.Initialize(CreateTestAgent(), "chats/1", new RequestBody
                {
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = CreateMultiPartPrompt(context, "single string")
                }, changeVector: null);

                var result = await handler.HandleRequest(context, CancellationToken.None);
                Assert.NotNull(result.Response);

                using (var session = store.OpenAsyncSession())
                {
                    var conversationDoc = await session.LoadAsync<ConversationTestDto>("chats/1");
                    Assert.NotNull(conversationDoc);

                    var lastMessage = conversationDoc.Messages.Last(m => m.Role == "user");
                    Assert.Equal("user", lastMessage.Role);

                    var contentArr = Assert.IsType<JArray>(lastMessage.Content);
                    Assert.Single(contentArr);
                    var firstPart = contentArr[0];
                    Assert.Equal("text", firstPart["type"]?.ToString());
                    Assert.Equal("single string", firstPart["text"]?.ToString());
                }
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task CanSetUserPromptWithEmptyString()
        {
            using var store = GetDocumentStore();
            var chat = store.AI.Conversation("fake-agent", "chats/", new AiConversationCreationOptions());
            var e1 = Assert.Throws<ArgumentException>(() => chat.SetUserPrompt(string.Empty));
            Assert.Contains("cannot be null or empty", e1.Message);
            var e2 = Assert.Throws<ArgumentException>(() => chat.AddUserPrompt(string.Empty));
            Assert.Contains("cannot be null or empty", e2.Message);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task CanSetUserPromptWithMultipleStrings()
        {
            using var store = GetDocumentStore();

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var handler = new MockLlmConversationHandler(Server.ServerStore, database) { Authentication = null };
                handler.Initialize(CreateTestAgent(), "chats/1", new RequestBody
                {
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = CreateMultiPartPrompt(context, "single string", "second string", "third string")
                }, changeVector: null);

                var result = await handler.HandleRequest(context, CancellationToken.None);
                Assert.NotNull(result.Response);

                using (var session = store.OpenAsyncSession())
                {
                    var conversationDoc = await session.LoadAsync<ConversationTestDto>("chats/1");
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

        [RavenFact(RavenTestCategory.Ai)]
        public async Task CanSetUserPromptWithMultipleStringsAndNull()
        {
            using var store = GetDocumentStore();
            var chat = store.AI.Conversation("fake-agent", "chats/", new AiConversationCreationOptions());
            var e = Assert.Throws<ArgumentNullException>(() => chat.AddUserPrompt("single string", null, "third string"));
            Assert.Contains("Value cannot be null.", e.Message);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task CanSetUserPromptWithMultipleStringsAndEmptyString()
        {
            using var store = GetDocumentStore();
            var chat = store.AI.Conversation("fake-agent", "chats/", new AiConversationCreationOptions());
            var e = Assert.Throws<ArgumentException>(() => chat.AddUserPrompt("single string", "", "third string"));
            Assert.Contains("cannot be null or empty", e.Message);
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task CanSetUserPromptWithEmptyArray()
        {
            using var store = GetDocumentStore();

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var handler = new MockLlmConversationHandler(Server.ServerStore, database) { Authentication = null };
                handler.Initialize(CreateTestAgent(), "chats/1", new RequestBody
                {
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = CreateMultiPartPrompt(context)
                }, changeVector: null);

                var e = await Assert.ThrowsAsync<InvalidOperationException>(() => handler.HandleRequest(context, CancellationToken.None));
                Assert.Contains("without a user prompt", e.Message);
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task CanSetUserPromptWithStringArray()
        {
            using var store = GetDocumentStore();

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var handler = new MockLlmConversationHandler(Server.ServerStore, database) { Authentication = null };
                handler.Initialize(CreateTestAgent(), "chats/1", new RequestBody
                {
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = CreateMultiPartPrompt(context, ExpectedStrings)
                }, changeVector: null);

                var result = await handler.HandleRequest(context, CancellationToken.None);
                Assert.NotNull(result.Response);

                using (var session = store.OpenAsyncSession())
                {
                    var conversationDoc = await session.LoadAsync<ConversationTestDto>("chats/1");
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

        [RavenFact(RavenTestCategory.Ai)]
        public async Task CanSetUserPromptWithEmptyList()
        {
            using var store = GetDocumentStore();

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var handler = new MockLlmConversationHandler(Server.ServerStore, database) { Authentication = null };
                handler.Initialize(CreateTestAgent(), "chats/1", new RequestBody
                {
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = CreateMultiPartPrompt(context)
                }, changeVector: null);

                var e = await Assert.ThrowsAsync<InvalidOperationException>(() => handler.HandleRequest(context, CancellationToken.None));
                Assert.Contains("without a user prompt", e.Message);
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task CanSetUserPromptWithStringList()
        {
            using var store = GetDocumentStore();

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var handler = new MockLlmConversationHandler(Server.ServerStore, database) { Authentication = null };
                handler.Initialize(CreateTestAgent(), "chats/1", new RequestBody
                {
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = CreateMultiPartPrompt(context, ExpectedStrings)
                }, changeVector: null);

                var result = await handler.HandleRequest(context, CancellationToken.None);
                Assert.NotNull(result.Response);

                using (var session = store.OpenAsyncSession())
                {
                    var conversationDoc = await session.LoadAsync<ConversationTestDto>("chats/1");
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
}
