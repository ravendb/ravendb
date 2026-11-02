using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Orders;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Settings;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using static SlowTests.Server.Documents.AI.CapturingChatCompletionClient;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_26268(ITestOutputHelper output) : RavenTestBase(output)
    {
        private const string AnthropicResponse =
            """
            {"id":"msg_1","type":"message","role":"assistant","content":[{"type":"text","text":"{\"Answer\":\"yes\"}"}],"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":5}}
            """;

        [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
        [InlineData(AiConnectorType.OpenAi, null, false, false)]
        [InlineData(AiConnectorType.OpenAi, null, true, true)]
        [InlineData(AiConnectorType.OpenAi, true, false, true)]
        [InlineData(AiConnectorType.OpenAi, false, true, false)]
        [InlineData(AiConnectorType.Anthropic, null, false, false)]
        [InlineData(AiConnectorType.Anthropic, null, true, true)]
        [InlineData(AiConnectorType.Anthropic, true, false, true)]
        [InlineData(AiConnectorType.Anthropic, false, true, false)]
        public async Task GenAi_PromptCache_ByDefaultOnlyForTasksWithQueryTools_UnlessSet(AiConnectorType connectorType, bool? enablePromptCache, bool withQuery, bool cached)
        {
            using var store = GetDocumentStore();
            var database = await GetDatabase(store.Database);

            var connection = new AiConnectionString { Name = "ai", Identifier = "ai", ModelType = AiModelType.Chat };
            AbstractChatCompletionProvider provider;
            string response;
            if (connectorType == AiConnectorType.Anthropic)
            {
                connection.AnthropicSettings = new AnthropicSettings("sk-ant-test", "claude-haiku-4-5", "https://api.anthropic.com/v1/") { EnablePromptCache = enablePromptCache };
                provider = new AnthropicChatCompletionProvider(connection.AnthropicSettings);
                response = AnthropicResponse;
            }
            else
            {
                connection.OpenAiSettings = new OpenAiSettings("fake-key", "https://fake.openai.com", "gpt-4o") { EnablePromptCache = enablePromptCache };
                provider = new OpenAiChatCompletionProvider(connection.OpenAiSettings);
                response = MockLlm.CreateAnswerResponse("{\"Answer\":\"yes\"}");
            }

            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(connection));
            store.Maintenance.Send(new AddGenAiOperation(new GenAiConfiguration
            {
                Name = "genai",
                ConnectionStringName = "ai",
                Collection = "Posts",
                Prompt = "Summarize the text.",
                SampleObject = "{\"Answer\":\"summary\"}",
                GenAiTransformation = new GenAiTransformation { Script = "ai.genContext({Text: this.Text});" },
                UpdateScript = "this.Answer = $output.Answer;",
                Queries = withQuery
                    ? [new AiAgentToolQuery { Name = "posts-by-text", Description = "Find posts by text", Query = "from Posts where Text = $text", ParametersSampleObject = "{\"text\":\"some text\"}" }]
                    : []
            }));

            GenAiTask genAi = null;
            Assert.True(await WaitForValueAsync(() =>
            {
                genAi = database.EtlLoader.Processes.OfType<GenAiTask>().FirstOrDefault();
                return Task.FromResult(genAi != null);
            }, true, timeout: 15_000), "the GenAI process was not loaded in time");

            // the process sends through this client, so the request never leaves the test
            var sentBody = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var client = new CapturingChatCompletionClient(database.DocumentsStorage.ContextPool, provider, body =>
            {
                sentBody.TrySetResult(body);
                return Ok(response);
            });
            var field = typeof(GenAiTask).GetField("_chatCompletionClient", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(field);
            var previous = (ChatCompletionClient)field.GetValue(genAi);
            field.SetValue(genAi, client);
            previous?.Dispose();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Post { Text = "Some text to summarize." }, "posts/1");
                await session.SaveChangesAsync();
            }

            var body = JObject.Parse(await sentBody.Task.WaitAsync(TimeSpan.FromSeconds(30)));
            var cacheField = connectorType == AiConnectorType.Anthropic ? body["cache_control"] : body["prompt_cache_key"];
            if (cached == false)
                Assert.Null(cacheField);
            else if (connectorType == AiConnectorType.Anthropic)
                Assert.Equal("ephemeral", (string)cacheField["type"]);
            else
                Assert.False(string.IsNullOrEmpty((string)cacheField), $"prompt_cache_key should be the conversation id, got: {cacheField}");
        }

        private class Post
        {
            public string Text;
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task ShouldIncludePromptCacheKeyInRequest()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order
                {
                    Company = "companies/1-A",
                    Lines =
                    [
                        new OrderLine
                        {
                            ProductName = "Laptop",
                            Quantity = 1
                        }
                    ]
                });
                await session.SaveChangesAsync();
            }

            var agent = new AiAgentConfiguration("test assistant", "fake-connection",
                "You are a test assistant.");

            agent.Parameters.Add(new AiAgentParameter("company"));
            agent.Queries =
            [
                new AiAgentToolQuery("RecentOrder", "Get recent orders", "from Orders where Company = $company limit 5")
                {
                    ParametersSampleObject = "{}"
                }
            ];
            agent.SampleObject = "{\"Answer\":\"The answer\"}";

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var creation = new AiConversationCreationOptions().AddParameter("company", "companies/1-A");
                var blittable = context.ReadObject(creation.ToJson(), "fake-params");
                blittable.TryGet(nameof(AiConversationCreationOptions.Parameters), out BlittableJsonReaderObject parameters);

                string capturedPromptCacheKey = null;

                var handler = new MockLlmConversationHandler(Server.ServerStore, database,
                    onRequest: payload =>
                    {
                        capturedPromptCacheKey ??= payload["prompt_cache_key"]?.ToString();
                        return null; // fall through to default handling
                    })
                {
                    Authentication = null
                };

                handler.Initialize(agent, "conversations/1-A", new RequestBody
                {
                    Parameters = parameters,
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = "show my orders"
                }, changeVector: null);

                await handler.HandleRequestAsync(context, CancellationToken.None);

                Assert.NotNull(capturedPromptCacheKey);
                Assert.Equal("conversations/1-A", capturedPromptCacheKey);
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task ShouldNotIncludePromptCacheKeyForGoogle()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order
                {
                    Company = "companies/1-A",
                    Lines =
                    [
                        new OrderLine
                        {
                            ProductName = "Laptop",
                            Quantity = 1
                        }
                    ]
                });
                await session.SaveChangesAsync();
            }

            var agent = new AiAgentConfiguration("test assistant", "fake-connection",
                "You are a test assistant.");

            agent.Parameters.Add(new AiAgentParameter("company"));
            agent.Queries =
            [
                new AiAgentToolQuery("RecentOrder", "Get recent orders", "from Orders where Company = $company limit 5")
                {
                    ParametersSampleObject = "{}"
                }
            ];
            agent.SampleObject = "{\"Answer\":\"The answer\"}";

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var creation = new AiConversationCreationOptions().AddParameter("company", "companies/1-A");
                var blittable = context.ReadObject(creation.ToJson(), "fake-params");
                blittable.TryGet(nameof(AiConversationCreationOptions.Parameters), out BlittableJsonReaderObject parameters);

                bool promptCacheKeySeen = false;

                var googleSettings = new GoogleChatCompletionProvider(new GoogleSettings("gemini-2.0-flash", "fake-key"));

                var handler = new MockLlmConversationHandler(Server.ServerStore, database,
                    onRequest: payload =>
                    {
                        if (payload["prompt_cache_key"] != null)
                            promptCacheKeySeen = true;
                        return null;
                    },
                    clientSettings: googleSettings)
                {
                    Authentication = null
                };

                handler.Initialize(agent, "conversations/1-A", new RequestBody
                {
                    Parameters = parameters,
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = "show my orders"
                }, changeVector: null);

                await handler.HandleRequestAsync(context, CancellationToken.None);

                Assert.False(promptCacheKeySeen, "prompt_cache_key should not be sent for Google provider");
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task ShouldRespectExplicitEnablePromptCacheSetting()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order
                {
                    Company = "companies/1-A",
                    Lines =
                    [
                        new OrderLine
                        {
                            ProductName = "Laptop",
                            Quantity = 1
                        }
                    ]
                });
                await session.SaveChangesAsync();
            }

            var agent = new AiAgentConfiguration("test assistant", "fake-connection",
                "You are a test assistant.");

            agent.Parameters.Add(new AiAgentParameter("company"));
            agent.Queries =
            [
                new AiAgentToolQuery("RecentOrder", "Get recent orders", "from Orders where Company = $company limit 5")
                {
                    ParametersSampleObject = "{}"
                }
            ];
            agent.SampleObject = "{\"Answer\":\"The answer\"}";

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var creation = new AiConversationCreationOptions().AddParameter("company", "companies/1-A");
                var blittable = context.ReadObject(creation.ToJson(), "fake-params");
                blittable.TryGet(nameof(AiConversationCreationOptions.Parameters), out BlittableJsonReaderObject parameters);

                bool promptCacheKeySeen = false;

                // Explicitly disable prompt cache on an OpenAI connection
                var openAiSettings = new OpenAiSettings("fake-key", "https://fake.openai.com", "gpt-4o") { EnablePromptCache = false };
                var settings = new OpenAiChatCompletionProvider(openAiSettings);

                var handler = new MockLlmConversationHandler(Server.ServerStore, database,
                    onRequest: payload =>
                    {
                        if (payload["prompt_cache_key"] != null)
                            promptCacheKeySeen = true;
                        return null;
                    },
                    clientSettings: settings)
                {
                    Authentication = null
                };

                handler.Initialize(agent, "conversations/1-A", new RequestBody
                {
                    Parameters = parameters,
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = "show my orders"
                }, changeVector: null);

                await handler.HandleRequestAsync(context, CancellationToken.None);

                Assert.False(promptCacheKeySeen, "prompt_cache_key should not be sent when EnablePromptCache is explicitly false");
            }
        }
    }
}
