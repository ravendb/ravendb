using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Orders;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents;
using Raven.Server.Documents.AI.Settings;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_27435 : RavenTestBase
    {
        public RavenDB_27435(ITestOutputHelper output) : base(output)
        {
        }

        private const string ProseAnswer = "Chai costs 18.0 according to the data.";
        private const string StructuredAnswer = "{\"Answer\":\"Chai costs 18.0\"}";

        private static OllamaChatCompletionClientSettings CreateOllamaSettings() =>
            new(new OllamaSettings { Uri = "http://localhost:11434", Model = "test-model" });

        private static AiAgentConfiguration CreateAgent()
        {
            var agent = new AiAgentConfiguration("shopping assistant", "fake-connection",
                "You are an AI agent of an online shop, helping customers answer queries about that topic only.");
            agent.Queries =
            [
                new AiAgentToolQuery("RecentOrders", "Get the recent orders", "from Orders limit 5")
                {
                    ParametersSampleObject = "{}"
                }
            ];
            agent.SampleObject = "{\"Answer\":\"The answer to the query\"}";
            return agent;
        }

        private async Task<DocumentDatabase> CreateDatabaseWithOrderAsync(Raven.Client.Documents.DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order
                {
                    Company = "companies/1-A",
                    Lines = [new OrderLine { ProductName = "Chai", Quantity = 2 }]
                });
                await session.SaveChangesAsync();
            }

            return await Databases.GetDocumentDatabaseInstanceFor(store);
        }

        private static HttpResponseMessage Ok(string content) => new(HttpStatusCode.OK)
        {
            Content = new StringContent(content)
        };

        [RavenFact(RavenTestCategory.Ai)]
        public async Task SplitMode_ToolTurnSendsToolsWithoutResponseFormat_FinalTurnSendsResponseFormatWithoutTools()
        {
            using var store = GetDocumentStore();
            var database = await CreateDatabaseWithOrderAsync(store);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var payloads = new List<JObject>();
                var handler = new MockLlmConversationHandler(Server.ServerStore, database,
                    onRequest: payload =>
                    {
                        payloads.Add(payload);
                        return payloads.Count switch
                        {
                            1 => Ok(MockLlm.CreateToolCallResponse("RecentOrders")),
                            2 => Ok(MockLlm.CreateProseAnswerResponse(ProseAnswer)),
                            _ => Ok(MockLlm.CreateAnswerResponse("\"Chai costs 18.0\""))
                        };
                    },
                    clientSettings: CreateOllamaSettings())
                {
                    Authentication = null
                };

                handler.Initialize(CreateAgent(), "Dummy", new RequestBody
                {
                    Parameters = context.ReadObject(new DynamicJsonValue(), "params"),
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = "how much does Chai cost?"
                }, changeVector: null);

                var r = await handler.HandleRequestAsync(context, CancellationToken.None);

                Assert.Equal(3, payloads.Count);

                Assert.NotNull(payloads[0]["tools"]);
                Assert.Null(payloads[0]["response_format"]);
                Assert.Null(payloads[0]["tool_choice"]);

                Assert.NotNull(payloads[1]["tools"]);
                Assert.Null(payloads[1]["response_format"]);
                Assert.Null(payloads[1]["tool_choice"]);

                Assert.Null(payloads[2]["tools"]);
                Assert.NotNull(payloads[2]["response_format"]);
                Assert.Null(payloads[2]["tool_choice"]);

                Assert.Contains("Chai costs 18.0", r.Response.ToString());
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task SplitMode_ProseAndStructuredMessagesPersisted_IterationsReset()
        {
            using var store = GetDocumentStore();
            var database = await CreateDatabaseWithOrderAsync(store);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var requestCount = 0;
                var handler = new MockLlmConversationHandler(Server.ServerStore, database,
                    onRequest: _ =>
                    {
                        requestCount++;
                        return requestCount switch
                        {
                            1 => Ok(MockLlm.CreateToolCallResponse("RecentOrders")),
                            2 => Ok(MockLlm.CreateProseAnswerResponse(ProseAnswer)),
                            _ => Ok(MockLlm.CreateAnswerResponse("\"Chai costs 18.0\""))
                        };
                    },
                    clientSettings: CreateOllamaSettings())
                {
                    Authentication = null
                };

                handler.Initialize(CreateAgent(), "Dummy", new RequestBody
                {
                    Parameters = context.ReadObject(new DynamicJsonValue(), "params"),
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = "how much does Chai cost?"
                }, changeVector: null);

                await handler.HandleRequestAsync(context, CancellationToken.None);
            }

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var conversation = database.DocumentsStorage.Get(context, "Dummy");
                Assert.NotNull(conversation);

                Assert.True(conversation.Data.TryGet(nameof(ConversationDocument.RemainingToolIterations), out int remaining));
                Assert.Equal(ConversationHandler.DefaultMaxModelIterationsPerCall, remaining);

                Assert.True(conversation.Data.TryGet(nameof(ConversationDocument.Messages), out BlittableJsonReaderArray messages));

                BlittableJsonReaderObject proseMessage = null;
                BlittableJsonReaderObject structuredMessage = null;
                foreach (BlittableJsonReaderObject message in messages)
                {
                    if (message.TryGet("role", out string role) == false || role != "assistant")
                        continue;
                    if (message.TryGet("content", out object content) == false || content == null)
                        continue;

                    if (content is BlittableJsonReaderObject)
                        structuredMessage = message;
                    else if (content.ToString() == ProseAnswer)
                        proseMessage = message;
                }

                Assert.NotNull(proseMessage);
                Assert.True(proseMessage.TryGet(ConversationDocument.OutputSchemaProperty, out string proseSchemaMarker));
                Assert.Equal("none", proseSchemaMarker);

                Assert.NotNull(structuredMessage);
                Assert.False(structuredMessage.TryGet(ConversationDocument.OutputSchemaProperty, out string _));
                Assert.Contains("Chai costs 18.0", structuredMessage.ToString());
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task SplitMode_ImmediateProse_TriggersSingleFollowUp()
        {
            using var store = GetDocumentStore();
            var database = await CreateDatabaseWithOrderAsync(store);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var payloads = new List<JObject>();
                var handler = new MockLlmConversationHandler(Server.ServerStore, database,
                    onRequest: payload =>
                    {
                        payloads.Add(payload);
                        return payloads.Count == 1
                            ? Ok(MockLlm.CreateProseAnswerResponse("Hello, how can I help?"))
                            : Ok(MockLlm.CreateAnswerResponse("\"Hello, how can I help?\""));
                    },
                    clientSettings: CreateOllamaSettings())
                {
                    Authentication = null
                };

                handler.Initialize(CreateAgent(), "Dummy", new RequestBody
                {
                    Parameters = context.ReadObject(new DynamicJsonValue(), "params"),
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = "hi"
                }, changeVector: null);

                var r = await handler.HandleRequestAsync(context, CancellationToken.None);

                Assert.Equal(2, payloads.Count);
                Assert.NotNull(payloads[0]["tools"]);
                Assert.Null(payloads[0]["response_format"]);
                Assert.Null(payloads[1]["tools"]);
                Assert.NotNull(payloads[1]["response_format"]);
                Assert.Contains("how can I help", r.Response.ToString());
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task OpenAiSettings_SinglePhase_Unchanged()
        {
            using var store = GetDocumentStore();
            var database = await CreateDatabaseWithOrderAsync(store);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var payloads = new List<JObject>();
                var handler = new MockLlmConversationHandler(Server.ServerStore, database,
                    onRequest: payload =>
                    {
                        payloads.Add(payload);
                        return payloads.Count == 1
                            ? Ok(MockLlm.CreateToolCallResponse("RecentOrders"))
                            : Ok(MockLlm.CreateAnswerResponse("\"Chai costs 18.0\""));
                    })
                {
                    Authentication = null
                };

                handler.Initialize(CreateAgent(), "Dummy", new RequestBody
                {
                    Parameters = context.ReadObject(new DynamicJsonValue(), "params"),
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = "how much does Chai cost?"
                }, changeVector: null);

                var r = await handler.HandleRequestAsync(context, CancellationToken.None);

                Assert.Equal(2, payloads.Count);
                foreach (var payload in payloads)
                {
                    Assert.NotNull(payload["tools"]);
                    Assert.NotNull(payload["response_format"]);
                }

                Assert.Contains("Chai costs 18.0", r.Response.ToString());
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task SplitMode_Streaming_ToolTurnsNonStreaming_FinalTurnStreamsSchemaOnly()
        {
            using var store = GetDocumentStore();
            var database = await CreateDatabaseWithOrderAsync(store);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var payloads = new List<JObject>();
                var streamingPayloads = new List<JObject>();
                var handler = new MockLlmConversationHandler(Server.ServerStore, database,
                    onRequest: payload =>
                    {
                        payloads.Add(payload);
                        return payloads.Count == 1
                            ? Ok(MockLlm.CreateToolCallResponse("RecentOrders"))
                            : Ok(MockLlm.CreateProseAnswerResponse(ProseAnswer));
                    },
                    clientSettings: CreateOllamaSettings(),
                    onStreamingRequest: payload =>
                    {
                        streamingPayloads.Add(payload);
                        return MockLlm.CreateSseAnswerResponse(StructuredAnswer);
                    })
                {
                    Authentication = null
                };

                handler.Initialize(CreateAgent(), "Dummy", new RequestBody
                {
                    Parameters = context.ReadObject(new DynamicJsonValue(), "params"),
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = "how much does Chai cost?"
                }, changeVector: null);

                using var output = new MemoryStream();
                var r = await handler.HandleStreamingRequestAsync(context, output, "Answer", CancellationToken.None);

                Assert.Equal(2, payloads.Count);
                foreach (var payload in payloads)
                {
                    Assert.NotNull(payload["tools"]);
                    Assert.Null(payload["response_format"]);
                    Assert.Null(payload["stream"]);
                }

                var streamingPayload = Assert.Single(streamingPayloads);
                Assert.Null(streamingPayload["tools"]);
                Assert.NotNull(streamingPayload["response_format"]);
                Assert.True(streamingPayload["stream"].Value<bool>());

                var streamed = Encoding.UTF8.GetString(output.ToArray());
                Assert.Contains("Chai costs 18.0", streamed.Replace("\"", "").Replace("\r\n", ""));
                Assert.DoesNotContain("according to the data", streamed);

                Assert.Contains("Chai costs 18.0", r.Response.ToString());
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public async Task SplitMode_NoSchemaConversation_Unchanged()
        {
            using var store = GetDocumentStore();
            var database = await CreateDatabaseWithOrderAsync(store);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var payloads = new List<JObject>();
                var handler = new MockLlmConversationHandler(Server.ServerStore, database,
                    onRequest: payload =>
                    {
                        payloads.Add(payload);
                        return payloads.Count == 1
                            ? Ok(MockLlm.CreateToolCallResponse("RecentOrders"))
                            : Ok(MockLlm.CreateProseAnswerResponse(ProseAnswer));
                    },
                    clientSettings: CreateOllamaSettings())
                {
                    Authentication = null
                };

                handler.Initialize(CreateAgent(), "Dummy", new RequestBody
                {
                    Parameters = context.ReadObject(new DynamicJsonValue(), "params"),
                    CreationOptions = new AiConversationCreationOptions(),
                    UserPrompt = "how much does Chai cost?",
                    OutputOptions = new AiServerOutputOptions { NoSchema = true }
                }, changeVector: null);

                var r = await handler.HandleRequestAsync(context, CancellationToken.None);

                Assert.Equal(2, payloads.Count);
                foreach (var payload in payloads)
                {
                    Assert.NotNull(payload["tools"]);
                    Assert.Null(payload["response_format"]);
                }

                Assert.Equal(ProseAnswer, r.Response.ToString());
            }
        }
    }
}
