using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_26911(ITestOutputHelper output) : RavenDB_24887_Base(output)
    {

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task WrongIndexInQuery(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using (var session = store.OpenAsyncSession())
            {
                foreach (var u in Users)
                    await session.StoreAsync(u);

                await session.SaveChangesAsync();
            }

            var userAgent = new AiAgentConfiguration("user-info-agent-1",
                config.ConnectionStringName,
                "Your role responsibility is to provide the user's name when requested OR change user name.")
            {
                Queries = new List<AiAgentToolQuery>()
                {
                    new AiAgentToolQuery
                    {
                        Name = "GetUserName",
                        Description = "Get the user name",
                        // wrong index: 'NonExistentIndex' does not exist in the database
                        Query = "from index 'NonExistentIndex' " +
                                "where id() = $userId " +
                                "select Name",
                        ParametersSampleObject = "{}"
                    },
                },
                Actions = new List<AiAgentToolAction>()
                {
                    new AiAgentToolAction("ChangeUserName",
                        "Updates the name of the current user interacting with the AI agent. have to send also the old name for validation.")
                    {
                        ParametersSampleObject = JsonConvert.SerializeObject(ChangeUserNameSampleRequest.Instance)
                    },
                }
            };
            userAgent.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            var userAgentId = (await store.AI.CreateAgentAsync(userAgent, MoviesSampleObject.Instance)).Identifier;

            var chat = store.AI.Conversation(userAgentId, "chats/1",
                new AiConversationCreationOptions().AddParameter("userId", "Users/1"));
            chat.Handle<ChangeUserNameSampleRequest, ActionToolResult>("ChangeUserName",
                r => ChangeUserNameAsync(store, r));

            chat.SetUserPrompt("What is my name?");
            var e = await Assert.ThrowsAsync<QueryToolFailedException>(() => chat.RunAsync<MoviesSampleObject>(CancellationToken.None));
            Assert.Contains("failed with status code 404", e.Message);
            Assert.Contains("NonExistentIndex", e.Message);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task WrongIndexInSubAgentQuery(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using (var session = store.OpenAsyncSession())
            {
                foreach (var u in Users)
                    await session.StoreAsync(u);

                await session.SaveChangesAsync();
            }

            // The sub-agent runs a query against an index that does not exist.
            var userAgent = new AiAgentConfiguration("user-info-agent-1",
                config.ConnectionStringName,
                "Your role responsibility is to provide the user's name when requested.")
            {
                Queries = new List<AiAgentToolQuery>()
                {
                    new AiAgentToolQuery
                    {
                        Name = "GetUserName",
                        Description = "Get the user name",
                        // wrong index: 'NonExistentIndex' does not exist in the database
                        Query = "from index 'NonExistentIndex' " +
                                "where id() = $userId " +
                                "select Name",
                        ParametersSampleObject = "{}"
                    },
                }
            };
            userAgent.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            var userAgentId = (await store.AI.CreateAgentAsync(userAgent, MoviesSampleObject.Instance)).Identifier;

            // The root agent delegates the name lookup to the sub-agent above.
            var rootAgent = new AiAgentConfiguration("root-agent-1",
                config.ConnectionStringName,
                "You are a User Profile Agent. When asked about the user's name, delegate to the sub-agent.")
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent
                    {
                        Identifier = userAgentId,
                        Description = "Get the user name."
                    }
                ]
            };
            rootAgent.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            var rootAgentId = (await store.AI.CreateAgentAsync<MoviesSampleObject>(rootAgent, MoviesSampleObject.Instance)).Identifier;

            var chat = store.AI.Conversation(rootAgentId, "chats/1",
                new AiConversationCreationOptions().AddParameter("userId", "Users/1"));

            chat.SetUserPrompt("What is my name?");

            // The sub-agent's query failure must propagate all the way to the user
            // (instead of being swallowed into a tool message for the model).
            var e = await Assert.ThrowsAsync<QueryToolFailedException>(() => chat.RunAsync<MoviesSampleObject>(CancellationToken.None));
            Assert.Contains("failed with status code 404", e.Message);
            Assert.Contains("NonExistentIndex", e.Message);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task WrongIndexInSubAgentQuery_ThreeLayers(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using (var session = store.OpenAsyncSession())
            {
                foreach (var u in Users)
                    await session.StoreAsync(u);

                await session.SaveChangesAsync();
            }

            // Layer 3 (deepest): the only agent that actually runs a query, against an index that does not exist.
            var lastSubAgent = new AiAgentConfiguration("last-sub-agent",
                config.ConnectionStringName,
                "Your role responsibility is to provide the user's name when requested.")
            {
                Queries = new List<AiAgentToolQuery>()
                {
                    new AiAgentToolQuery
                    {
                        Name = "GetUserName",
                        Description = "Get the user name",
                        // wrong index: 'NonExistentIndex' does not exist in the database
                        Query = "from index 'NonExistentIndex' " +
                                "where id() = $userId " +
                                "select Name",
                        ParametersSampleObject = "{}"
                    },
                }
            };
            lastSubAgent.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            var lastSubAgentId = (await store.AI.CreateAgentAsync<MoviesSampleObject>(lastSubAgent, MoviesSampleObject.Instance)).Identifier;

            // Layer 2 (middle): delegates the name lookup to the deepest agent.
            var middleSubAgent = new AiAgentConfiguration("middle-sub-agent",
                config.ConnectionStringName,
                "Your role responsibility is to provide the user's name when requested. Delegate to the sub-agent.")
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent
                    {
                        Identifier = lastSubAgentId,
                        Description = "Get the user name."
                    }
                ]
            };
            middleSubAgent.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            var middleSubAgentId = (await store.AI.CreateAgentAsync<MoviesSampleObject>(middleSubAgent, MoviesSampleObject.Instance)).Identifier;

            // Layer 1 (root): delegates the name lookup to the middle agent.
            var rootAgent = new AiAgentConfiguration("sub-agent-root",
                config.ConnectionStringName,
                "You are a User Profile Agent. When asked about the user's name, delegate to the sub-agent.")
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent
                    {
                        Identifier = middleSubAgentId,
                        Description = "Get the user name."
                    }
                ]
            };
            rootAgent.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            var rootAgentId = (await store.AI.CreateAgentAsync<MoviesSampleObject>(rootAgent, MoviesSampleObject.Instance)).Identifier;

            var chat = store.AI.Conversation(rootAgentId, "chats/1",
                new AiConversationCreationOptions().AddParameter("userId", "Users/1"));

            chat.SetUserPrompt("What is my name?");

            // The query failure happens in the deepest sub-agent and must propagate up
            // through every layer all the way to the user - exactly like WrongIndexInSubAgentQuery.
            var e = await Assert.ThrowsAsync<QueryToolFailedException>(() => chat.RunAsync<MoviesSampleObject>(CancellationToken.None));
            Assert.Contains("failed with status code 404", e.Message);
            Assert.Contains("NonExistentIndex", e.Message);
        }
    }
}
