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
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task WrongIndexInQuery(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using (var session = store.OpenAsyncSession())
            {
                foreach (var m in Movies)
                    await session.StoreAsync(m);

                foreach (var u in Users)
                    await session.StoreAsync(u);

                foreach (var r in Rates)
                    await session.StoreAsync(r);

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
            var e = await Assert.ThrowsAsync<AiException>(() => chat.RunAsync<MoviesSampleObject>(CancellationToken.None));
            var msg =
                "The request to '/databases/WrongIndexInQuery_1/queries' failed with status code 404 and returned an empty response body. Request: Query: from index 'NonExistentIndex' where id() = $userId select Name, QueryParameters: {\"userId\":\"Users/1\"}";
            Assert.Contains(msg, e.Message);
        }
    }
}
