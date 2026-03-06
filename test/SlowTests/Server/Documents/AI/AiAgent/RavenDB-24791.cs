using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;


namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_24791 : RavenTestBase
    {
        public RavenDB_24791(ITestOutputHelper output) : base(output)
        {
        }

        private class QuestionOutputSchema
        {
            public static QuestionOutputSchema Instance = new();

            public string Answer = "Combined answer of the answers for the questions ";

            public List<string> RelevantQuestionsIds = ["The questions ids relevant to the query or response"];
        }

        private class Question
        {
            public string Id { get; set; }
            public string Author { get; set; }
            public int Priority { get; set; }
            public bool ShouldAnswer { get; set; }
            public string ActualQuestion { get; set; }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task ArrayParameter(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Question { Id = "Questions/16", Author = "Aviv", ActualQuestion = "Is Russia still at war?" });
                await session.StoreAsync(new Question { Id = "Questions/17", Author = "Karmel", ActualQuestion = "What time is it in Israel?" });
                await session.StoreAsync(new Question { Id = "Questions/18", Author = "Shahar", ActualQuestion = "What time is it in Italy?" });

                await session.SaveChangesAsync();
            }

            var agent = new AiAgentConfiguration("knowledge-agent",
                config.ConnectionStringName,
                "You are an ai agent that answer knowledge questions"
            )
            {
                Queries =
                [
                    new AiAgentToolQuery
                    {
                        Name = "QuestionsSearch",
                        Description = "search questions",
                        Query = "from 'Questions' where Author in ($authors)",
                        ParametersSampleObject = "{}"
                    }
                ]
            };

            agent.Parameters.Add(new AiAgentParameter("authors"));

            var createResult = await store.AI.CreateAgentAsync(agent, QuestionOutputSchema.Instance);
            var chat = store.AI.Conversation(
                createResult.Identifier,
                "Chats/",
                new AiConversationCreationOptions().AddParameter("authors", new string[] { "Aviv" }));

            chat.SetUserPrompt("answer the questions using the tool I provided you");
            var more = await chat.RunAsync<QuestionOutputSchema>(CancellationToken.None);
            Assert.True(more.Status == AiConversationResult.Done);

            var aviv = more.Answer;
            Assert.NotNull(aviv.Answer);

            using (var session = store.OpenAsyncSession())
            {
                var messages = (await session.LoadAsync<Chat>(chat.Id)).Messages;
                var toolCallAnswer = JsonConvert.DeserializeObject<List<Question>>(messages[4].Content.ToString()); // query results
                Assert.Equal(1, toolCallAnswer.Count);
                Assert.Equal("Aviv", toolCallAnswer.FirstOrDefault()?.Author);
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task IntParameter(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Question { Id = "Questions/16", Author = "Aviv", Priority = 10, ActualQuestion = "Is Russia still at war?" });
                await session.StoreAsync(new Question { Id = "Questions/17", Author = "Karmel", Priority = 20, ActualQuestion = "What time is it in Israel?" });
                await session.StoreAsync(new Question { Id = "Questions/18", Author = "Shahar", Priority = 30, ActualQuestion = "What time is it in Italy?" });

                await session.SaveChangesAsync();
            }

            var agent = new AiAgentConfiguration("knowledge-agent",
                config.ConnectionStringName,
                "You are an ai agent that answer knowledge questions"
            )
            {
                Queries =
                [
                    new AiAgentToolQuery
                    {
                        Name = "QuestionsSearch",
                        Description = "search questions",
                        Query = "from 'Questions' where Priority>=$priority",
                        ParametersSampleObject = "{}"
                    }
                ]
            };

            agent.Parameters.Add(new AiAgentParameter("priority"));
            int priority = 25;

            var createResult = await store.AI.CreateAgentAsync(agent, QuestionOutputSchema.Instance);
            var chat = store.AI.Conversation(
                createResult.Identifier,
                "Chats/",
                new AiConversationCreationOptions().AddParameter("priority", priority));

            chat.SetUserPrompt("answer the questions using the tool I provided you");
            var more = await chat.RunAsync<QuestionOutputSchema>(CancellationToken.None);
            Assert.True(more.Status == AiConversationResult.Done);

            var shahar = more.Answer;
            Assert.NotNull(shahar.Answer);

            using (var session = store.OpenAsyncSession())
            {
                var messages = (await session.LoadAsync<Chat>(chat.Id)).Messages;
                var toolCallAnswer = JsonConvert.DeserializeObject<List<Question>>(messages[4].Content.ToString()); // query results
                Assert.Equal(1, toolCallAnswer.Count);
                Assert.Equal("Shahar", toolCallAnswer.FirstOrDefault()?.Author);
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task DoubleParameter(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Question { Id = "Questions/16", Author = "Aviv", Priority = 10, ActualQuestion = "Is Russia still at war?" });
                await session.StoreAsync(new Question { Id = "Questions/17", Author = "Karmel", Priority = 20, ActualQuestion = "What time is it in Israel?" });
                await session.StoreAsync(new Question { Id = "Questions/18", Author = "Shahar", Priority = 30, ActualQuestion = "What time is it in Italy?" });

                await session.SaveChangesAsync();
            }

            var agent = new AiAgentConfiguration("knowledge-agent",
                config.ConnectionStringName,
                "You are an ai agent that answer knowledge questions"
            )
            {
                Queries =
                [
                    new AiAgentToolQuery
                    {
                        Name = "QuestionsSearch",
                        Description = "search questions",
                        Query = "from 'Questions' where Priority>=$priority",
                        ParametersSampleObject = "{}"
                    }
                ]
            };

            agent.Parameters.Add(new AiAgentParameter("priority"));
            double priority = 25.7;

            var createResult = await store.AI.CreateAgentAsync(agent, QuestionOutputSchema.Instance);
            var chat = store.AI.Conversation(
                createResult.Identifier,
                "Chats/",
                new AiConversationCreationOptions().AddParameter("priority", priority));

            chat.SetUserPrompt("answer the questions using the tool I provided you");
            var more = await chat.RunAsync<QuestionOutputSchema>(CancellationToken.None);
            Assert.True(more.Status == AiConversationResult.Done);

            var shahar = more.Answer;
            Assert.NotNull(shahar.Answer);

            using (var session = store.OpenAsyncSession())
            {
                var messages = (await session.LoadAsync<Chat>(chat.Id)).Messages;
                var toolCallAnswer = JsonConvert.DeserializeObject<List<Question>>(messages[4].Content.ToString()); // query results
                Assert.Equal(1, toolCallAnswer.Count);
                Assert.Equal("Shahar", toolCallAnswer.FirstOrDefault()?.Author);
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task BoolParameter(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Question { Id = "Questions/16", ShouldAnswer = false, Author = "Aviv", Priority = 10, ActualQuestion = "Is Russia still at war?" });
                await session.StoreAsync(new Question { Id = "Questions/17", ShouldAnswer = true, Author = "Karmel", Priority = 20, ActualQuestion = "What time is it in Israel?" });
                await session.StoreAsync(new Question { Id = "Questions/18", ShouldAnswer = false, Author = "Shahar", Priority = 30, ActualQuestion = "What time is it in Italy?" });

                await session.SaveChangesAsync();
            }

            var agent = new AiAgentConfiguration("knowledge-agent",
                config.ConnectionStringName,
                "You are an ai agent that answer knowledge questions"
            )
            {
                Queries =
                [
                    new AiAgentToolQuery
                    {
                        Name = "QuestionsSearch",
                        Description = "search questions",
                        Query = "from 'Questions' where ShouldAnswer==$shouldAnswer",
                        ParametersSampleObject = "{}"
                    }
                ]
            };
            agent.Parameters.Add(new AiAgentParameter("shouldAnswer"));

            var createResult = await store.AI.CreateAgentAsync(agent, QuestionOutputSchema.Instance);

            var chat = store.AI.Conversation(
                createResult.Identifier,
                "Chats/",
                new AiConversationCreationOptions().AddParameter("shouldAnswer", true));

            chat.SetUserPrompt("answer the questions using the tool I provided you");
            var more = await chat.RunAsync<QuestionOutputSchema>(CancellationToken.None);
            Assert.True(more.Status == AiConversationResult.Done);

            var shahar = more.Answer;
            Assert.NotNull(shahar.Answer);

            using (var session = store.OpenAsyncSession())
            {
                var messages = (await session.LoadAsync<Chat>(chat.Id)).Messages;
                var toolCallAnswer = JsonConvert.DeserializeObject<List<Question>>(messages[4].Content.ToString()); // query results
                Assert.Equal(1, toolCallAnswer.Count);
                Assert.Equal("Karmel", toolCallAnswer.FirstOrDefault()?.Author);
            }
        }

        private class Chat
        {
            public List<ChatMessage> Messages { get; set; }
        }

        public class ChatMessage
        {
            [JsonPropertyName("role")]
            public string Role { get; set; }

            [JsonPropertyName("content")]
            public JToken Content { get; set; }

            [JsonPropertyName("date")]
            public DateTime Date { get; set; }
        }
    }
}
