using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
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
    public class RavenDB_25869(ITestOutputHelper output) : RavenTestBase(output)
    {

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task SubAgentsParamsTests(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User() { Id = "Users/1", Name = "Shahar", WatchedMovies = new HashSet<string>() });
                await session.SaveChangesAsync();
            }

            var changeUserNameAgent = new AiAgentConfiguration(
                "change-user-name-agent",
                config.ConnectionStringName,
                "Responsible for changing the user's name. Validates the old name before update."
            )
            {
                Queries = new List<AiAgentToolQuery>
                {
                    new AiAgentToolQuery
                    {
                        Name = "GetUserName",
                        Description = "Get the current user name",
                        Query = "from Users where id() = $userId select Name",
                        ParametersSampleObject = "{}"
                    }
                },

                Actions = new List<AiAgentToolAction>
                {
                    new AiAgentToolAction(
                        "ChangeUserName",
                        "Change the user's name. Requires validation using the old name."
                    )
                    {
                        ParametersSampleObject =
                            JsonConvert.SerializeObject(ChangeUserNameSampleRequest.Instance)
                    }
                }
            };
            changeUserNameAgent.Parameters.Add(new AiAgentParameter("userId", "The id of the user whose name should be changed", 
                AiAgentParameterPolicy.ForbidModelGeneration));
            var changeUserNameAgentId = (await store.AI.CreateAgentAsync(changeUserNameAgent, MoviesSampleObject.Instance)).Identifier;


            var userProfileAgent = new AiAgentConfiguration(
                "user-profile-agent-1",
                config.ConnectionStringName,
                "Provides user profile capabilities and delegates name changes to a sub-agent."
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent
                    {
                        Identifier = changeUserNameAgentId,
                        Description =
                            "Use to get the user name and to change the user name after validation."
                    }
                ]
            };
            userProfileAgent.Parameters.Add(new AiAgentParameter("userId", "The current user id",
                AiAgentParameterPolicy.ForbidModelGeneration));
            var userProfileAgentId = (await store.AI.CreateAgentAsync(userProfileAgent, MoviesSampleObject.Instance)).Identifier;

            var chat = store.AI.Conversation(userProfileAgentId, "chats/1",
                new AiConversationCreationOptions().AddParameter("userId", "Users/1"));
            chat.Handle<ChangeUserNameSampleRequest>($"{changeUserNameAgentId}/ChangeUserName", (r) =>
            {
                Assert.Equal("Users/1", r.UserId);
                return new ActionToolResult
                {
                    IsSuccessful = true,
                    Answer = $"Name of user '{r.UserId}' changed from '{r.OldUserName}' to '{r.NewUserName}'"
                };
            });

            chat.SetUserPrompt("change my name from 'Shahar' to 'Aviv'");
            var r = await chat.RunAsync<MoviesSampleObject>();
            Assert.Equal(AiConversationResult.Done, r.Status);

            using (var session = store.OpenAsyncSession())
            {
                var doc = (await session.Advanced.LoadStartingWithAsync<Chat>("chats/1/" + changeUserNameAgentId)).First();
                var userMessages = doc.Messages.Where(m => m.Role == "user");
                Assert.True(userMessages.Any(a => a.Content is string content && content == "AI Agent Parameters:\nuserId = Users/1" + Environment.NewLine), doc.ToString());
            }


            var userProfileAgent2 = new AiAgentConfiguration(
                "user-profile-agent-2",
                config.ConnectionStringName,
                "Provides user profile capabilities and delegates name changes to a sub-agent."
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent
                    {
                        Identifier = changeUserNameAgentId,
                        Description =
                            "Use to get the user name and to change the user name after validation."
                    }
                ]
            };

            userProfileAgent2.Parameters.Add(new AiAgentParameter("currentUserId", "The current user id"));
            var userProfileAgent2Id = (await store.AI.CreateAgentAsync(userProfileAgent2, MoviesSampleObject.Instance)).Identifier;

            var chat2 = store.AI.Conversation(userProfileAgent2Id, "chats/2",
                new AiConversationCreationOptions().AddParameter("currentUserId", "Users/1"));
            chat2.Handle<ChangeUserNameSampleRequest>($"{changeUserNameAgentId}/ChangeUserName", r =>
                new ActionToolResult
                {
                    IsSuccessful = true,
                    Answer = $"Name of user '{r.UserId}' changed from '{r.OldUserName}' to '{r.NewUserName}'"
                }
            );

            chat2.SetUserPrompt("change my name from 'Shahar' to 'Aviv'");
            await Assert.ThrowsAsync<MissingAiAgentParameterException>(() => chat2.RunAsync<MoviesSampleObject>());

            changeUserNameAgent.Parameters.First(x => x.Name == "userId").Policy = AiAgentParameterPolicy.Default;
            await store.AI.CreateAgentAsync(changeUserNameAgent, MoviesSampleObject.Instance);
            var chat22 = store.AI.Conversation(userProfileAgent2Id, "chats/2",
                new AiConversationCreationOptions().AddParameter("currentUserId", "Users/1"));
            chat22.Handle<ChangeUserNameSampleRequest>($"{changeUserNameAgentId}/ChangeUserName", r =>
                new ActionToolResult
                {
                    IsSuccessful = true,
                    Answer = $"Name of user '{r.UserId}' changed from '{r.OldUserName}' to '{r.NewUserName}'"
                }
            );
            chat22.SetUserPrompt("change my name from 'Shahar' to 'Aviv'");
            var r22 = await chat22.RunAsync<MoviesSampleObject>();
            Assert.Equal(AiConversationResult.Done, r22.Status);

            using (var session = store.OpenAsyncSession())
            {
                var doc = (await session.Advanced.LoadStartingWithAsync<Chat>("chats/1/" + changeUserNameAgentId)).First();
                var userMessages = doc.Messages.Where(m => m.Role == "user").ToList();
                Assert.True(userMessages.Any(a => a.Content is string content && content.Contains("userId = ") && content.Contains("currentUserId = ") == false), doc.ToString());
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task MissingParamOnSubAgent(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User() { Id = "Users/1", Name = "Shahar", WatchedMovies = new HashSet<string>() });
                await session.SaveChangesAsync();
            }

            var changeUserNameAgent = new AiAgentConfiguration(
                "change-user-name-agent",
                config.ConnectionStringName,
                "Responsible for changing the user's name. Validates the old name before update."
            )
            {
                Queries = new List<AiAgentToolQuery>
                {
                    new AiAgentToolQuery
                    {
                        Name = "GetUserName",
                        Description = "Get the current user name",
                        Query = "from Users where id() = $userId select Name",
                        ParametersSampleObject = "{}"
                    }
                },

                Actions = new List<AiAgentToolAction>
                {
                    new AiAgentToolAction(
                        "ChangeUserName",
                        "Change the user's name. Requires validation using the old name."
                    )
                    {
                        ParametersSampleObject =
                            JsonConvert.SerializeObject(ChangeUserNameSampleRequest.Instance)
                    }
                }
            };
            changeUserNameAgent.Parameters.Add(new AiAgentParameter("userId", "The id of the user whose name should be changed"));
            var changeUserNameAgentId = (await store.AI.CreateAgentAsync(changeUserNameAgent, MoviesSampleObject.Instance)).Identifier;


            var userProfileAgent = new AiAgentConfiguration(
                "user-profile-agent-1",
                config.ConnectionStringName,
                "Provides user profile capabilities and delegates name changes to a sub-agent."
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent
                    {
                        Identifier = changeUserNameAgentId,
                        Description =
                            "Use to get the user name and to change the user name after validation."
                    }
                ]
            };
            userProfileAgent.Parameters.Add(new AiAgentParameter("userId", "The current user id"));
            var userProfileAgentId = (await store.AI.CreateAgentAsync(userProfileAgent, MoviesSampleObject.Instance)).Identifier;

            var chat = store.AI.Conversation(userProfileAgentId, "chats/1",
                new AiConversationCreationOptions().AddParameter("userId", "Users/1"));
            chat.Handle<ChangeUserNameSampleRequest>($"{changeUserNameAgentId}/ChangeUserName", (r) =>
            {
                Assert.Equal("Users/1", r.UserId);
                return new ActionToolResult
                {
                    IsSuccessful = true,
                    Answer = $"Name of user '{r.UserId}' changed from '{r.OldUserName}' to '{r.NewUserName}'"
                };
            });

            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            db.ForTestingPurposesOnly().ShouldAiAgentAddMutualParameterForSubAgentReq = (parent, paramName) =>
            {
                if (parent.Name == userProfileAgentId && paramName == "userId")
                    return false;

                return true;
            };

            chat.SetUserPrompt("change my name from 'Shahar' to 'Aviv'");
            await Assert.ThrowsAsync<MissingAiAgentParameterException>(() => chat.RunAsync<MoviesSampleObject>());
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task ThreeLevelsOfNesting(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
            await RavenDB_24887_2.CreateMoviesDatabase(store);

            var userAgent3 = new AiAgentConfiguration("user-info-agent-3",
                config.ConnectionStringName,
                "Your role responsibility is to provide the user's name when requested."
            )
            {
                Queries = new List<AiAgentToolQuery>()
            {
                new AiAgentToolQuery
                {
                    Name = "GetUserName",
                    Description = "Get the user name",
                    Query = "from Users " +
                            "where id() = $userId " +
                            "select Name",
                    ParametersSampleObject = "{}"
                },
            }
            };
            userAgent3.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            var userAgent3Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent3, MoviesSampleObject.Instance)).Identifier;


            var userAgent2 = new AiAgentConfiguration("user-info-agent-2",
                config.ConnectionStringName,
                "Your role responsibility is to provide the user's name when requested."
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent
                {
                    Identifier = userAgent3Id,
                    Description = "Use to ask about user name."
                }
                ]
            };
            userAgent2.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with", sendToModel: false));
            var userAgent2Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent2, MoviesSampleObject.Instance)).Identifier;


            var userAgent1 = new AiAgentConfiguration("user-info-agent-1",
                config.ConnectionStringName,
                "Your role responsibility is to provide the user's name when requested."
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent
                {
                    Identifier = userAgent2Id,
                    Description = "Use to ask about user name."
                }
                ]
            };
            userAgent1.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            var userAgent1Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent1, MoviesSampleObject.Instance)).Identifier;

            var chat = store.AI.Conversation(userAgent1Id, "chats/1",
                new AiConversationCreationOptions().AddParameter("userId", "Users/1"));
            chat.SetUserPrompt("Whats my name?");
            var r = await chat.RunAsync<MoviesSampleObject>();
            Assert.NotNull(r.Answer);
            Assert.Contains("shahar", r.Answer.Answer.ToLower());
            Assert.Equal(AiConversationResult.Done, r.Status);
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task MissingParamOnSubAgent3Levels(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User() { Id = "Users/1", Name = "Shahar", WatchedMovies = new HashSet<string>() });
                await session.SaveChangesAsync();
            }

            var changeUserNameAgent = new AiAgentConfiguration(
                "change-user-name-agent",
                config.ConnectionStringName,
                "Responsible for changing the user's name. Validates the old name before update."
            )
            {
                Queries = new List<AiAgentToolQuery>
                {
                    new AiAgentToolQuery
                    {
                        Name = "GetUserName",
                        Description = "Get the current user name",
                        Query = "from Users where id() = $userId select Name",
                        ParametersSampleObject = "{}"
                    }
                },

                Actions = new List<AiAgentToolAction>
                {
                    new AiAgentToolAction(
                        "ChangeUserName",
                        "Change the user's name. Requires validation using the old name."
                    )
                    {
                        ParametersSampleObject =
                            JsonConvert.SerializeObject(ChangeUserNameSampleRequest.Instance)
                    }
                }
            };
            changeUserNameAgent.Parameters.Add(new AiAgentParameter("userId", "The id of the user whose name should be changed"));
            var changeUserNameAgentId = (await store.AI.CreateAgentAsync(changeUserNameAgent, MoviesSampleObject.Instance)).Identifier;

            var userProfileAgent2 = new AiAgentConfiguration(
                "user-profile-agent-2",
                config.ConnectionStringName,
                "Provides user profile capabilities and delegates name changes to a sub-agent."
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent
                    {
                        Identifier = changeUserNameAgentId,
                        Description =
                            "Use to get the user name and to change the user name after validation."
                    }
                ]
            };
            userProfileAgent2.Parameters.Add(new AiAgentParameter("userId", "The current user id"));
            var userProfileAgent2Id = (await store.AI.CreateAgentAsync(userProfileAgent2, MoviesSampleObject.Instance)).Identifier;


            var userProfileAgent1 = new AiAgentConfiguration(
                "user-profile-agent-1",
                config.ConnectionStringName,
                "Provides user profile capabilities and delegates name changes to a sub-agent."
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent
                    {
                        Identifier = userProfileAgent2Id,
                        Description =
                            "Use to get the user name and to change the user name after validation."
                    }
                ]
            };
            userProfileAgent1.Parameters.Add(new AiAgentParameter("userId", "The current user id"));
            var userProfileAgent1Id = (await store.AI.CreateAgentAsync(userProfileAgent1, MoviesSampleObject.Instance)).Identifier;

            var chat = store.AI.Conversation(userProfileAgent1Id, "chats/1",
                new AiConversationCreationOptions().AddParameter("userId", "Users/1"));
            chat.Handle<ChangeUserNameSampleRequest>($"{changeUserNameAgentId}/ChangeUserName", (r) =>
            {
                Assert.Equal("Users/1", r.UserId);
                return new ActionToolResult
                {
                    IsSuccessful = true,
                    Answer = $"Name of user '{r.UserId}' changed from '{r.OldUserName}' to '{r.NewUserName}'"
                };
            });

            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            db.ForTestingPurposesOnly().ShouldAiAgentAddMutualParameterForSubAgentReq = (parent, paramName) =>
            {
                if (parent.Name == userProfileAgent1Id && paramName == "userId")
                    return false;

                return true;
            };

            chat.SetUserPrompt("change my name from 'Shahar' to 'Aviv'");
            await Assert.ThrowsAsync<MissingAiAgentParameterException>(() => chat.RunAsync<MoviesSampleObject>());
        }

        public class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public HashSet<string> WatchedMovies { get; set; }
        }

        public class ChangeUserNameSampleRequest
        {
            public static ChangeUserNameSampleRequest Instance = new()
            {
                UserId = "Users/123456789",
                OldUserName = "James Parker",
                NewUserName = "James Smith"
            };

            public string UserId { get; set; }
            public string NewUserName { get; set; }
            public string OldUserName { get; set; }
        }

        private class Chat
        {
            public List<Message> Messages { get; set; }

            public override string ToString()
            {
                return string.Join(", ", Messages);
            }
        }

        private class Message
        {
            [JsonProperty("role")]
            public string Role { get; set; }

            [JsonProperty("content")]
            public object Content { get; set; }

            public override string ToString()
            {
                string contentDescription = Content switch
                {
                    null => "null",
                    string s => $"\"{s}\"",
                    _ => $"<{Content.GetType().Name}>"
                };

                return $"{{ \"role\": \"{Role}\", \"content\": {contentDescription} }}";
            }
        }

        public class ActionToolResult
        {
            public bool IsSuccessful { get; set; }
            public string Answer { get; set; }
        }

        public class MoviesSampleObject
        {
            public static MoviesSampleObject Instance = new()
            {
                Answer = "Answer to the user question",
                MoviesIds = ["The movies ids relevant to the query or response"],
                MoviesNames = ["The movies names relevant to the query or response"]
            };

            public string Answer;

            public List<string> MoviesIds { get; set; }
            public List<string> MoviesNames { get; set; }
        }
    }
}
