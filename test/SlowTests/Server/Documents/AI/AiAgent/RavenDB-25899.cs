using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using static SlowTests.Server.Documents.AI.AiAgent.AiAgentBasics;


namespace SlowTests.Server.Documents.AI.AiAgent
{
    public class RavenDB_25899(ITestOutputHelper output) : RavenDB_24887_Base(output)
    {
        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task TestWithSubAgentDepthOf2(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
            await CreateMoviesDatabase(store);

            var userAgent = new AiAgentConfiguration("user-info-agent",
                config.ConnectionStringName,
                "Your role responsibility is to provide the user's name when requested OR change user name."
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

            var recommendationAgent = new AiAgentConfiguration("recommendation-agent",
                config.ConnectionStringName,
                "You are a User Movie Insights Agent in a movie recommendation system. " +
                "You can retrieve the following details:" + Environment.NewLine +
                "- The list of movies the user has watched, ordered by their ratings (highest to lowest)." + Environment.NewLine +
                "- The user’s top 3 genre affinities, ranked by score." + Environment.NewLine +
                "Always return results in a concise, structured way that can be used directly by the recommendation engine."
            )
            {
                Queries = new List<AiAgentToolQuery>()
                {
                    // watched list ordered by user rates
                    new AiAgentToolQuery
                    {
                        Name = "GetUserWatchedMoviesRates",
                        Description =
                            "Get user watched list movies ordered by the user rate - descending",
                        Query = "from Ratings as r " +
                                "where r.UserId = $userId " +
                                "order by r.RatingValue desc " +
                                "select { MovieId: r.MovieId, Title: load(r.MovieId).Title, Rating: r.RatingValue}",
                        ParametersSampleObject = "{}"
                    },

                    // top 3 genres for the user
                    new AiAgentToolQuery
                    {
                        Name = "GetUserAffinitiesByGenres",
                        Description =
                            "Get user genre affinities sorted by score, ordered by average Score (rating) - descending",
                        Query = "from index 'UserGenreAffinity' " +
                                "where UserId = $userId " +
                                "order by Score desc " +
                                "select Genre, Score, Count " +
                                "limit 0, 3",
                        ParametersSampleObject = "{}"
                    }
                }
            };
            recommendationAgent.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));

            var userAgentId = (await store.AI.CreateAgentAsync(userAgent, MoviesSampleObject.Instance)).Identifier;
            var recommendationAgentId = (await store.AI.CreateAgentAsync(recommendationAgent, MoviesSampleObject.Instance)).Identifier;

            var agent = new AiAgentConfiguration("movies-agent",
                config.ConnectionStringName,
                "You are a User Profile Agent in a movie recommendation and rating system. " +
                "Your purpose is to provide accurate, structured information about a specific user. " +
                "You can retrieve details such as: " + Environment.NewLine + "- The user’s profile (including watched movies). " + Environment.NewLine +
                "- The list of movies the user has rated, ordered by rating. " + Environment.NewLine +
                "- The user’s genre affinities, sorted by preference scores. " + Environment.NewLine +
                "Always return results in a clear, concise, and structured way that can be used directly by the movie recommendation system"
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent
                    {
                        Identifier = recommendationAgentId,
                        Description = "Use to ask: " + Environment.NewLine +
                                      "- The list of movies the user has rated, ordered by rating. " + Environment.NewLine +
                                      "- The user’s genre affinities, sorted by preference scores. "
                    },
                    new AiAgentToolSubAgent
                    {
                        Identifier = userAgentId,
                        Description = "Get the user name."
                    }
                ]
            };
            agent.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            var identifier = (await store.AI.CreateAgentAsync(agent, MoviesSampleObject.Instance)).Identifier;

            using var chat = new OnMemoryConversation(
                store, agent, identifier, conversationId: null,
                new AiConversationCreationOptions().AddParameter("userId", "Users/1"));

            // var chat = store.AI.Conversation(identifier, "chats/",
            //     new AiConversationCreationOptions().AddParameter("userId", "Users/1"));

            chat.Handle<ChangeUserNameSampleRequest, ActionToolResult>("user-info-agent/ChangeUserName", 
                r => ChangeUserNameAsync(store, r));

            chat.SetUserPrompt("Whats my name?");
            var r = await chat.RunAsync<ActionToolResult>();
            Assert.Contains("Shahar Hikri", r.Answer.Answer.ToString());

            chat.SetUserPrompt("Can you change my name from 'Shahar Hikri' to 'Aviv Rachmani'?");
            r = await chat.RunAsync<ActionToolResult>();
            using (var session = store.OpenAsyncSession())
            {
                var name = (await session.LoadAsync<User>("Users/1")).Name;
                Assert.Equal("Aviv Rachmani", name);
            }

            chat.SetUserPrompt("Can you check my name again? whats my name?");
            r = await chat.RunAsync<ActionToolResult>();
            Assert.Contains("Aviv Rachmani", r.Answer.Answer.ToString());
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task TestWithSubAgentDepthOf3(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
            await CreateMoviesDatabase(store);

            var userAgent3 = new AiAgentConfiguration("user-info-agent-3",
                config.ConnectionStringName,
                "Your role responsibility is to provide the user's name when requested OR change user name."
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
            userAgent3.Parameters.Add(new AiAgentParameter("userId", "the id of the requested user"));
            var userAgent3Id = (await store.AI.CreateAgentAsync(userAgent3, MoviesSampleObject.Instance)).Identifier;

            var userAgent1 = new AiAgentConfiguration("user-info-agent-1",
                config.ConnectionStringName,
                "Your role responsibility is to provide the user's name when requested OR change user name."
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent
                    {
                        Identifier = userAgent3Id,
                        Description = "Use to get user name and to change user name."
                    }
                ]
            };
            userAgent1.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            var userAgent1Id = (await store.AI.CreateAgentAsync(userAgent1, MoviesSampleObject.Instance)).Identifier;

            var userAgent0 = new AiAgentConfiguration("user-info-agent-0",
                config.ConnectionStringName,
                "Your role responsibility is to provide the user's name when requested OR change user name."
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent
                    {
                        Identifier = userAgent1Id,
                        Description = "Use to get user name and to change user name."
                    }
                ]
            };
            userAgent0.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            var userAgent0Id = (await store.AI.CreateAgentAsync(userAgent0, MoviesSampleObject.Instance)).Identifier;


            using var chat = new OnMemoryConversation(
                store, userAgent0, userAgent0Id, conversationId: null,
                new AiConversationCreationOptions().AddParameter("userId", "Users/1"));


            chat.Handle<ChangeUserNameSampleRequest, ActionToolResult>("user-info-agent-1/user-info-agent-3/ChangeUserName",
                r => ChangeUserNameAsync(store, r));

            chat.SetUserPrompt("Whats my name?");
            var r = await chat.RunAsync<ActionToolResult>();
            Assert.Contains("Shahar Hikri", r.Answer.Answer.ToString());

            chat.SetUserPrompt("Can you change my name from 'Shahar Hikri' to 'Aviv Rachmani'?");
            r = await chat.RunAsync<ActionToolResult>();
            using (var session = store.OpenAsyncSession())
            {
                var name = (await session.LoadAsync<User>("Users/1")).Name;
                Assert.Equal("Aviv Rachmani", name);
            }

            chat.SetUserPrompt("Can you check my name again? whats my name?");
            r = await chat.RunAsync<ActionToolResult>();
            Assert.Contains("Aviv Rachmani", r.Answer.Answer.ToString());
        }


        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task TestSubAgent2OpenActionTools2Agents(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
            await CreateMoviesDatabase(store);

            var userAgent2a = new AiAgentConfiguration("user-info-agent-2a",
                config.ConnectionStringName,
                "You are authorized to edit the user's name and to create movie-rating records associated with the user." +
                "Use exclusively the 'ChangeUserName' tool for changing the user's name. " +
                "Use exclusively the 'RateMovie' tool for adding a movie-rating record (rating a movie). " +
                "Do not perform these actions in any other way."
            )
            {
                Actions = new List<AiAgentToolAction>()
            {
                new AiAgentToolAction("ChangeUserName",
                    "Updates the name of the current user interacting with the AI agent. have to send also the old name for validation.")
                {
                    ParametersSampleObject = JsonConvert.SerializeObject(ChangeUserNameSampleRequest.Instance)
                },
                new AiAgentToolAction("RateMovie",
                    "Add movie rate required movie name and rate value between 0 to 5 (can be double, doesn't has to be integer)")
                {
                    ParametersSampleObject = JsonConvert.SerializeObject(RateToolSampleRequest.Instance)
                },
            }
            };
            userAgent2a.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            var userAgent2aId = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent2a, MoviesSampleObject.Instance)).Identifier;

            var userAgent2b = new AiAgentConfiguration("user-info-agent-2b",
                config.ConnectionStringName,
                "You are authorized to add movies to user watched List." +
                "Use exclusively the 'AddToMoviesWatchedList' tool for adding a movie to user's watched list. " +
                "Do not perform these action in any other way."
            )
            {
                Actions = new List<AiAgentToolAction>()
            {
                new AiAgentToolAction("AddToMovieWatchedList",
                    "Use for adding a movie to user watched list.")
                {
                    ParametersSampleObject = JsonConvert.SerializeObject(AddMovieToWatchedListSampleRequest.Instance)
                }
            }
            };
            userAgent2b.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            var userAgent2bId = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent2b, MoviesSampleObject.Instance)).Identifier;

            var userAgent1 = new AiAgentConfiguration("user-info-agent-1",
                config.ConnectionStringName,
                "You are a User Profile Agent on movies rating system."
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent
                {
                    Identifier = userAgent2aId,
                    Description = "Use for adding movie rate for the current user you talking with OR changing user name (cannot ask for both at once)"
                },
                new AiAgentToolSubAgent
                {
                    Identifier = userAgent2bId,
                    Description = "Use for adding a movie to user watched list (can add one movie per request). "
                }
                ]
            };
            userAgent1.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            var userAgent1Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent1, MoviesSampleObject.Instance)).Identifier;


            using var chat = new OnMemoryConversation(
                store, userAgent1, userAgent1Id, conversationId: null,
                new AiConversationCreationOptions().AddParameter("userId", "Users/1"));

            chat.Handle<ChangeUserNameSampleRequest, ActionToolResult>("user-info-agent-2a/ChangeUserName", async (r) =>
            {
                var res = (await ChangeUserNameAsync(store, r)) as ActionToolResult;
                // Console.WriteLine(res.Answer);
                return res;
            });
            chat.Handle<RateToolSampleRequest, ActionToolResult>("user-info-agent-2a/RateMovie", async (r) =>
            {
                var res = await RateMovieAsync(store, "Users/1", r) as ActionToolResult;
                // Console.WriteLine(res.Answer);
                return res;
            });
            chat.Handle<AddMovieToWatchedListSampleRequest, ActionToolResult>("user-info-agent-2b/AddToMovieWatchedList", async (r) =>
            {
                var res = await AddMovieAsync(store, "Users/1", r) as ActionToolResult;
                return res;
            });

            chat.SetUserPrompt("Please rate the movie \"Toy Story\" as 5, add the movie Nixon and Sudden Death to my watched list, change my name from 'Shahar Hikri' to 'Aviv Rachmani', and also please add Sudden Death to my watched list");
            var r = await chat.RunAsync<MoviesSampleObject>();
            Assert.Equal(AiConversationResult.Done, r.Status);

            using (var session = store.OpenAsyncSession())
            {
                var u = await session.LoadAsync<User>("Users/1");
                Assert.Equal("Aviv Rachmani", u.Name);
                Assert.Equal(5, u.WatchedMovies.Count);
            }
            Assert.Equal(Rates.Count + 1, (await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation())).Collections["Ratings"]);

            chat.SetUserPrompt("Please rate the movie \"Toy Story\" as 4, add the movie Casino and Sudden Death to my watched list, change my name from 'Aviv Rachmani' to 'Omer Adam'");
            r = await chat.RunAsync<MoviesSampleObject>();
            Assert.Equal(AiConversationResult.Done, r.Status);

            using (var session = store.OpenAsyncSession())
            {
                var u = await session.LoadAsync<User>("Users/1");
                Assert.Equal("Omer Adam", u.Name);
                Assert.Equal(6, u.WatchedMovies.Count);
            }
            Assert.Equal(Rates.Count + 2, (await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation())).Collections["Ratings"]);
        }

        private class OnMemoryConversation : IDisposable
        {
            private Dictionary<string, BlittableJsonReaderObject> _documents = null;
            private readonly Dictionary<string, Func<string, Task<object>>> _invocations = new();
            private string _userPrompt = null;
            private List<AiAgentActionResponse> _actionResponses = null;
            private JsonOperationContext _context;
            private IDisposable _disposable;

            private readonly DocumentStore _store;
            private readonly AiAgentConfiguration _agent;
            private readonly string _conversationId;
            private readonly AiConversationCreationOptions _creationOptions;

            public OnMemoryConversation(DocumentStore store, AiAgentConfiguration agent, string agentId, string conversationId = null, AiConversationCreationOptions creationOptions = null)
            {
                _store = store ?? throw new ArgumentNullException(nameof(store));
                _agent = agent ?? throw new ArgumentNullException(nameof(agent));
                agent.Identifier = agentId ?? agent.Identifier ?? throw new ArgumentNullException(nameof(agentId));
                _conversationId = conversationId ?? "RootConversation";
                _creationOptions = creationOptions;

                _disposable = _store.GetRequestExecutor().ContextPool.AllocateOperationContext(out _context);
            }

            public void SetUserPrompt(string prompt)
            {
                _userPrompt = prompt;
            }

            public async Task<AiAnswer<TAnswer>> RunAsync<TAnswer>()
            {
                if (_userPrompt == null)
                    throw new InvalidOperationException("Set user prompt first");

                while (true)
                {
                    if (_userPrompt == null && _actionResponses == null)
                        throw new InvalidOperationException("No user prompt or action responses available");

                    var r = await _store.Maintenance.SendAsync(new RunTestConversationOperation<TAnswer>(
                        _context, _agent, _conversationId, documents: _documents, _userPrompt, _creationOptions, _actionResponses)) as TestResult<TAnswer>;

                    if (r == null) 
                        throw new InvalidOperationException("Failed to get response");

                    ExchangeDocuments(r.Documents);
                    _userPrompt = null;

                    if (r.ActionRequests != null && r.ActionRequests.Count > 0)
                    {
                        await InvokeActionsAsync(r.ActionRequests).ConfigureAwait(false);
                        continue;
                    }

                    _actionResponses = null;

                    if (_documents.TryGetValue(_conversationId, out BlittableJsonReaderObject doc) == false)
                        throw new InvalidOperationException($"Failed to get conversation document with id {_conversationId}");

                    doc.TryGet("Messages", out BlittableJsonReaderArray messages);
                    var lastMessage = messages[^1] as BlittableJsonReaderObject;

                    if (lastMessage == null || lastMessage.TryGet("content", out BlittableJsonReaderObject content) == false)
                        throw new InvalidOperationException("Failed to get answer from the last message");

                    return new AiAnswer<TAnswer>
                    {
                        Answer = _store.GetRequestExecutor().Conventions.Serialization.DefaultConverter.FromBlittable<TAnswer>(content),
                        Status = AiConversationResult.Done,
                        Usage = r.TotalUsage, Elapsed = r.Elapsed
                    };
                }
            }

            private void ExchangeDocuments(Dictionary<string, BlittableJsonReaderObject> newDocs)
            {
                foreach (var kvp in _documents ?? [])
                {
                    if (newDocs.ContainsKey(kvp.Key) == false)
                        kvp.Value.Dispose();
                }

                _documents = newDocs;
            }

            private async Task InvokeActionsAsync(List<AiAgentActionRequest> actionRequests)
            {
                var actionResponses = new List<AiAgentActionResponse>();

                foreach (var request in actionRequests)
                {
                    if (_invocations.TryGetValue(request.Name, out var action) == false)
                        throw new InvalidOperationException($"No action registered for '{request.Name}'");

                    var result = await action(request.Arguments);
                    actionResponses.Add(new AiAgentActionResponse
                    {
                        ToolId = request.ToolId,
                        Content = JsonConvert.SerializeObject(result)
                    });
                }

                _actionResponses = actionResponses;
            }

            public void Handle<TArgs, TResult>(string actionName, Func<TArgs, Task<TResult>> action)
                where TArgs : class
                where TResult : class
            {
                if (_invocations.ContainsKey(actionName))
                    throw new InvalidOperationException($"Action '{actionName}' already exists.");

                _invocations[actionName] = async argsAsString =>
                {
                    var args = JsonConvert.DeserializeObject<TArgs>(argsAsString);
                    var result = await action(args);
                    return (TResult)result;
                };
            }

            public void Dispose()
            {
                _disposable?.Dispose();
            }
        }

    }
}

