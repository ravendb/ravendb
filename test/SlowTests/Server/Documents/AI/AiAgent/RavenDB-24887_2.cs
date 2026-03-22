using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class RavenDB_24887_2(ITestOutputHelper output) : RavenDB_24887_Base(output)
{
    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanCallOneAgentFromAnother(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
        await CreateMoviesDatabase(store);

        var userAgent = new AiAgentConfiguration("user-info-agent",
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

        var identifier = (await store.AI.CreateAgentAsync<MoviesSampleObject>(agent, MoviesSampleObject.Instance)).Identifier;

        var chat = store.AI.Conversation(identifier, "chats/1",
            new AiConversationCreationOptions().AddParameter("userId", "Users/1"));
        chat.SetUserPrompt("Whats my name and my favorite genres?");
        var r = await chat.RunAsync<MoviesSampleObject>();
        Assert.Equal(AiConversationResult.Done, r.Status);
        Assert.NotNull(r.Answer);
        Assert.Contains("shahar", r.Answer.ToString().ToLower());
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanCallOneAgentFromAnotherWithError(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
        await CreateMoviesDatabase(store);

        var userAgent = new AiAgentConfiguration("user-info-agent",
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
        userAgent.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));

        var recommendationAgent = new AiAgentConfiguration("recommendation-agent",
            config.ConnectionStringName,
            "You are a User Movie Insights Agent in a movie recommendation system. " +
            "You can retrieve details such as:" + Environment.NewLine +
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
                    Identifier = userAgentId,
                    Description = "Use to ask: " + Environment.NewLine +
                                  "- The list of movies the user has rated, ordered by rating. " + Environment.NewLine +
                                  "- The user’s genre affinities, sorted by preference scores. "
                },
                new AiAgentToolSubAgent
                {
                    Identifier = recommendationAgentId,
                    Description = "Use to ask about user profile, such as: user name and watched list."
                }
            ]
        };
        agent.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
        var identifier = (await store.AI.CreateAgentAsync<MoviesSampleObject>(agent, MoviesSampleObject.Instance)).Identifier;

        var db = await GetDatabase(store.Database);
        db.ForTestingPurposesOnly().BeforeAiAgentTalk = document =>
        {
            if (document.Agent == "user-info-agent")
            {
                // throw on sub-agent
                throw new RefusedToAnswerException("test")
                {
                    RequestId = "test123"
                };
            }

        };

        var chat = store.AI.Conversation(identifier, "chats/1",
            new AiConversationCreationOptions().AddParameter("userId", "Users/1"));
        chat.SetUserPrompt("Whats my name and my favorite genres?");
        var r = await chat.RunAsync<MoviesSampleObject>();

        //check if the sub-agent "user-info-agent" error is consumed
        Assert.Equal(AiConversationResult.Done, r.Status);
        Assert.NotNull(r.Answer);

        const string expectedFailingToolCallAnswer = "Error has been occurred during the tool call execution: ";
        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<Chat>("chats/1");
            var toolCallsAnswers = doc.Messages.Where(m => m.Role == "tool");
            Assert.True(toolCallsAnswers.Any(a => a.Content is string content && content.StartsWith(expectedFailingToolCallAnswer)));
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ThreeLevelsOfNesting(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
        await CreateMoviesDatabase(store);

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
        userAgent2.Parameters.Add(new AiAgentParameter("theUserId", "the id of the current user that you talk with"));
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
        Assert.Equal(AiConversationResult.Done, r.Status);
        Assert.NotNull(r.Answer);
        Assert.Contains("shahar", r.Answer.ToString().ToLower());
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ActionToolsOnSubAgentWithDifferentParam(Options options, GenAiConfiguration config)
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
        userAgent3.Parameters.Add(new AiAgentParameter("userId", "the id of the requested user, not his hold Name (equals to 'currentUserId')"));
        var userAgent3Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent3, MoviesSampleObject.Instance)).Identifier;

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
        userAgent1.Parameters.Add(new AiAgentParameter("currentUserId", "the id of the current user that you talk with"));
        var userAgent1Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent1, MoviesSampleObject.Instance)).Identifier;

        var chat = store.AI.Conversation(userAgent1Id, "chats/1",
            new AiConversationCreationOptions().AddParameter("currentUserId", "Users/1"));
        chat.Handle<ChangeUserNameSampleRequest, ActionToolResult>("user-info-agent-3/ChangeUserName", async (r) =>
        {
            return await ChangeUserNameAsync(store, r);
        });

        chat.SetUserPrompt("Can you change my name from 'Shahar Hikri' to 'Aviv Rachmani'?");
        var r = await chat.RunAsync<MoviesSampleObject>();
        Assert.Equal(AiConversationResult.Done, r.Status);

        using (var session = store.OpenAsyncSession())
        {
            var name = (await session.LoadAsync<User>("Users/1")).Name;
            Assert.Equal("Aviv Rachmani", name);
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ThreeLevelsOfNesting_endless_loop(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
        await CreateMoviesDatabase(store);

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
                    Description = "Get the user name"
                }
            ]
        };
        userAgent2.Parameters.Add(new AiAgentParameter("theUserId", "the id of the current user that you talk with"));
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
                    Description =  "Get the user name"
                }
            ]
        };
        userAgent1.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
        var userAgent1Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent1, MoviesSampleObject.Instance)).Identifier;

        var chat = store.AI.Conversation(userAgent1Id, "chats/1",
            new AiConversationCreationOptions().AddParameter("userId", "Users/1"));
        chat.SetUserPrompt("Whats my name?");
        var r = await chat.RunAsync<MoviesSampleObject>();
        Assert.Equal(AiConversationResult.Done, r.Status);
        Assert.NotNull(r.Answer);
        Assert.Contains("shahar", r.Answer.ToString().ToLower());
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task SubAgent2OpenActionTools(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
        await CreateMoviesDatabase(store);

        var userAgent2 = new AiAgentConfiguration("user-info-agent-2",
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
        userAgent2.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
        var userAgent2Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent2, MoviesSampleObject.Instance)).Identifier;


        var userAgent1 = new AiAgentConfiguration("user-info-agent-1",
            config.ConnectionStringName, GetSystemPrompt(userAgent2Id, false)
        )
        {
            SubAgents =
            [
                new AiAgentToolSubAgent
                {
                    Identifier = userAgent2Id,
                    Description = "Use for adding movie rate for the current user you talking with OR changing user name (cannot ask for both at once)"
                }
            ]
        };
        userAgent1.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
        var userAgent1Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent1, MoviesSampleObject.Instance)).Identifier;

        var chat = store.AI.Conversation(userAgent1Id, "chats/",
            new AiConversationCreationOptions().AddParameter("userId", "Users/1"));
        chat.Handle<ChangeUserNameSampleRequest, ActionToolResult>("user-info-agent-2/ChangeUserName", async (r) =>
        {
            var res = (await ChangeUserNameAsync(store, r)) as ActionToolResult;
            // Console.WriteLine(res.Answer);
            return res;
        });
        chat.Handle<RateToolSampleRequest, ActionToolResult>("user-info-agent-2/RateMovie", async (r) =>
        {
            var res = await RateMovieAsync(store, "Users/1", r) as ActionToolResult;
            // Console.WriteLine(res.Answer);
            return res;
        });

        chat.SetUserPrompt("Can you rate the movie \"Toy Story\" as 5 and change my name from 'Shahar Hikri' to 'Aviv Rachmani'?");
        var r = await chat.RunAsync<MoviesSampleObject>();
        Assert.Equal(AiConversationResult.Done, r.Status);

        using (var session = store.OpenAsyncSession())
        {
            var u = await session.LoadAsync<User>("Users/1");
            Assert.Equal("Aviv Rachmani", u.Name);
        }
        Assert.Equal(Rates.Count + 1, (await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation())).Collections["Ratings"]);

        chat.SetUserPrompt("Can you rate the movie \"Toy Story\" as 4 and change my name from 'Aviv Rachmani' to 'Omer Adam'?");
        r = await chat.RunAsync<MoviesSampleObject>();
        Assert.Equal(AiConversationResult.Done, r.Status);
        
        using (var session = store.OpenAsyncSession())
        {
            var u = await session.LoadAsync<User>("Users/1");
            Assert.Equal("Omer Adam", u.Name);
        }
        Assert.Equal(Rates.Count + 2, (await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation())).Collections["Ratings"]);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CheckMaxToolIterationsLimitOnSubAgent(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
        await CreateMoviesDatabase(store);

        var userAgent2 = new AiAgentConfiguration("user-info-agent-2",
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
        userAgent2.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
        var userAgent2Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent2, MoviesSampleObject.Instance)).Identifier;


        var userAgent1 = new AiAgentConfiguration("user-info-agent-1",
            config.ConnectionStringName,
            GetSystemPrompt(userAgent2Id, oncePrompt: false)
        )
        {
            SubAgents =
            [
                new AiAgentToolSubAgent
                {
                    Identifier = userAgent2Id,
                    Description = "Use for adding movie rate for the current user you talking with OR changing user name (cannot ask for both at once)"
                }
            ],
            MaxModelIterationsPerCall = 1
        };
        userAgent1.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
        var userAgent1Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent1, MoviesSampleObject.Instance)).Identifier;

        var chat = store.AI.Conversation(userAgent1Id, "chats/",
            new AiConversationCreationOptions().AddParameter("userId", "Users/1"));
        chat.Handle<ChangeUserNameSampleRequest, ActionToolResult>("user-info-agent-2/ChangeUserName", async (r) =>
        {
            var res = (await ChangeUserNameAsync(store, r)) as ActionToolResult;
            // Console.WriteLine(res.Answer);
            return res;
        });
        chat.Handle<RateToolSampleRequest, ActionToolResult>("user-info-agent-2/RateMovie", async (r) =>
        {
            var res = await RateMovieAsync(store, "Users/1", r) as ActionToolResult;
            // Console.WriteLine(res.Answer);
            return res;
        });

        chat.SetUserPrompt("Can you rate the movie \"Toy Story\" as 5 and change my name from 'Shahar Hikri' to 'Aviv Rachmani'?");
        var r = await chat.RunAsync<MoviesSampleObject>();
        Assert.Equal(AiConversationResult.Done, r.Status);

        // check 2 conversation (1 sub-conversations)
        Assert.Equal(2, (await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation())).Collections["@conversations"]);

        // check that nothing has been changed
        using (var session = store.OpenAsyncSession())
        {
            var u = await session.LoadAsync<User>("Users/1");
            Assert.Equal("Shahar Hikri", u.Name);
        }
        Assert.Equal(Rates.Count, (await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation())).Collections["Ratings"]);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task SubAgent2OpenActionToolsAtOnce(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
        await CreateMoviesDatabase(store);

        var userAgent2 = new AiAgentConfiguration("user-info-agent-2",
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
        userAgent2.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
        var userAgent2Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent2, MoviesSampleObject.Instance)).Identifier;


        var userAgent1 = new AiAgentConfiguration("user-info-agent-1",
            config.ConnectionStringName,
            "You are a User Profile Agent on movies rating system."
        )
        {
            SubAgents =
            [
                new AiAgentToolSubAgent
                {
                    Identifier = userAgent2Id,
                    Description = "Provide BOTH the movie rating and the new username together in one call. You can't change name without rating a movie and you cant rate a movie without changing the name. you have to ask for both in the call otherwise the tool call will fail.",
                }
            ]
        };
        userAgent1.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
        var userAgent1Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent1, MoviesSampleObject.Instance)).Identifier;

        var chat = store.AI.Conversation(userAgent1Id, "chats/",
            new AiConversationCreationOptions().AddParameter("userId", "Users/1"));
        chat.Handle<ChangeUserNameSampleRequest, ActionToolResult>("user-info-agent-2/ChangeUserName", async (r) =>
        {
            var res = (await ChangeUserNameAsync(store, r)) as ActionToolResult;
            // Console.WriteLine(res.Answer);
            return res;
        });
        chat.Handle<RateToolSampleRequest, ActionToolResult>("user-info-agent-2/RateMovie", async (r) =>
        {
            var res = await RateMovieAsync(store, "Users/1", r) as ActionToolResult;
            // Console.WriteLine(res.Answer);
            return res;
        });

        chat.SetUserPrompt("Can you rate the movie \"Toy Story\" as 5 and change my name from 'Shahar Hikri' to 'Aviv Rachmani'?");
        var r = await chat.RunAsync<MoviesSampleObject>();
        Assert.Equal(AiConversationResult.Done, r.Status);

        using (var session = store.OpenAsyncSession())
        {
            var u = await session.LoadAsync<User>("Users/1");
            Assert.Equal("Aviv Rachmani", u.Name);
        }
        Assert.Equal(Rates.Count + 1, (await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation())).Collections["Ratings"]);

        chat.SetUserPrompt("Can you rate the movie \"Toy Story\" as 4 and change my name from 'Aviv Rachmani' to 'Omer Adam'?");
        r = await chat.RunAsync<MoviesSampleObject>();
        Assert.Equal(AiConversationResult.Done, r.Status);
        
        using (var session = store.OpenAsyncSession())
        {
            var u = await session.LoadAsync<User>("Users/1");
            Assert.Equal("Omer Adam", u.Name);
        }
        Assert.Equal(Rates.Count + 2, (await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation())).Collections["Ratings"]);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task SubAgent2OpenActionTools2Agents(Options options, GenAiConfiguration config)
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

        var chat = store.AI.Conversation(userAgent1Id, "chats/",
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

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task SubAgentWitAnotherActionCallOnTheFirstActionCallResponse(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
        await CreateMoviesDatabase(store);

        var userAgent2a = new AiAgentConfiguration("user-info-agent-2a",
            config.ConnectionStringName,
            "You are authorized to edit the user's name and to create movie-rating records associated with the user." +
            "Use exclusively the 'ChangeUserName' tool for changing the user's name. " +
            "Do not perform these actions in any other way."
        )
        {
            Actions = new List<AiAgentToolAction>()
            {
                new AiAgentToolAction("ChangeUserName",
                    "Changes the user's name. Always provide both OldUserName and NewUserName. " +
                    "This tool MUST be called strictly one time per assistant message. " +
                    "Never send more than ONE tool call in a single assistant response. " +
                    "If the user requests multiple name changes in one message, you must execute them SEQUENTIALLY: " +
                    "1) Send EXACTLY ONE ChangeUserName tool call for the first change. " +
                    "2) Wait for the tool's response. " +
                    "3) Only after receiving that response, send EXACTLY ONE ChangeUserName tool call for the second change. " +
                    "4) Do NOT answer the user until ALL required tool calls are completed. " +
                    "If you attempt to handle multiple changes, you must break them into multiple assistant messages, each containing exactly one tool call. " +
                    "Never combine multiple ChangeUserName operations into a single tool_calls array.")
                {
                    ParametersSampleObject = JsonConvert.SerializeObject(ChangeUserNameSampleRequest.Instance)
                }
            }
        };
        userAgent2a.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
        var userAgent2aId = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent2a, MoviesSampleObject.Instance)).Identifier;

        var userAgent1 = new AiAgentConfiguration("user-info-agent-1",
            config.ConnectionStringName,
            "You are a routing agent. Your only job is to forward the user's message EXACTLY as written (verbatim, without any modification, summary, interpretation, or additional wording) to the sub-agent 'user-info-agent-2a' using its tool call.\n" +
            "Do NOT answer the user directly.\n" +
            "Do NOT process, interpret, rewrite, or summarize the user's message.\n" +
            "Do NOT add explanations, comments, or reasoning.\n\n" +
            "Always trigger exactly ONE tool call — the sub-agent — and pass the user's message as-is in the parameter 'subAgentUserPrompt'.\n\n" +
            "If the user writes:\n" +
            "\"hello, I want to change my name and also rate a movie\"\n\n" +
            "You MUST send exactly:\n" +
            "\"hello, I want to change my name and also rate a movie\"\n\n" +
            "Your ONLY task: forward the raw user message to the sub-agent tool and return its response to the user."
        )
        {
            SubAgents =
            [
                new AiAgentToolSubAgent
                {
                    Identifier = userAgent2aId,
                    Description = "Handle the user's request exactly as it is received. " +
                                  "The parent agent forwards the user's raw prompt verbatim. " +
                                  "Treat the incoming 'subAgentUserPrompt' as if it was written directly by the user. " +
                                  "Do not expect structured parameters unless the user explicitly provided them. " +
                                  "Fully process the user's request and return the appropriate response."
                }
            ]
        };
        userAgent1.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
        var userAgent1Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent1, MoviesSampleObject.Instance)).Identifier;

        var chat = store.AI.Conversation(userAgent1Id, "chats/",
            new AiConversationCreationOptions().AddParameter("userId", "Users/1"));
        chat.Handle<ChangeUserNameSampleRequest, ActionToolResult>("user-info-agent-2a/ChangeUserName", async (r) =>
        {
            var res = (await ChangeUserNameAsync(store, r)) as ActionToolResult;
            // Console.WriteLine(res.Answer);
            return res;
        });

        var msg = "change my name from Shahar Hikri to Aviv Rahmani, and then change my name to Aviv Rahmani to Omer Adam";
        chat.SetUserPrompt(msg);
        var r = await chat.RunAsync<MoviesSampleObject>();
        Assert.Equal(AiConversationResult.Done, r.Status);
    }


    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ResumeChatAfterError(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
        await CreateMoviesDatabase(store);

        var userAgent = new AiAgentConfiguration("user-info-agent",
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
        userAgent.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));

        var recommendationAgent = new AiAgentConfiguration("recommendation-agent",
            config.ConnectionStringName,
            "You are a User Movie Insights Agent in a movie recommendation system. " +
            "You can retrieve details such as:" + Environment.NewLine +
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
            "Your purpose is to provide accurate and structured information about a specific user. " +

            "You MUST use the available sub-agents to retrieve all information. " +

            "Use 'recommendation-agent' for watched movies, rated movies, and genre affinities. " +
            "Use 'user-info-agent' only when the user explicitly asks for their name. " +

            "Do not retrieve or infer the user name unless explicitly requested. " +
            "Do not ask the user for additional information. " +
            "Do not answer from your own knowledge. " +

            "You may call multiple sub-agents step-by-step if needed, and must base your final answer only on their responses. " +

            "Always return results in a clear, concise, and structured format suitable for direct use by the system."
        )
        {
            SubAgents =
            [
                new AiAgentToolSubAgent
                {
                    Identifier = userAgentId,
                    Description = "MUST be used only when the user explicitly asks for their name."
                },
                new AiAgentToolSubAgent
                {
                    Identifier = recommendationAgentId,
                    Description = "MUST be used for watched movies, ratings, and genre affinities."
                }
            ]
        };
        agent.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
        var identifier = (await store.AI.CreateAgentAsync<MoviesSampleObject>(agent, MoviesSampleObject.Instance)).Identifier;

        var db = await GetDatabase(store.Database);
        bool throwEx = false;
        db.ForTestingPurposesOnly().BeforeAiAgentTalk = document =>
        {
            if (document.Agent == "user-info-agent" && throwEx)
            {
                // throw on sub-agent
                throw new RefusedToAnswerException("test")
                {
                    RequestId = "test123"
                };
            }

        };

        // chat without error
        var chat = store.AI.Conversation(identifier, "chats/1",
            new AiConversationCreationOptions().AddParameter("userId", "Users/1"));
        chat.SetUserPrompt("Whats my top 5 movies?");
        var r = await chat.RunAsync<MoviesSampleObject>();
        Assert.Equal(AiConversationResult.Done, r.Status);

        // error
        throwEx = true;
        chat.SetUserPrompt("Whats my name and my favorite genres?");
        r = await chat.RunAsync<MoviesSampleObject>();
        Assert.Equal(AiConversationResult.Done, r.Status);
        Assert.NotNull(r.Answer);

        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<Chat>("chats/1");
            var toolCallsAnswers = doc.Messages.Where(m => m.Role == "tool").ToList();
            Assert.True(toolCallsAnswers.Any(a => a.Content is string content && content.Contains("Failed to communicate with the agent 'user-info-agent'")));
        }

        // Resume execution after an error
        throwEx = false;
        chat.SetUserPrompt("Whats my name and my favorite genres?");
        r = await chat.RunAsync<MoviesSampleObject>();
        Assert.Equal(AiConversationResult.Done, r.Status);
    }

}
