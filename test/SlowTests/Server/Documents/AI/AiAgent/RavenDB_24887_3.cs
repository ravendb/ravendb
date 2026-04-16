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

public class RavenDB_24887_3(ITestOutputHelper output) : RavenDB_24887_Base(output)
{
    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ActionToolsOnSubAgent_DepthOf3(Options options, GenAiConfiguration config)
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
        userAgent1.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
        var userAgent1Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent1, MoviesSampleObject.Instance)).Identifier;

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
        var userAgent0Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent0, MoviesSampleObject.Instance)).Identifier;

        var chat = store.AI.Conversation(userAgent0Id, "chats/1",
            new AiConversationCreationOptions().AddParameter("userId", "Users/1"));
        chat.Handle<ChangeUserNameSampleRequest>("user-info-agent-1/user-info-agent-3/ChangeUserName", (r) => ChangeUserNameAsync(store, r));

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
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [false, false])]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    public async Task SubAgent2OpenActionTools_DepthOf3(Options options, GenAiConfiguration config, bool level1OncePrompt, bool level0OncePrompt)
    {
        const string atOncePrompt =
            "Provide BOTH the movie rating and the new username together in one call. You can't change name without rating a movie and you cant rate a movie without changing the name. you have to ask for both in the call otherwise the tool call will fail.";
        const string twicePrompt = "Use for adding movie rate for the current user you talking with OR changing user name (cannot ask for both at once)";

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

        var userAgent1 = new AiAgentConfiguration("user-info-agent-1", config.ConnectionStringName, GetSystemPrompt(userAgent2Id, level1OncePrompt)
        )
        {
            SubAgents =
            [
                new AiAgentToolSubAgent
                {
                    Identifier = userAgent2Id,
                    Description = level1OncePrompt ? atOncePrompt : twicePrompt
                }
            ]
        };
        userAgent1.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
        var userAgent1Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent1, MoviesSampleObject.Instance)).Identifier;

        var userAgent0 = new AiAgentConfiguration("user-info-agent-0", config.ConnectionStringName, GetSystemPrompt(userAgent1Id, level0OncePrompt)
        )
        {
            SubAgents =
            [
                new AiAgentToolSubAgent
                {
                    Identifier = userAgent1Id,
                    Description = level0OncePrompt ? atOncePrompt : twicePrompt

                }
            ]
        };
        userAgent0.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
        var userAgent0Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent0, MoviesSampleObject.Instance)).Identifier;

        var chat = store.AI.Conversation(userAgent0Id, "chats/",
            new AiConversationCreationOptions().AddParameter("userId", "Users/1"));
        chat.Handle<ChangeUserNameSampleRequest, ActionToolResult>("user-info-agent-1/user-info-agent-2/ChangeUserName", async (r) =>
        {
            var res = (await ChangeUserNameAsync(store, r)) as ActionToolResult;
            // Console.WriteLine(res.Answer);
            return res;
        });
        chat.Handle<RateToolSampleRequest, ActionToolResult>("user-info-agent-1/user-info-agent-2/RateMovie", async (r) =>
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
    public async Task SubAgent2OpenActionTools_Depth3and4(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
        await CreateMoviesDatabase(store);

        // Level3
        var addMovieToListAgentId = await GetAddMovieToListAgent();

        // Level2
        var changeUserNameAgentId = await GetChangeUserNameAgent();
        var rateMovieAgentId = await GetRateMovieAgent();
        var userAgent2bId = await GetUserAgent2b(addMovieToListAgentId);

        // Level1
        var userAgent1aId = await GetUserAgent1a(changeUserNameAgentId, rateMovieAgentId);
        var userAgent1bId = await GetUserAgent1b(addMovieToList: userAgent2bId);

        // Level0
        var userAgent0Id = await GetUserAgent0(rateOrChangeName: userAgent1aId, addMovieToList: userAgent1bId);

        /*
        UserAgent0
            ├── UserAgent1a
            │   ├── changeUserNameAgent
            │   └── rateMovieAgent
            └── UserAgent1b
                └── userAgent2b
                    └── addMovieToListAgent
        */

        var addMovieToListActionName = $"{userAgent1bId}/{userAgent2bId}/{addMovieToListAgentId}/AddToMovieWatchedList";
        var changeUserNameActionName = $"{userAgent1aId}/{changeUserNameAgentId}/ChangeUserName";
        var rateMovieActionName = $"{userAgent1aId}/{rateMovieAgentId}/RateMovie";

        var chat = store.AI.Conversation(userAgent0Id, "chats/",
            new AiConversationCreationOptions().AddParameter("userId", "Users/1"));
        chat.Handle<ChangeUserNameSampleRequest, ActionToolResult>(changeUserNameActionName, async (r) =>
        {
            var res = (await ChangeUserNameAsync(store, r)) as ActionToolResult;
            // Console.WriteLine(res.Answer);
            return res;
        });
        chat.Handle<RateToolSampleRequest, ActionToolResult>(rateMovieActionName, async (r) =>
        {
            var res = await RateMovieAsync(store, "Users/1", r) as ActionToolResult;
            // Console.WriteLine(res.Answer);
            return res;
        });
        chat.Handle<AddMovieToWatchedListSampleRequest, ActionToolResult>(addMovieToListActionName, async (r) =>
        {
            var res = await AddMovieAsync(store, "Users/1", r) as ActionToolResult;
            return res;
        });

        chat.SetUserPrompt("Please rate the movie \"Toy Story\" as 5, add the movie Nixon and Sudden Death to my watched list,and also change my name from 'Shahar Hikri' to 'Aviv Rachmani'");
        var r = await chat.RunAsync<MoviesSampleObject>();
        Assert.Equal(AiConversationResult.Done, r.Status);

        using (var session = store.OpenAsyncSession())
        {
            var u = await session.LoadAsync<User>("Users/1");
            Assert.Equal("Aviv Rachmani", u.Name);
            Assert.Equal(5, u.WatchedMovies.Count);
        }

        Assert.Equal(Rates.Count + 1, (await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation())).Collections["Ratings"]);

        async Task<string> GetAddMovieToListAgent()
        {
            var addMovieToListAgent = new AiAgentConfiguration("add-movie-to-list-agent",
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
            addMovieToListAgent.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            return (await store.AI.CreateAgentAsync<MoviesSampleObject>(addMovieToListAgent, MoviesSampleObject.Instance)).Identifier;
        }

        async Task<string> GetChangeUserNameAgent()
        {
            var changeUserNameAgent = new AiAgentConfiguration(
                "change-name-agent",
                config.ConnectionStringName,
                "You are authorized ONLY to change the user's name. " +
                "Use exclusively the 'ChangeUserName' tool for changing the user's name. " +
                "Do not attempt to perform movie rating or any other action."
            )
            {
                Actions = new List<AiAgentToolAction>()
                {
                    new AiAgentToolAction(
                        "ChangeUserName",
                        "Updates the name of the current user interacting with the AI agent. Must include the old name for validation.")
                    {
                        ParametersSampleObject = JsonConvert.SerializeObject(ChangeUserNameSampleRequest.Instance)
                    }
                }
            };
            changeUserNameAgent.Parameters.Add(new AiAgentParameter("userId", "the id of the requested user"));
            return (await store.AI.CreateAgentAsync<MoviesSampleObject>(changeUserNameAgent, MoviesSampleObject.Instance)).Identifier;
        }

        async Task<string> GetRateMovieAgent()
        {
            var rateMovieAgent = new AiAgentConfiguration(
                "rate-movie-agent",
                config.ConnectionStringName,
                "You are authorized ONLY to create movie-rating records for the current user. " +
                "You MUST call the 'RateMovie' tool to perform this action. " +
                "Do NOT return a textual answer instead of calling the tool. " +
                "If a rating is requested, ALWAYS call the tool. " +
                "Never simulate or describe the action. " +
                "After calling the tool, stop generating."
            )
            {
                Actions = new List<AiAgentToolAction>()
                {
                    new AiAgentToolAction(
                        "RateMovie",
                        "Adds a movie rating. Requires movie name and rating value between 0 and 5 (double allowed).")
                    {
                        ParametersSampleObject = JsonConvert.SerializeObject(RateToolSampleRequest.Instance)
                    }
                }
            };
            rateMovieAgent.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            return (await store.AI.CreateAgentAsync<MoviesSampleObject>(rateMovieAgent, MoviesSampleObject.Instance)).Identifier;
        }

        async Task<string> GetUserAgent2b(string addMovieToListAgentIdentifier)
        {
            var userAgent2b = new AiAgentConfiguration("user-info-agent-2b",
                config.ConnectionStringName,
                "You are authorized to add movies to user watched List." +
                "Use exclusively the 'AddToMoviesWatchedList' tool for adding a movie to user's watched list. " +
                "Do not perform these action in any other way."
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent
                    {
                        Identifier = addMovieToListAgentIdentifier,
                        Description = "Use for adding a movie to user watched list."
                    }
                ]
            };
            userAgent2b.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            return (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent2b, MoviesSampleObject.Instance)).Identifier;
        }

        async Task<string> GetUserAgent1a(string changeUserName, string rateMovie)
        {
            var userAgent1a = new AiAgentConfiguration("user-info-agent-1a",
                config.ConnectionStringName,
                "You are a User Profile Agent on movies rating system."
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent
                    {
                        Identifier = changeUserName,
                        Description = "Use for changing user name"
                    },
                    new AiAgentToolSubAgent
                    {
                        Identifier = rateMovie,
                        Description = "Use for adding movie rate for changing user name"
                    }
                ]
            };
            userAgent1a.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            return (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent1a, MoviesSampleObject.Instance)).Identifier;
        }

        async Task<string> GetUserAgent1b(string addMovieToList)
        {
            var userAgent1b = new AiAgentConfiguration("user-info-agent-1b",
                config.ConnectionStringName,
                "You are a User Profile Agent on movies rating system."
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent
                    {
                        Identifier = addMovieToList,
                        Description = "Use for adding a movie to user watched list (can add one movie per request). "
                    }
                ]
            };
            userAgent1b.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            return (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent1b, MoviesSampleObject.Instance)).Identifier;
        }

        async Task<string> GetUserAgent0(string rateOrChangeName, string addMovieToList)
        {
            var userAgent0 = new AiAgentConfiguration("user-info-agent-0",
                config.ConnectionStringName,
                "You are a User Profile Agent on a movies rating system. " +
                "If the user requests multiple actions, you must execute all of them using the available tools. " +
                "Do not ask for clarification if all required data is provided. " +
                "Always prefer calling tools over returning a textual answer when actions are required."
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent
                    {
                        Identifier = rateOrChangeName,
                        Description = "Use for performing user-related actions such as rating movies or changing user name. " +
                                      "You may perform multiple actions in sequence if needed. " +
                                      "Execute them step-by-step using the appropriate tools and do not ask the user to choose between actions if all required information is provided."
                    },
                    new AiAgentToolSubAgent
                    {
                        Identifier = addMovieToList,
                        Description = "Use for adding a movie to user watched list (can add one movie per request). " +
                                      "You may call this tool multiple times if multiple movies need to be added."
                    }
                ]
            };
            userAgent0.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
            return (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent0, MoviesSampleObject.Instance)).Identifier;
        }

    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ResumeChatAfterError_Depth3(Options options, GenAiConfiguration config)
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
        userAgent.Parameters.Add(new AiAgentParameter("userId", "the id of the requested user"));

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

        var agent1 = new AiAgentConfiguration("movies-agent-1",
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
        agent1.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
        var identifier1 = (await store.AI.CreateAgentAsync<MoviesSampleObject>(agent1, MoviesSampleObject.Instance)).Identifier;

        var agent0 = new AiAgentConfiguration("movies-agent-0",
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
                    Identifier = identifier1,
                    Description = "Use to ask: " + Environment.NewLine +
                                  "- about user profile, such as: user name and watched list.. " + Environment.NewLine +
                                  "- The list of movies the user has rated, ordered by rating. " + Environment.NewLine +
                                  "- The user’s genre affinities, sorted by preference scores. "
                }
            ]
        };
        agent0.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
        var identifier0 = (await store.AI.CreateAgentAsync<MoviesSampleObject>(agent0, MoviesSampleObject.Instance)).Identifier;

        var db = await GetDatabase(store.Database);
        bool throwEx = false;
        db.ForTestingPurposesOnly().BeforeAiAgentTalk = document =>
        {
            if (document.Agent == userAgentId && throwEx)
            {
                // throw on sub-agent
                throw new RefusedToAnswerException("test")
                {
                    RequestId = "test123"
                };
            }

        };

        var chat = store.AI.Conversation(identifier0, "chats/1",
            new AiConversationCreationOptions().AddParameter("userId", "Users/1"));

        // error
        throwEx = true;
        chat.SetUserPrompt("Hey, Im the user, Whats my name and my favorite genres?");
        var r = await chat.RunAsync<MoviesSampleObject>();

        // check if the sub-agent "user-info-agent" error is consumed
        Assert.Equal(AiConversationResult.Done, r.Status);
        Assert.NotNull(r.Answer);

        using (var session = store.OpenAsyncSession())
        {
            var doc1 = (await session.Advanced.LoadStartingWithAsync<Chat>("chats/1/movies-agent-1/")).Single(d => d.Id.Contains("/recommendation-agent") == false);
            var toolCallsAnswers1 = doc1.Messages.Where(m => m.Role == "tool").ToList();
            Assert.True(toolCallsAnswers1.Any(a => a.Content is string content && content.Contains($"Failed to communicate with the agent '{userAgentId}'")));

            var doc0 = await session.LoadAsync<Chat>("chats/1");
            var toolCallsAnswers0 = doc0.Messages.Where(m => m.Role == "tool").ToList();
            Assert.False(toolCallsAnswers0.Any(a => a.Content is string content && content.Contains($"Failed to communicate with the agent '{userAgentId}'")));
        }

        // Resume execution after an error
        throwEx = false;
        chat.SetUserPrompt("Whats my name and my favorite genres?");
        r = await chat.RunAsync<MoviesSampleObject>();
        Assert.Equal(AiConversationResult.Done, r.Status);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CheckMaxToolIterationsLimitOnSubAgent_Depth3(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));
        await CreateMoviesDatabase(store);

        var userAgent3 = new AiAgentConfiguration("user-info-agent-3",
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
        userAgent3.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
        var userAgent3Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent3, MoviesSampleObject.Instance)).Identifier;

        var userAgent2 = new AiAgentConfiguration("user-info-agent-2",
            config.ConnectionStringName,
            GetSystemPrompt(userAgent3Id, oncePrompt: false)
        )
        {
            SubAgents =
            [
                new AiAgentToolSubAgent
                {
                    Identifier = userAgent3Id,
                    Description = "Use for adding movie rate for the current user you talking with OR changing user name (cannot ask for both at once)"
                }
            ],
            MaxModelIterationsPerCall = 2
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
            MaxModelIterationsPerCall = 2
        };
        userAgent1.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
        var userAgent1Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent1, MoviesSampleObject.Instance)).Identifier;

        var chat = store.AI.Conversation(userAgent1Id, "chats/",
            new AiConversationCreationOptions().AddParameter("userId", "Users/1"));
        chat.Handle<ChangeUserNameSampleRequest, ActionToolResult>("user-info-agent-2/user-info-agent-3/ChangeUserName", async (r) =>
        {
            var res = (await ChangeUserNameAsync(store, r)) as ActionToolResult;
            // Console.WriteLine(res.Answer);
            return res;
        });
        chat.Handle<RateToolSampleRequest, ActionToolResult>("user-info-agent-2/user-info-agent-3/RateMovie", async (r) =>
        {
            var res = await RateMovieAsync(store, "Users/1", r) as ActionToolResult;
            // Console.WriteLine(res.Answer);
            return res;
        });

        chat.SetUserPrompt("Can you rate the movie \"Toy Story\" as 5 and change my name from 'Shahar Hikri' to 'Aviv Rachmani'?");
        var r = await chat.RunAsync<MoviesSampleObject>();
        Assert.Equal(AiConversationResult.Done, r.Status);

        // check 3 conversation (1 sub-conversations, 1 sub-sub-conversations)
        Assert.Equal(3, (await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation())).Collections["@conversations"]);

        // check that nothing has been changed
        using (var session = store.OpenAsyncSession())
        {
            var u = await session.LoadAsync<User>("Users/1");
            Assert.Equal("Shahar Hikri", u.Name);
        }
        Assert.Equal(Rates.Count, (await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation())).Collections["Ratings"]);
    }

}
