using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.AiAgent;

public class RavenDB_24887_3(ITestOutputHelper output) : RavenTestBase(output)
{
    public record Reply
    {
        public string Message { get; set; }
    }

    public class Movie
    {
        public string Id { get; set; }
        public string Title { get; set; }
        public int Year { get; set; }
        public string[] Genres { get; set; }
        public HashSet<string> Tags { get; set; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    }

    public class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public HashSet<string> WatchedMovies { get; set; }
    }

    public class Rating
    {
        public string Id { get; set; }
        public string UserId { get; set; }
        public string MovieId { get; set; }
        public double RatingValue { get; set; }
        public DateTime TimeStamp { get; set; }
    }

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
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [true, false])]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
    public async Task SubAgent2OpenActionTools_DepthOf3(Options options, GenAiConfiguration config, bool level1OncePrompt, bool level0OncePrompt)
    {
        const string atOncePrompt =
            "Provide BOTH the movie rating and the new username together in one call. You can't change name without rating a movie and you cant rate a movie without changing the name. you have to ask for both in the call otherwide the tool call will fail.";
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
                    Description = level1OncePrompt ? atOncePrompt : twicePrompt
                }
            ]
        };
        userAgent1.Parameters.Add(new AiAgentParameter("userId", "the id of the current user that you talk with"));
        var userAgent1Id = (await store.AI.CreateAgentAsync<MoviesSampleObject>(userAgent1, MoviesSampleObject.Instance)).Identifier;

        var userAgent0 = new AiAgentConfiguration("user-info-agent-0",
            config.ConnectionStringName,
            "You are a User Profile Agent on movies rating system."
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
                "Use exclusively the 'RateMovie' tool for adding a movie rating. " +
                "Do not attempt to change the user’s name or perform any unrelated action."
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
                "You are a User Profile Agent on movies rating system."
            )
            {
                SubAgents =
                [
                    new AiAgentToolSubAgent
                    {
                        Identifier = rateOrChangeName,
                        Description = "Use for adding movie rate for the current user you talking with OR changing user name (cannot ask for both at once) OR adding a movie to user watched list (can add one movie per request)"
                    },
                    new AiAgentToolSubAgent
                    {
                        Identifier = addMovieToList,
                        Description = "Use for adding a movie to user watched list (can add one movie per request). "
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

    private class Chat
    {
        public string Id { get; set; }
        public List<Message> Messages { get; set; }
    }

    private class Message
    {
        [JsonProperty("role")]
        public string Role { get; set; }

        [JsonProperty("content")]
        public object Content { get; set; }
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
            "You are a User Profile Agent on movies rating system."
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
            "You are a User Profile Agent on movies rating system."
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


    private static async Task<ActionToolResult> ChangeUserNameAsync(IDocumentStore store, ChangeUserNameSampleRequest req)
    {
        using (var session = store.OpenAsyncSession())
        {
            var user = await session.LoadAsync<User>(req.UserId);
            if (user.Name.ToLower() != req.OldUserName.ToLower())
            {
                return new ActionToolResult
                {
                    IsSuccessful = false,
                    Answer = $"Your old name isn't '{req.OldUserName}'"
                };
            }

            user.Name = req.NewUserName;
            await session.SaveChangesAsync();

            return new ActionToolResult
            {
                IsSuccessful = true,
                Answer = $"Name of user '{user.Id}' changed from '{req.OldUserName}' to '{req.NewUserName}'"
            };
        }
    }

    private static async Task<object> RateMovieAsync(IDocumentStore store, string userId, RateToolSampleRequest req)
    {
        if (req.RateValue < 0 || req.RateValue > 5)
        {
            return new ActionToolResult
            {
                IsSuccessful = false,
                Answer = $"Cant rate \"{req.MovieName}\" with the rate value {req.RateValue} - rate value has to be between 0 to 5"
            };
        }

        using (var session = store.OpenAsyncSession())
        {
            var movies = await session
                .Advanced
                .AsyncRawQuery<Movie>("from Movies where Title = $name")
                .AddParameter("name", req.MovieName.ToLower())
                .ToListAsync();

            if (movies == null || movies.Count == 0)
            {
                return new ActionToolResult
                {
                    IsSuccessful = false,
                    Answer = $"Movie with the name \"{req.MovieName}\" doesn't exist on the database"
                };
            }

            var user = await session.LoadAsync<User>(userId);

            foreach (var m in movies)
            {
                user.WatchedMovies.Add(m.Id);
                await session.StoreAsync(new Rating()
                {
                    Id = "Ratings/",
                    MovieId = m.Id,
                    UserId = userId,
                    RatingValue = req.RateValue,
                    TimeStamp = DateTime.Now
                });
            }

            await session.SaveChangesAsync();

            return new ActionToolResult
            {
                IsSuccessful = true,
                Answer =
                    $"Found {movies.Count} movies with the name '{req.MovieName}' and rated them by score '{req.RateValue}'"
            };
        }
    }

    private static async Task<object> AddMovieAsync(IDocumentStore store, string userId, AddMovieToWatchedListSampleRequest req)
    {
        using (var session = store.OpenAsyncSession())
        {
            var movies = await session
                .Advanced
                .AsyncRawQuery<Movie>("from Movies where Title = $name")
                .AddParameter("name", req.MovieName.ToLower())
                .ToListAsync();

            if (movies == null || movies.Count == 0)
            {
                return new ActionToolResult
                {
                    IsSuccessful = false,
                    Answer = $"Movie with the name \"{req.MovieName}\" doesn't exist on the database"
                };
            }

            var user = await session.LoadAsync<User>(userId);
            var movieId = string.Empty;
            foreach (var m in movies)
            {
                movieId = m.Id;
                user.WatchedMovies.Add(m.Id);
                break;
            }

            await session.SaveChangesAsync();

            return new ActionToolResult
            {
                IsSuccessful = true,
                Answer =
                    $"The movie '{req.MovieName}'  (id: {movieId}) was added to user watched list"
            };
        }
    }

    private static async Task CreateMoviesDatabase(DocumentStore store)
    {
        using (var session = store.OpenAsyncSession())
        {
            foreach (var m in Movies)
            {
                await session.StoreAsync(m);
            }

            foreach (var u in Users)
            {
                await session.StoreAsync(u);
            }

            foreach (var r in Rates)
            {
                await session.StoreAsync(r);
            }

            await session.SaveChangesAsync();
        }

        await new UserGenreAffinity().ExecuteAsync(store);

        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Query<UserGenreAffinity.Result, UserGenreAffinity>()
                .Customize(x => x.WaitForNonStaleResults()) // ✅ ensures index is caught up
                .ToListAsync();
        }
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

    public class RateToolSampleRequest
    {
        public static RateToolSampleRequest Instance = new()
        {
            MovieName = "The name of the movie the user wants to rate",
            RateValue = 4.5
        };

        public string MovieName { get; set; }
        public double RateValue { get; set; }
    }

    public class AddMovieToWatchedListSampleRequest
    {
        public static AddMovieToWatchedListSampleRequest Instance = new()
        {
            MovieName = "The name of the movie the user wants to add to its watched list",
        };

        public string MovieName { get; set; }
    }

    public class ActionToolResult
    {
        public bool IsSuccessful { get; set; }
        public string Answer { get; set; }
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

    private static List<Movie> Movies = new()
    {
        new Movie() { Id = "Movies/1", Title = "Tom and Huck", Year = 1995, Genres = new[] { "Adventure", "Children" }, Tags = new HashSet<string>() { "Library System", "based on a book", "Mark Twain", "19th century", "abandoned house" } },

        new Movie() { Id = "Movies/2", Title = "Toy Story", Year = 1995, Genres = new[] { "Adventure", "Animation", "Children", "Comedy", "Fantasy" }, Tags = new HashSet<string>() { "children", "Disney", "animation", "pixar", "funny" } },

        new Movie() { Id = "Movies/3", Title = "Casino", Year = 1995, Genres = new[] { "Crime", "Drama" }, Tags = new HashSet<string>() { "Martin Scorsese", "robert de niro", "Tumey's DVDs", "Documentary like", "imdb top 250" } },

        new Movie() { Id = "Movies/4", Title = "Heat", Year = 1995, Genres = new[] { "Action", "Crime", "Thriller" }, Tags = new HashSet<string>() { "atmospheric", "dialogue", "Tumey's DVDs", "realistic action", "Al Pacino" } },

        new Movie() { Id = "Movies/5", Title = "Four Rooms", Year = 1995, Genres = new[] { "Comedy" }, Tags = new HashSet<string>() { "multiple storylines", "Tim Roth", "Tumey's VHS", "4", "Antonio Banderas" } },

        new Movie() { Id = "Movies/6", Title = "American President, The", Year = 1995, Genres = new[] { "Comedy", "Drama", "Romance" }, Tags = new HashSet<string>() { "clever dialogue", "Aaron Sorkin", "Romance", "seen more than once", "politics" } },

        new Movie() { Id = "Movies/7", Title = "GoldenEye", Year = 1995, Genres = new[] { "Action", "Adventure", "Thriller" }, Tags = new HashSet<string>() { "james bond", "Tumey's DVDs", "007", "Bond", "casual sex" } },

        new Movie() { Id = "Movies/8", Title = "Jumanji", Year = 1995, Genres = new[] { "Adventure", "Children", "Fantasy" }, Tags = new HashSet<string>() { "Robin Williams", "fantasy", "time travel", "animals", "childhood recaptured" } },

        new Movie() { Id = "Movies/9", Title = "Nixon", Year = 1995, Genres = new[] { "Drama" }, Tags = new HashSet<string>() { "Tumey's VHS", "based on a true story", "own", "politics", "president" } },

        new Movie()
        {
            Id = "Movies/10", Title = "Grumpier Old Men", Year = 1995, Genres = new[] { "Comedy", "Romance" }, Tags = new HashSet<string>() { "comedinha de velhinhos engraÃƒÂ§ada", "comedinha de velhinhos engraÃ§ada", "grun running", "midwest", "Minnesota" }
        },

        new Movie() { Id = "Movies/11", Title = "Sense and Sensibility", Year = 1995, Genres = new[] { "Drama", "Romance" }, Tags = new HashSet<string>() { "boring", "nothing happens", "18th century", "classic", "emotional" } },

        new Movie() { Id = "Movies/12", Title = "Sudden Death", Year = 1995, Genres = new[] { "Action" }, Tags = new HashSet<string>() { "Action", "Jean-Claude Van Damme", "Can't remember", "Peter Hyams", "1990s" } },

        new Movie() { Id = "Movies/13", Title = "Balto", Year = 1995, Genres = new[] { "Adventure", "Animation", "Children" }, Tags = new HashSet<string>() { "James Horner", "wolves", "dogsled", "sort of boring", "dogs" } },

        new Movie() { Id = "Movies/14", Title = "Ace Ventura: When Nature Calls", Year = 1995, Genres = new[] { "Comedy" }, Tags = new HashSet<string>() { "Jim Carrey", "detective", "silly fun", "very funny", "Rhino action :D" } },

        new Movie() { Id = "Movies/15", Title = "Dracula: Dead and Loving It", Year = 1995, Genres = new[] { "Comedy", "Horror" }, Tags = new HashSet<string>() { "vampire", "vampires", "Leslie Nielsen", "spoof", "funny" } },

        new Movie() { Id = "Movies/16", Title = "Get Shorty", Year = 1995, Genres = new[] { "Comedy", "Crime", "Thriller" }, Tags = new HashSet<string>() { "contrived", "forgetable", "funny", "silly", "funny!" } },

        new Movie() { Id = "Movies/17", Title = "Cutthroat Island", Year = 1995, Genres = new[] { "Action", "Adventure", "Romance" }, Tags = new HashSet<string>() { "adventure", "dumb but funny", "Matthew Modine", "pirates", "Renny Harlin" } },

        new Movie() { Id = "Movies/18", Title = "Sabrina", Year = 1995, Genres = new[] { "Comedy", "Romance" }, Tags = new HashSet<string>() { "as good maybe better than original", "great cast", "long island", "love story", "remake" } },

        new Movie() { Id = "Movies/19", Title = "Money Train", Year = 1995, Genres = new[] { "Action", "Comedy", "Crime", "Drama", "Thriller" }, Tags = new HashSet<string>() { "afternoon section", "lame", "worthwhile", "action hero", "anti hero" } },

        new Movie() { Id = "Movies/20", Title = "Father of the Bride Part II", Year = 1995, Genres = new[] { "Comedy" }, Tags = new HashSet<string>() { "Fantasy", "pregnancy", "remake", "family", "Steve Martin" } },

        new Movie() { Id = "Movies/21", Title = "Waiting to Exhale", Year = 1995, Genres = new[] { "Comedy", "Drama", "Romance" }, Tags = new HashSet<string>() { "characters", "slurs", "based on novel or book", "chick flick", "divorce" } },
    };

    private static List<User> Users = new()
    {
        new User() { Id = "Users/1", Name = "Shahar Hikri", WatchedMovies = new HashSet<string>() { "Movies/1", "Movies/2", "Movies/15" } },

        new User() { Id = "Users/2", Name = "Noa Levi", WatchedMovies = new HashSet<string>() { "Movies/3", "Movies/7", "Movies/12" } },

        new User() { Id = "Users/3", Name = "Itay Cohen", WatchedMovies = new HashSet<string>() { "Movies/5", "Movies/9", "Movies/13", "Movies/20" } },

        new User() { Id = "Users/4", Name = "Maya Ben-David", WatchedMovies = new HashSet<string>() { "Movies/2", "Movies/8", "Movies/14", "Movies/17", "Movies/21" } },

        new User() { Id = "Users/5", Name = "Yonatan Mizrahi", WatchedMovies = new HashSet<string>() { "Movies/4", "Movies/6", "Movies/8" } },
    };

    private static List<Rating> Rates = new()
    {
        // User 1 (Shahar Hikri) - 3 ratings
        new Rating() { Id = "Ratings/1", UserId = "Users/1", MovieId = "Movies/1", RatingValue = 4.8, TimeStamp = new DateTime(2025, 5, 1, 10, 0, 0) },
        new Rating() { Id = "Ratings/2", UserId = "Users/1", MovieId = "Movies/2", RatingValue = 3.6, TimeStamp = new DateTime(2025, 5, 2, 14, 30, 0) },
        new Rating() { Id = "Ratings/3", UserId = "Users/1", MovieId = "Movies/15", RatingValue = 2.9, TimeStamp = new DateTime(2025, 5, 3, 18, 45, 0) },

        // User 2 (Noa Levi) - 3 ratings
        new Rating() { Id = "Ratings/4", UserId = "Users/2", MovieId = "Movies/3", RatingValue = 4.2, TimeStamp = new DateTime(2025, 5, 4, 9, 15, 0) },
        new Rating() { Id = "Ratings/5", UserId = "Users/2", MovieId = "Movies/7", RatingValue = 3.1, TimeStamp = new DateTime(2025, 5, 5, 20, 0, 0) },
        new Rating() { Id = "Ratings/6", UserId = "Users/2", MovieId = "Movies/12", RatingValue = 2.4, TimeStamp = new DateTime(2025, 5, 6, 11, 10, 0) },

        // User 3 (Itay Cohen) - 4 ratings
        new Rating() { Id = "Ratings/7", UserId = "Users/3", MovieId = "Movies/5", RatingValue = 4.5, TimeStamp = new DateTime(2025, 5, 7, 16, 50, 0) },
        new Rating() { Id = "Ratings/8", UserId = "Users/3", MovieId = "Movies/9", RatingValue = 3.3, TimeStamp = new DateTime(2025, 5, 8, 12, 25, 0) },
        new Rating() { Id = "Ratings/9", UserId = "Users/3", MovieId = "Movies/13", RatingValue = 1.9, TimeStamp = new DateTime(2025, 5, 9, 19, 40, 0) },
        new Rating() { Id = "Ratings/10", UserId = "Users/3", MovieId = "Movies/20", RatingValue = 4.7, TimeStamp = new DateTime(2025, 5, 10, 15, 5, 0) },

        // User 4 (Maya Ben-David) - 5 ratings
        new Rating() { Id = "Ratings/11", UserId = "Users/4", MovieId = "Movies/2", RatingValue = 3.8, TimeStamp = new DateTime(2025, 5, 11, 10, 0, 0) },
        new Rating() { Id = "Ratings/12", UserId = "Users/4", MovieId = "Movies/8", RatingValue = 4.1, TimeStamp = new DateTime(2025, 5, 12, 13, 45, 0) },
        new Rating() { Id = "Ratings/13", UserId = "Users/4", MovieId = "Movies/14", RatingValue = 2.7, TimeStamp = new DateTime(2025, 5, 13, 17, 20, 0) },
        new Rating() { Id = "Ratings/14", UserId = "Users/4", MovieId = "Movies/17", RatingValue = 4.9, TimeStamp = new DateTime(2025, 5, 14, 21, 30, 0) },
        new Rating() { Id = "Ratings/15", UserId = "Users/4", MovieId = "Movies/21", RatingValue = 3.0, TimeStamp = new DateTime(2025, 5, 15, 8, 55, 0) },

        // User 5 (Yonatan Mizrahi) - 2 ratings
        new Rating() { Id = "Ratings/16", UserId = "Users/5", MovieId = "Movies/4", RatingValue = 4.4, TimeStamp = new DateTime(2025, 5, 16, 14, 10, 0) },
        new Rating() { Id = "Ratings/17", UserId = "Users/5", MovieId = "Movies/6", RatingValue = 2.6, TimeStamp = new DateTime(2025, 5, 17, 18, 20, 0) },
        new Rating() { Id = "Ratings/18", UserId = "Users/5", MovieId = "Movies/8", RatingValue = 4.3, TimeStamp = new DateTime(2025, 5, 12, 13, 45, 0) },
    };

    private class UserGenreAffinity : AbstractIndexCreationTask<Rating, UserGenreAffinity.Result>
    {
        public class Result
        {
            public string UserId { get; set; }
            public string Genre { get; set; }
            public int Count { get; set; }
            public double Sum { get; set; }
            public double Score { get; set; }
        }

        public UserGenreAffinity()
        {
            Map = ratings => from r in ratings
                             let m = LoadDocument<Movie>(r.MovieId)
                             from gnr in (m.Genres ?? Array.Empty<string>())
                             select new
                             {
                                 r.UserId,
                                 Genre = gnr,
                                 Count = 1,
                                 Sum = (double)r.RatingValue,
                                 Score = (double)r.RatingValue
                             };

            Reduce = results => from r in results
                                group r by new { r.UserId, r.Genre }
                into g
                                let totalCount = g.Sum(x => x.Count)
                                let totalSum = g.Sum(x => x.Sum)
                                select new
                                {
                                    UserId = g.Key.UserId,
                                    Genre = g.Key.Genre,
                                    Count = totalCount,
                                    Sum = totalSum,
                                    Score = totalSum / totalCount
                                };

            Index(x => x.UserId, FieldIndexing.Exact);
            Index(x => x.Genre, FieldIndexing.Search);
            StoreAllFields(FieldStorage.Yes);
        }
    }
}
