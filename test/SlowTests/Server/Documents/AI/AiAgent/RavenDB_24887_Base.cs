using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Server.Documents.AI.AiAgent
{
    public abstract class RavenDB_24887_Base(ITestOutputHelper output) : RavenTestBase(output)
    {
        internal record Reply
        {
            public string Message { get; set; }
        }

        internal class Movie
        {
            public string Id { get; set; }
            public string Title { get; set; }
            public int Year { get; set; }
            public string[] Genres { get; set; }
            public HashSet<string> Tags { get; set; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        internal class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public HashSet<string> WatchedMovies { get; set; }
        }

        internal class Rating
        {
            public string Id { get; set; }
            public string UserId { get; set; }
            public string MovieId { get; set; }
            public double RatingValue { get; set; }
            public DateTime TimeStamp { get; set; }
        }

        internal class Chat
        {
            public string Id { get; set; }
            public List<Message> Messages { get; set; }
        }

        internal class Message
        {
            [JsonProperty("role")]
            public string Role { get; set; }

            [JsonProperty("content")]
            public object Content { get; set; }
        }


        internal class MoviesSampleObject
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

            public override string ToString()
            {
                return $"Answer: {Answer}, " +
                       $"MoviesIds: [{string.Join(", ", MoviesIds ?? new List<string>())}], " +
                       $"MoviesNames: [{string.Join(", ", MoviesNames ?? new List<string>())}]";
            }
        }

        internal class RateToolSampleRequest
        {
            public static RateToolSampleRequest Instance = new()
            {
                MovieName = "The name of the movie the user wants to rate",
                RateValue = 4.5
            };

            public string MovieName { get; set; }
            public double RateValue { get; set; }
        }

        internal class AddMovieToWatchedListSampleRequest
        {
            public static AddMovieToWatchedListSampleRequest Instance = new()
            {
                MovieName = "The name of the movie the user wants to add to its watched list",
            };

            public string MovieName { get; set; }
        }

        internal class ActionToolResult
        {
            public bool IsSuccessful { get; set; }
            public string Answer { get; set; }
        }

        internal class ChangeUserNameSampleRequest
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


        internal class UserGenreAffinity : AbstractIndexCreationTask<Rating, UserGenreAffinity.Result>
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


        internal static List<Movie> Movies = new()
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
                Id = "Movies/10", Title = "Grumpier Old Men", Year = 1995, Genres = new[] { "Comedy", "Romance" },
                Tags = new HashSet<string>() { "comedinha de velhinhos engraÃƒÂ§ada", "comedinha de velhinhos engraÃ§ada", "grun running", "midwest", "Minnesota" }
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


        internal static List<User> Users = new()
        {
            new User() { Id = "Users/1", Name = "Shahar Hikri", WatchedMovies = new HashSet<string>() { "Movies/1", "Movies/2", "Movies/15" } },

            new User() { Id = "Users/2", Name = "Noa Levi", WatchedMovies = new HashSet<string>() { "Movies/3", "Movies/7", "Movies/12" } },

            new User() { Id = "Users/3", Name = "Itay Cohen", WatchedMovies = new HashSet<string>() { "Movies/5", "Movies/9", "Movies/13", "Movies/20" } },

            new User() { Id = "Users/4", Name = "Maya Ben-David", WatchedMovies = new HashSet<string>() { "Movies/2", "Movies/8", "Movies/14", "Movies/17", "Movies/21" } },

            new User() { Id = "Users/5", Name = "Yonatan Mizrahi", WatchedMovies = new HashSet<string>() { "Movies/4", "Movies/6", "Movies/8" } },
        };

        internal static List<Rating> Rates = new()
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


        internal static async Task CreateMoviesDatabase(DocumentStore store)
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


        internal static async Task<ActionToolResult> ChangeUserNameAsync(IDocumentStore store, ChangeUserNameSampleRequest req)
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

        internal static async Task<object> RateMovieAsync(IDocumentStore store, string userId, RateToolSampleRequest req)
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

        internal static async Task<object> AddMovieAsync(IDocumentStore store, string userId, AddMovieToWatchedListSampleRequest req)
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

        internal static string GetSystemPrompt(string toolName, bool oncePrompt)
        {
            if (oncePrompt)
                return $"""
                    You are a User Profile Agent on a movie rating system.

                    You can edit the user's name and create movie rating records.

                    When the user asks for multiple actions, you MUST perform all of them in a SINGLE tool call.

                    Rules:
                    - Never split the request into multiple tool calls.
                    - Combine all requested actions into the same instruction.
                    - Call the tool exactly once.
                    - Never produce multiple tool calls.
                    - Never repeat the same tool call !!!
                    - After producing the tool call, stop generating.

                    Example:

                    User:
                    "Rate the movie 'Toy Story' as 5 and change my name from 'Shahar Hikri' to 'Aviv Rachmani'."

                    Assistant:
                    Call tool: "{toolName}"
                    "Rate the movie 'Toy Story' as 5 and change my name from 'Shahar Hikri' to 'Aviv Rachmani'."

                    Tool results:
                    Movie rating added successfully.
                    User name updated successfully.
                    
                    Assistant: 
                    "Done! name changed to Aviv Rachmani and 'Toy Story' rated as 5."
                    """;

            return $"""
                You are authorized to edit the user's name and to create movie-rating records associated with the user.

                When the user asks for multiple actions, you must call the sub-agent tool once for each action.

                If multiple actions are requested, emit multiple tool calls in the SAME response.

                Do not wait for a tool result before calling another tool.

                Each tool call must perform exactly one action.

                Never combine multiple actions in a single tool call.

                Example:

                User:
                "Rate the movie 'Toy Story' as 5 and change my name from 'Shahar Hikri' to 'Aviv Rachmani'."

                Assistant:
                [
                  Call tool: "{toolName}"
                  "Rate the movie 'Toy Story' as 5."

                  Call tool: "{toolName}"
                  "Change my name from 'Shahar Hikri' to 'Aviv Rachmani'."
                ]
                
                Tool results:
                Movie rating added successfully.
                User name updated successfully.

                Assistant: 
                "Done! name changed to Aviv Rachmani and 'Toy Story' rated as 5."
                """;
        }

    }
}
