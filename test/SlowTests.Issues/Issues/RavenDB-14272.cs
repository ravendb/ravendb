using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14272 : RavenTestBase
    {
        public RavenDB_14272(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Projection_With_A_Single_Primitive_Field(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var userTalk = SaveUserTalk(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<UserTalk>()
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(userTalk.Name, result[0]);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task Projection_With_A_Single_Primitive_Field_Async(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var userTalk = SaveUserTalk(store);

                using (var session = store.OpenAsyncSession())
                {
                    var result = await session.Query<UserTalk>()
                        .Select(x => x.Name)
                        .ToListAsync();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(userTalk.Name, result[0]);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Projection_With_A_Single_Field(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var userTalk = SaveUserTalk(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<UserTalk>()
                        .Select(x => x.UserDefs)
                        .ToList();

                    Assert.Equal(1, result.Count);
                    Assert.True(userTalk.UserDefs.Keys.SequenceEqual(result[0].Keys));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task Projection_With_A_Single_Field_Async(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var userTalk = SaveUserTalk(store);

                using (var session = store.OpenAsyncSession())
                {
                    var result = await session.Query<UserTalk>()
                        .Select(x => x.UserDefs)
                        .ToListAsync();

                    Assert.Equal(1, result.Count);
                    Assert.True(userTalk.UserDefs.Keys.SequenceEqual(result[0].Keys));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Select_Fields1(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var userTalk = SaveUserTalk(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.DocumentQuery<UserTalk>()
                        .SelectFields<TalkUserIds>()
                        .ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(2, result[0].UserDefs.Count);
                    Assert.True(userTalk.UserDefs.Keys.SequenceEqual(result[0].UserDefs.Keys));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task Select_Fields1_Async(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var userTalk = SaveUserTalk(store);

                using (var session = store.OpenAsyncSession())
                {
                    var result = await session.Advanced.AsyncDocumentQuery<UserTalk>()
                        .SelectFields<TalkUserIds>()
                        .ToListAsync();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(2, result[0].UserDefs.Count);
                    Assert.True(userTalk.UserDefs.Keys.SequenceEqual(result[0].UserDefs.Keys));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Select_Fields2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var userTalk = SaveUserTalk(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.DocumentQuery<UserTalk>()
                        .SelectFields<TalkUserIds>(nameof(UserTalk.UserDefs))
                        .ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(2, result[0].UserDefs.Count);
                    Assert.True(userTalk.UserDefs.Keys.SequenceEqual(result[0].UserDefs.Keys));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task Select_Fields2_Async(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var userTalk = SaveUserTalk(store);

                using (var session = store.OpenAsyncSession())
                {
                    var result = await session.Advanced.AsyncDocumentQuery<UserTalk>()
                        .SelectFields<TalkUserIds>(nameof(UserTalk.UserDefs))
                        .ToListAsync();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(2, result[0].UserDefs.Count);
                    Assert.True(userTalk.UserDefs.Keys.SequenceEqual(result[0].UserDefs.Keys));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Select_Fields3(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var userTalk = SaveUserTalk(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.DocumentQuery<UserTalk>()
                        .SelectFields<TalkUserIds>(new QueryData(new[] { nameof(TalkUserIds.UserDefs) }, new[] { nameof(TalkUserIds.UserDefs) }))
                        .ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(2, result[0].UserDefs.Count);
                    Assert.True(userTalk.UserDefs.Keys.SequenceEqual(result[0].UserDefs.Keys));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task Select_Fields3_Async(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var userTalk = SaveUserTalk(store);

                using (var session = store.OpenAsyncSession())
                {
                    var result = await session.Advanced.AsyncDocumentQuery<UserTalk>()
                        .SelectFields<TalkUserIds>(new QueryData(new[] { nameof(TalkUserIds.UserDefs) }, new[] { nameof(TalkUserIds.UserDefs) }))
                        .ToListAsync();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(2, result[0].UserDefs.Count);
                    Assert.True(userTalk.UserDefs.Keys.SequenceEqual(result[0].UserDefs.Keys));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Select_Fields4(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var userTalk = SaveUserTalk(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.DocumentQuery<UserTalk>()
                        .SelectFields<string>("Name")
                        .ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(userTalk.Name, result[0]);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task Select_Fields4_Async(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var userTalk = SaveUserTalk(store);

                using (var session = store.OpenAsyncSession())
                {
                    var result = await session.Advanced.AsyncDocumentQuery<UserTalk>()
                        .SelectFields<string>("Name")
                        .ToListAsync();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(userTalk.Name, result[0]);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Project_Into(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var userTalk = SaveUserTalk(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<UserTalk>()
                        .ProjectInto<TalkUserIds>()
                        .ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(2, result[0].UserDefs.Count);
                    Assert.True(userTalk.UserDefs.Keys.SequenceEqual(result[0].UserDefs.Keys));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task Project_Into_Async(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var userTalk = SaveUserTalk(store);

                using (var session = store.OpenAsyncSession())
                {
                    var result = await session.Query<UserTalk>()
                        .ProjectInto<TalkUserIds>()
                        .ToListAsync();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(2, result[0].UserDefs.Count);
                    Assert.True(userTalk.UserDefs.Keys.SequenceEqual(result[0].UserDefs.Keys));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Streaming_Query_Projection(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var userTalk = SaveUserTalk(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<UserTalk>().ProjectInto<TalkUserIds>();
                    var stream = session.Advanced.Stream(query);

                    while (stream.MoveNext())
                    {
                        var projection = stream.Current.Document;

                        Assert.NotNull(projection);
                        Assert.NotNull(projection.UserDefs);
                        Assert.Equal(2, projection.UserDefs.Count);
                        Assert.True(userTalk.UserDefs.Keys.SequenceEqual(projection.UserDefs.Keys));
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task Streaming_Query_Projection_Async(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var userTalk = SaveUserTalk(store);

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<UserTalk>().ProjectInto<TalkUserIds>();
                    await using var stream = await session.Advanced.StreamAsync(query);

                    while (await stream.MoveNextAsync())
                    {
                        var projection = stream.Current.Document;

                        Assert.NotNull(projection);
                        Assert.NotNull(projection.UserDefs);
                        Assert.Equal(2, projection.UserDefs.Count);
                        Assert.True(userTalk.UserDefs.Keys.SequenceEqual(projection.UserDefs.Keys));
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Streaming_Document_Query_Projection(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var userTalk = SaveUserTalk(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<UserTalk>()
                        .SelectFields<TalkUserIds>(nameof(UserTalk.UserDefs));
                    var stream = session.Advanced.Stream(query);

                    while (stream.MoveNext())
                    {
                        var projection = stream.Current.Document;

                        Assert.NotNull(projection);
                        Assert.NotNull(projection.UserDefs);
                        Assert.Equal(2, projection.UserDefs.Count);
                        Assert.True(userTalk.UserDefs.Keys.SequenceEqual(projection.UserDefs.Keys));
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task Streaming_Document_Query_Projection_Async(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var userTalk = SaveUserTalk(store);

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Advanced.AsyncDocumentQuery<UserTalk>()
                        .SelectFields<TalkUserIds>(nameof(UserTalk.UserDefs));
                    var stream = await session.Advanced.StreamAsync(query);

                    while (await stream.MoveNextAsync())
                    {
                        var projection = stream.Current.Document;

                        Assert.NotNull(projection);
                        Assert.NotNull(projection.UserDefs);
                        Assert.Equal(2, projection.UserDefs.Count);
                        Assert.True(userTalk.UserDefs.Keys.SequenceEqual(projection.UserDefs.Keys));
                    }
                }
            }
        }

        private UserTalk SaveUserTalk(DocumentStore store)
        {
            var userTalk = new UserTalk
            {
                UserDefs = new Dictionary<string, TalkUserDef>()
                {
                    {"test1", new TalkUserDef() },
                    {"test2", new TalkUserDef() }
                },
                Name = "Grisha"
            };

            using (var session = store.OpenSession())
            {
                session.Store(userTalk);
                session.SaveChanges();
            }

            return userTalk;
        }

        public class UserTalk
        {
            public Dictionary<string, TalkUserDef> UserDefs { get; set; }

            public string Name { get; set; }
        }

        public class TalkUserIds
        {
            public Dictionary<string, TalkUserDef> UserDefs { get; set; }
        }

        public class TalkUserDef
        {

        }
    }
}
