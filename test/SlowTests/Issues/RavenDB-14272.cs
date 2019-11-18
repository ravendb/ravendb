using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14272 : RavenTestBase
    {
        public RavenDB_14272(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Projection_With_A_Single_Field()
        {
            using (var store = GetDocumentStore())
            {
                var userTalk = new UserTalk
                {
                    UserDefs = new Dictionary<string, TalkUserDef>()
                    {
                        {"test1", new TalkUserDef() },
                        {"test2", new TalkUserDef() }
                    }
                };

                using (var session = store.OpenSession())
                {
                    session.Store(userTalk);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<UserTalk>()
                        .Select(x => x.UserDefs)
                        .ToList();

                    Assert.Equal(1, result.Count);
                    Assert.True(userTalk.UserDefs.Keys.SequenceEqual(result[0].Keys));
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.DocumentQuery<UserTalk>()
                        .SelectFields<TalkUserIds>()
                        .ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(2, result[0].UserDefs.Count);
                    Assert.True(userTalk.UserDefs.Keys.SequenceEqual(result[0].UserDefs.Keys));
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<UserTalk>()
                        .ProjectInto<TalkUserIds>()
                        .ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(2, result[0].UserDefs.Count);
                    Assert.True(userTalk.UserDefs.Keys.SequenceEqual(result[0].UserDefs.Keys));
                }

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

        [Fact]
        public async Task Projection_With_A_Single_Field_Async()
        {
            using (var store = GetDocumentStore())
            {
                var userTalk = new UserTalk
                {
                    UserDefs = new Dictionary<string, TalkUserDef>()
                    {
                        {"test1", new TalkUserDef() },
                        {"test2", new TalkUserDef() }
                    }
                };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(userTalk);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var result = await session.Query<UserTalk>()
                        .Select(x => x.UserDefs)
                        .ToListAsync();

                    Assert.Equal(1, result.Count);
                    Assert.True(userTalk.UserDefs.Keys.SequenceEqual(result[0].Keys));
                }

                using (var session = store.OpenAsyncSession())
                {
                    var result = await session.Advanced.AsyncDocumentQuery<UserTalk>()
                        .SelectFields<TalkUserIds>()
                        .ToListAsync();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(2, result[0].UserDefs.Count);
                    Assert.True(userTalk.UserDefs.Keys.SequenceEqual(result[0].UserDefs.Keys));
                }

                using (var session = store.OpenAsyncSession())
                {
                    var result = await session.Query<UserTalk>()
                        .ProjectInto<TalkUserIds>()
                        .ToListAsync();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(2, result[0].UserDefs.Count);
                    Assert.True(userTalk.UserDefs.Keys.SequenceEqual(result[0].UserDefs.Keys));
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<UserTalk>().ProjectInto<TalkUserIds>();
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

        public class UserTalk
        {
            public Dictionary<string, TalkUserDef> UserDefs { get; set; }
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
