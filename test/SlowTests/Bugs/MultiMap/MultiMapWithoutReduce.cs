using FastTests;
using Xunit;
using System.Linq;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Bugs.MultiMap
{
    public class MultiMapWithoutReduce : RavenTestBase
    {
        public MultiMapWithoutReduce(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanQueryFromMultipleSources(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var user = new User
                    {
                        Name = "Ayende Rahien"
                    };
                    session.Store(user);

                    for (int i = 0; i < 5; i++)
                    {
                        session.Store(new Post
                        {
                            AuthorId = user.Id,
                            Title = "blah"
                        });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Count();
                    var posts = session.Query<Post>().Customize(x => x.WaitForNonStaleResults()).Count();

                    Assert.Equal(1, users);
                    Assert.Equal(5, posts);
                }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Post
        {
            public string Id { get; set; }
            public string Title { get; set; }
            public string AuthorId { get; set; }
        }
    }
}
