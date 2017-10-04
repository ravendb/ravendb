using System.Linq;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class MyEntity
    {
        public string Id { get; set; }
        public string AuthorId { get; set; }
        public string Title { get; set; }
        public string Language { get; set; }
    }
    class Index : AbstractIndexCreationTask<MyEntity>
    {
        public Index()
        {
            Map = entities => from e in entities
                select new
                {
                    e.AuthorId,
                    e.Title,
                    e.Language,
                };
        }
    }

    public class QueriesStartingWithAtShouldWork : RavenTestBase
    {
        [Fact]
        public void ShouldAtBeEscaped()
        {
            var store = NewDocumentStore();

            new Index().Execute(store);
            using (var session = store.OpenSession())
            {
                session.Store(new MyEntity
                {
                    AuthorId = "users/42",
                    Title = "@andrew is a fine guy",
                    Language = "en"
                });
                session.SaveChanges();
            }
            using (var session = store.OpenSession())
            {
                var requestedLangs = new[] { "en", "pt" };
                var query = session.Query<MyEntity, Index>()
                    .Customize(q => q.WaitForNonStaleResults())
                    .Where(e => e.AuthorId == "users/42" && e.Title.StartsWith("@") && e.Language.In(requestedLangs));

                Assert.Contains("@andrew is a fine guy",
                    query.ToList().Select(e => e.Title).ToList());
            }
        }
    }
}
