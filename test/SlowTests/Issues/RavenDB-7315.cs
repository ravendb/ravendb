using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
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
        private readonly string _indexName;

        public override string IndexName => _indexName ?? base.IndexName;

        public Index(string name):this()
        {
            _indexName = name;
        }

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
        public QueriesStartingWithAtShouldWork(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ShouldAtBeEscaped(Options options)
        {
            var store = GetDocumentStore(options);

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
                var requestedLangs = new[] {"en", "pt"};
                var query = session.Query<MyEntity, Index>()
                    .Customize(q => q.WaitForNonStaleResults())
                    .Where(e => e.AuthorId == "users/42" && e.Title.StartsWith("@") && e.Language.In(requestedLangs));

                Assert.Contains("@andrew is a fine guy",
                    query.ToList().Select(e => e.Title).ToList());
            }
        }
    }
}
