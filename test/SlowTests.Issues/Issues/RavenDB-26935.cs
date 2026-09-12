using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26935(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void NonAsciiTermTruncationTest(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            new DocIndex().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Doc { Id = "doc-1", StrVal = new string('a', 3000) });
                session.Store(new Doc { Id = "doc-2", StrVal = "Jiří Krasnec" });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            WaitForUserToContinueTheTest(store);
            using (var session = store.OpenSession())
            {
                var full = session.Query<Doc, DocIndex>()
                    .Search(x => x.StrVal, "krasnec")
                    .ToList();

                var truncated = session.Query<Doc, DocIndex>()
                    .Search(x => x.StrVal, "krasn")
                    .ToList();

                Assert.Empty(truncated);
                Assert.Single(full);
            }
        }
    }

    private class DocIndex : AbstractIndexCreationTask<Doc>
    {
        public DocIndex()
        {
            Map = docs =>
                from doc in docs
                select new Doc
                {
                    Id = doc.Id,
                    StrVal = doc.StrVal,
                };

            Index(x => x.StrVal, FieldIndexing.Search);

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Lucene;
        }
    }

    private class Doc
    {
        public string Id { get; set; }
        public string StrVal { get; set; }
    }
}
