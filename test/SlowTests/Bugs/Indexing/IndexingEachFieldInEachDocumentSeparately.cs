using FastTests;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Indexing
{
    public class IndexingEachFieldInEachDocumentSeparately : RavenTestBase
    {
        public IndexingEachFieldInEachDocumentSeparately(ITestOutputHelper output) : base(output)
        {
        }

        [Fact(Skip = "Missing feature : RavenDB-6152 ")]
        public void ForIndexing()
        {
            using (var store = GetDocumentStore())
            {
                //store.Configuration.Catalog.Catalogs.Add(new TypeCatalog(typeof(MyAnalyzerGenerator)));
                using (var s = store.OpenSession())
                {
                    s.Store(new { Name = "Ayende Rahien" });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var objects = s.Advanced.DocumentQuery<object>()
                        .WhereEquals("Name", "Ayende")
                        .ToArray();

                    Assert.NotEmpty(objects);
                }
            }
        }

        //public class MyAnalyzerGenerator : AbstractAnalyzerGenerator
        //{
        //    public override Analyzer GenerateAnalyzerForIndexing(string indexName, Lucene.Net.Documents.Document document, Analyzer previousAnalyzer)
        //    {
        //        return new StandardAnalyzer(Version.LUCENE_29);
        //    }

        //    public override Analyzer GenerateAnalyzerForQuerying(string indexName, string query, Analyzer previousAnalyzer)
        //    {
        //        return new StandardAnalyzer(Version.LUCENE_29);
        //    }
        //}
    }
}
