using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class InfinityStringTest : RavenTestBase
    {
        public InfinityStringTest(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TestInfinityString()
        {
            using (var store = GetDocumentStore())
            {
                new DocIndex().Execute(store);

                using (IDocumentSession session = store.OpenSession())
                {
                    session.Store(new Doc
                    {
                        Id = "Docs/1",
                        StrVal = "Infinity",
                    });
                    session.Store(new Doc
                    {
                        Id = "Docs/2",
                        StrVal = "-Infinity",
                    });
                    session.Store(new Doc
                    {
                        Id = "Docs/3",
                        StrVal = "NaN",
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (IDocumentSession session = store.OpenSession())
                {
                    var results = session
                        .Query<Doc, DocIndex>()
                        .Where(x=>x.StrVal == "-Infinity" || x.StrVal == "Infinity" || x.StrVal == "NaN")
                        .ToArray();

                    Assert.Equal(3, results.Length);
                }
            }
        }
        public class DocIndex : AbstractIndexCreationTask<Doc>
        {
            public DocIndex()
            {
                Map = docs => from doc in docs
                    select new
                    {
                        doc.Id,
                        StrVal = doc.StrVal.Replace("_", ""),
                        //StrVal = doc.StrVal.ToString().Replace("_", ""), // calling .ToString() works around the issue
                    };
            }
        }

        public class Doc
        {
            public string Id { get; set; }
            public string StrVal { get; set; }
        }
    }


}
