using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_6345 : RavenTestBase
    {
        public RavenDB_6345(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void NotOperatorShouldBeInvokedOnTheProperAstNode()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new SomeClassIndex());
                using (var session = store.OpenSession())
                {
                    session.Store(new SomeClass { Culture = "EU", CatalogId = "Catalog/Test", ModelId = 4 });
                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);
                    var query = session.Query<SomeClass>("SomeClassIndex").Where(x => x.Culture.Equals("EU") && !(x.ModelId == 5) || x.CatalogId == "Catalog/Test");
                    Assert.Single(query.ToList());
                }
            }
        }

        public class SomeClassIndex : AbstractIndexCreationTask<SomeClass>
        {
            public SomeClassIndex()
            {
                Map = docs => from doc in docs select new { doc.Culture, doc.ModelId, doc.CatalogId };
            }
        }
        public class SomeClass
        {
            public string Culture { get; set; }
            public int ModelId { get; set; }
            public string CatalogId { get; set; }
        }
    }
}
