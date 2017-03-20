using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_6345 : RavenTestBase
    {
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
                    WaitForIndexing(store);
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
