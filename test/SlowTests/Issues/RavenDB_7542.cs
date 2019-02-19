using System.Collections.Generic;
using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_7542 : RavenTestBase
    {
        [Fact]
        public void Wait_for_non_stale_results_as_of_now_on_index_working_on_all_docs()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition()
                {
                    Name = "Testing_Sort",
                    Maps =
                    {
                        "from doc in docs select new { name = doc.name, uid = doc.uid}"
                    },
                    Fields = new Dictionary<string, IndexFieldOptions>()
                    {
                        {"uid", new IndexFieldOptions()
                        {
                        }}
                    }
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new Product("Products/101", "test101", 2, "a"));
                    session.Store(new Product("Products/10", "test10", 3, "b"));
                    session.Store(new Product("Products/106", "test106", 4, "c"));
                    session.Store(new Product("Products/107", "test107", 5));
                    session.Store(new Product("Products/103", "test107", 6));
                    session.Store(new Product("Products/108", "new_testing", 90, "d"));
                    session.Store(new Order());
                    session.Store(new Company());
                    
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    var objects = session.Advanced.DocumentQuery<Product>("Testing_Sort").WhereIn(x => x.uid, new int[] {4, 6, 90})
                        .WaitForNonStaleResults().ToList();
                    
                    Assert.Equal(3, objects.Count);
                }
            }
        }

        private class Product
        {
            public Product(string id, string name, int uid, string order = "")
            {
                Id = id;
                this.uid = uid;
                this.name = name;
                this.order = order;
            }
            
            public string Id { get; set; }
            public int uid { get; set; }
            public string name { get; set; }
            public string order { get; set; }
        }
    }
}
