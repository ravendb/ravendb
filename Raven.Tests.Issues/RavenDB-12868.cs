using Raven.Abstractions.Indexing;
using Raven.Tests.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_12868:RavenTest
    {
        public class Product
        {
            public List<string> Names { get; set; }
        }

        public class IdHolder
        {
            public string ProductId { get; set; }
        }

        public class Order
        {

            public List<IdHolder> Products { get; set; }
        }

        [Fact]
        public void ShouldNotConvertUriToStringWhenIndexing2()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Product
                    {
                        Names = new List<string>
                        {
                            "a","b","c"
                        }
                    }, "products/1");
                    session.Store(new Order
                    {
                        Products = new List<IdHolder>
                        {
                            new IdHolder{ ProductId = "products/1" }
                        }
                    }, "orders/1");
                    session.SaveChanges();
                }

                var index = new IndexDefinition();
                index.Map = @"from order in docs.Orders select new {
                    a = 1,
                    _ = this.Recurse(order, x=>x.Products).Select(x=>x.LoadDocument(x.ProductId/4).Names.Select(y=>y/4))
}
            ";
                store.DatabaseCommands.PutIndex("erroneoudIndex", index);

                WaitForIndexing(store);
                Raven.Abstractions.Data.IndexingError[] errors = null;
                errors = store.DatabaseCommands.GetStatistics().Errors;

                SpinWait.SpinUntil(() => {
                    errors = store.DatabaseCommands.GetStatistics().Errors;
                    return errors != null;
                }, 5000);
                Assert.NotEmpty(errors);

                Assert.Equal("orders/1", errors.First().Document);

            }
        }
    }
}
