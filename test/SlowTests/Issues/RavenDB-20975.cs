using System.Collections.Generic;
using System.Linq;
using FastTests;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20975 : RavenTestBase
    {
        public RavenDB_20975(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Support_ThenBy_ThenByDescending_In_Projection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new()
                            {
                                Discount = 10,
                                ProductName = "A"
                            },
                            new()
                            {
                                Discount = 10,
                                ProductName = "B"
                            }
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from doc in session.Query<Order>()
                        let line = doc.Lines.OrderByDescending(x => x.Discount).ThenByDescending(x => x.ProductName).FirstOrDefault()
                        select new
                        {
                            Line = line
                        };

                    var result = query.ToList();
                    Assert.Equal(1, result.Count);
                    Assert.Equal(result[0].Line.ProductName, "B");

                    query = from doc in session.Query<Order>()
                        let line = doc.Lines.OrderByDescending(x => x.Discount).ThenBy(x => x.ProductName).FirstOrDefault()
                        select new
                        {
                            Line = line
                        };

                    result = query.ToList();
                    Assert.Equal(1, result.Count);
                    Assert.Equal(result[0].Line.ProductName, "A");
                }
            }
        }
    }
}
