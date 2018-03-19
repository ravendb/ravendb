using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_9624 : RavenTestBase
    {
        [Fact]
        public void Can_Use_Let_with_From_Alias_thats_a_Reserved_Word()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                PricePerUnit = 10,
                                Quantity = 5
                            },
                            new OrderLine
                            {
                                PricePerUnit = 20,
                                Quantity = 10
                            }
                        }
                    });

                    session.SaveChanges();

                }
                using (var session = store.OpenSession())
                {
                    var query = from order in session.Query<Order>()
                                let sum = order.Lines.Sum(l => l.PricePerUnit * l.Quantity)
                                select new
                                {
                                    Sum = sum
                                };

                    Assert.Equal(
@"declare function output(__ravenDefaultAlias) {
	var order = __ravenDefaultAlias;
	var sum = order.Lines.map(function(l){return l.PricePerUnit*l.Quantity;}).reduce(function(a, b) { return a + b; }, 0);
	return { Sum : sum };
}
from Orders as __ravenDefaultAlias select output(__ravenDefaultAlias)", query.ToString());

                    var result = query.ToList();

                    Assert.Equal(250, result[0].Sum);

                }
            }
        }
    }
}
