using System.Collections.Generic;
using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8728 : RavenTestBase
    {
        [Fact]
        public void Can_sum_or_group_by_array_length()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "companies/1",
                        Lines = new List<OrderLine>()
                        {
                            new OrderLine(),
                            new OrderLine(),
                        }
                    });

                    session.Store(new Order
                    {
                        Company = "companies/1",
                        Lines = new List<OrderLine>()
                        {
                            new OrderLine(),
                            new OrderLine(),
                        }
                    });


                    session.SaveChanges();

                    var results = session.Advanced.RawQuery<dynamic>("from Orders group by Company select sum(Lines.Length) as LinesLength").ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(4, (int)results[0].LinesLength);

                    var results2 = session.Advanced.RawQuery<dynamic>("from Orders group by Lines.Length select count(Lines.Length), key() as LinesLength").ToList();

                    Assert.Equal(1, results2.Count);
                    Assert.Equal(2, (int)results2[0].Count);
                    Assert.Equal(2, (int)results2[0].LinesLength);

                    var results3 = session.Query<Order>().GroupBy(x => x.Company).Select(x => new
                    {
                        LinesLength = x.Sum(z => z.Lines.Count)
                    }).ToList();

                    Assert.Equal(1, results3.Count);
                    Assert.Equal(4, results3[0].LinesLength);

                    var results4 = session.Query<Order>().GroupBy(x => x.Lines.Count).Select(x => new
                    {
                        Count = x.Count(),
                        LinesLength = x.Key
                    }).ToList();

                    Assert.Equal(1, results4.Count);
                    Assert.Equal(2, results4[0].Count);
                    Assert.Equal(2, results4[0].LinesLength);
                }
            }
        }
    }
}
