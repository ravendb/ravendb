using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Util;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8728 : RavenTestBase
    {
        public RavenDB_8728(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_sum_or_group_by_list_count()
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

        [Fact]
        public void Can_sum_or_group_by_array_length()
        {
            using (var store = GetDocumentStore())
            {
                var createdAt = SystemTime.UtcNow;

                using (var session = store.OpenSession())
                {
                    session.Store(new Post
                    {
                        CreatedAt = createdAt,
                        Comments = new Post[4]
                    });

                    session.Store(new Post
                    {
                        CreatedAt = createdAt,
                        Comments = new Post[4]
                    });

                    session.Store(new Post
                    {
                        CreatedAt = createdAt,
                        Comments = new Post[4]
                    });

                    session.SaveChanges();

                    var results = session.Query<Post>()
                        .Customize(x=>x.WaitForNonStaleResults())
                        .GroupBy(x => x.CreatedAt).Select(g => new
                    {
                        CommentsCount = g.Sum(x => x.Comments.Length),
                        PostsCount = g.Count()
                    }).ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(3, results[0].PostsCount);
                    Assert.Equal(12, results[0].CommentsCount);

                    var results4 = session.Query<Post>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .GroupBy(x => x.Comments.Length).Select(x => new
                    {
                        Count = x.Count(),
                        CommentsLength = x.Key
                    }).ToList();

                    Assert.Equal(1, results4.Count);
                    Assert.Equal(3, results4[0].Count);
                    Assert.Equal(4, results4[0].CommentsLength);
                }
            }
        }
    }
}
