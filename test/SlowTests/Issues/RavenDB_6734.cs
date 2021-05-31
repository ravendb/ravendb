using System;
using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_6734 : RavenTestBase
    {
        public RavenDB_6734(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldThrow()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.ThrowIfQueryPageSizeIsNotSet = true;
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidOperationException>(() =>
                    {
                        session.Query<Order>().ToList();
                    });

                    Assert.Contains("Attempt to query without explicitly specifying a page size", e.Message);

                    e = Assert.Throws<InvalidOperationException>(() =>
                    {
                        session.Advanced.DocumentQuery<Order>().ToList();
                    });

                    Assert.Contains("Attempt to query without explicitly specifying a page size", e.Message);

                    var orders = session.Query<Order>()
                        .Take(10)
                        .ToList();

                    Assert.Equal(0, orders.Count);

                    orders = session.Advanced.DocumentQuery<Order>()
                        .Take(10)
                        .ToList();

                    Assert.Equal(0, orders.Count);
                }
            }
        }
    }
}
