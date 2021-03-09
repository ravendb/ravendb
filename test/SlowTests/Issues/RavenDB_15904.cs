using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15904 : RavenTestBase
    {
        private static readonly double MaxJsDate = (DateTime.MaxValue - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;

        public RavenDB_15904(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldThrowBetterErrorOnInvalidJavaScriptDate()
        {
            using (var store = GetDocumentStore())
            {
                new Times().Execute(store);

                var baseline = new DateTime(2021, 1, 1);
                const string id = "orders/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Order {OrderedAt = baseline}, id);

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<Order>(id);

                    Assert.True(doc.OrderedAt.Ticks > MaxJsDate);

                    var q = session.Advanced.RawQuery<BlittableJsonReaderObject>(
                        @"from index 'Times' as x 
                            select {
                                DateTime : new Date(x.Ticks)
                            }");

                    var ex = Assert.Throws<RavenException>(() => q.First());
                    Assert.Contains("Invalid 'DateInstance' on property 'DateTime'. Date value : 'NaN'", ex.Message);
                    Assert.Contains("Note that JavaScripts 'Date' measures time as the number of milliseconds that have passed since the Unix epoch", ex.Message);

                }

                using (var session = store.OpenSession())
                {
                    var tooBig = MaxJsDate + 10;

                    var q = session.Advanced.RawQuery<BlittableJsonReaderObject>(
                        @"from Orders as x 
                            select {
                                DateTime : new Date($num)
                            }").AddParameter("num", tooBig);

                    var ex = Assert.Throws<RavenException>(() => q.First());
                    Assert.Contains($"Invalid 'DateInstance' on property 'DateTime'. Date value : '{tooBig}'", ex.Message);
                }
            }
        }

        private class Times : AbstractIndexCreationTask<Order>
        {
            public Times()
            {
                Map = orders => 
                    from o in orders
                    select new
                    {
                        o.OrderedAt.Ticks
                    };
            }
        }
    }
}
