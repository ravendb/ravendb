using System;
using System.Linq;
using FastTests;
using Tests.Infrastructure;
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void ShouldThrowBetterErrorOnUndefinedJavaScriptDate(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Times1().Execute(store);

                var baseline = new DateTime(2021, 1, 1);
                const string id = "orders/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Order {OrderedAt = baseline}, id);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<Order>(id);

                    Assert.True(doc.OrderedAt.Ticks > MaxJsDate);

                    // here x.Ticks is undefined as it was not stored, thus it should throw
                    var q = session.Advanced.RawQuery<BlittableJsonReaderObject>(
                        @"from index 'Times1' as x 
                            select {
                                DateTime : new Date(x.Ticks)
                            }");

                    var ex = Assert.Throws<RavenException>(() => q.First());
                    Assert.Contains("Invalid 'DateInstance' on property 'DateTime'. Date value : 'NaN'", ex.Message);
                    Assert.Contains("Note that JavaScripts 'Date' measures time as the number of milliseconds that have passed since the Unix epoch", ex.Message);

                }
            }
        }

        private class Times1 : AbstractIndexCreationTask<Order>
        {
            public Times1()
            {
                Map = orders => 
                    from o in orders
                    select new
                    {
                        o.OrderedAt.Ticks
                    };
            }
        }
        
        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void ShouldThrowBetterErrorOnTooBigJavaScriptDate(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Times2().Execute(store);

                var baseline = new DateTime(2021, 1, 1);
                const string id = "orders/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Order {OrderedAt = baseline}, id);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<Order>(id);

                    Assert.True(doc.OrderedAt.Ticks > MaxJsDate);

                    // here x.Ticks is undefined as it is not within JS number-range, thus it should throw
                    var q = session.Advanced.RawQuery<BlittableJsonReaderObject>(
                        @"from index 'Times2' as x 
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

        private class Times2 : AbstractIndexCreationTask<Order, Times2.Result>
        {
            public class Result
            {
                public long Ticks { get; set; }
            }

            public Times2()
            {
                Map = orders => 
                    from o in orders
                    select new Result
                    {
                        Ticks = o.OrderedAt.Ticks
                    };
                
                Store(x => x.Ticks, FieldStorage.Yes);
            }
        }
    }
}
