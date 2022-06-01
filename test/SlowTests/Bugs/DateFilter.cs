using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class DateFilter : RavenTestBase
    {
        public DateFilter(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void WhenDefiningIndexWithSystemType_IndexShouldGetDefined(Options options)
        {
            using (var store = GetDocumentStore(options))
                new Orders_BySentDate().Execute(store);
        }

        private class Orders_BySentDate : AbstractIndexCreationTask<Order>
        {
            public Orders_BySentDate()
            {
                Map = orders => from o in orders
                                where o.SentDate >= new DateTime(2011, 5, 23, 0, 0, 0, DateTimeKind.Utc)
                                select new { o.Id };
            }
        }

        private class Order
        {
            public string Id { get; set; }
            public DateTime SentDate { get; set; }
        }
    }
}
