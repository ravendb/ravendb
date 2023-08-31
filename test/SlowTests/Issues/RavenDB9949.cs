using Raven.Client.Documents.Linq;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB9949 : RavenTestBase
    {
        public RavenDB9949(ITestOutputHelper output) : base(output)
        {
        }

        public enum EventType
        {
            Printed,
            Checked
        }

        public class Event
        {
            public int Quantity;
            public bool? Handled;
            public EventType Type;
            public EventType Sort;
        }
        public class Purchase
        {
            public IEnumerable<Event> Events;
            public int? Quantity;
            public int QuantityInvoiced;
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanQueryNullables(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var result = new
                    {
                        Quantity = 5
                    };

                    var query = (from purchase in session.Query<Purchase>()
                                 let isPrinted = purchase.Events.Where(a => a.Type == EventType.Printed && a.Handled == true)
                                 let isChecked = purchase.Events.Where(a => a.Sort == EventType.Checked).Sum(a => a.Quantity) >= result.Quantity
                                 select new
                                 {
                                     IsPrinted = isPrinted
                                 }).ToList();

                }
            }
        }
    }
}
