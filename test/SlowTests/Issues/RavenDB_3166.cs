using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3166 : RavenTestBase
    {
        public RavenDB_3166(ITestOutputHelper output) : base(output)
        {
        }

        private class EventsWithDates
        {
            public Dictionary<DateTime, string> Events { get; set; }
            public DateTime CreationTime { get; set; }
        }

        private class EventsWithDates2
        {
            public Dictionary<string, DateTime> Events { get; set; }
            public DateTime CreationTime { get; set; }
        }

        [Fact]
        public void QueryOnDictionaryWithDateTimeAsKeyShouldWork()
        {
            var dt = new DateTime(1982, 11, 28);
            var dates = new List<object>() { dt, "Shalom"};
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new EventsWithDates() { CreationTime = dt, Events = new Dictionary<DateTime, string>() { { dt, "Tal was born" }, { new DateTime(1576, 8, 13), "Something happened" } } });
                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);

                    var res = session.Query<EventsWithDates>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Events.Any(y => y.Key.In(dates)))
                        .ToList();
                    var res2 = session.Query<EventsWithDates>()
                                                .Where(x => x.CreationTime.In(dates))
                                                .Customize(x => x.WaitForNonStaleResults())
                                                .ToList();
                    var res3 = session.Query<EventsWithDates>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.CreationTime == dt)
                        .ToList();
                    Assert.NotEmpty(res);
                    Assert.NotEmpty(res2);
                    Assert.NotEmpty(res3);

                    //Assert.Equal(res.Entity, "Tal Weiss");
                }
            }
        }

        [Fact]
        public void QueryOnDictionaryWithDateTimeAsValueShouldWork()
        {
            var dt = new DateTime(1982, 11, 28);
            var dates = new List<object>() {  dt, new DateTime(2015,1,1), new DateTime(1992,1,2)};
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new EventsWithDates2() { CreationTime = dt, Events = new Dictionary<string, DateTime> { { "Tal was born", dt }, { "Something happened", new DateTime(1576, 8, 13) } } });
                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);

                    var res = session.Query<EventsWithDates2>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Events.Any(y => y.Value.In(dates)))
                        .ToList();

                    Assert.NotEmpty(res);
                }
            }
        }
    }
}
