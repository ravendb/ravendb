using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3166: RavenTest
    {
        public class EventsWithDates
        {
            public Dictionary<DateTime, String> Events { get; set; }
            public DateTime CreationTime { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public void QueryOnDictionaryWithDateTimeAsKeyShouldWork()
        {
            var dt = new DateTime(1982, 11, 28);
            var dates = new List<object> {7, dt, "Shalom", 17.3};
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new EventsWithDates
                    {
                        Name ="Grisha",
                        CreationTime = dt,
                        Events = new Dictionary<DateTime, string>
                        {
                            { dt, "Tal was born" },
                            { new DateTime(1576, 8, 13), "Something happened" }
                        } });
                    session.SaveChanges();
                    WaitForIndexing(store, timeout: TimeSpan.FromSeconds(30));
                    try
                    {
                        
                        var res1 = session.Query<EventsWithDates>()
                            .Where(x => x.Events.Any(y => y.Key.In(dates)))
                            .ToList();

                        var res2 = session.Query<EventsWithDates>()
                            .Where(x => x.CreationTime.In(dates))
                            .ToList();

                        var res3 = session.Query<EventsWithDates>()
                            .Where(x => x.CreationTime == dt)
                            .ToList();

                        var res4 = session.Query<EventsWithDates>()
                            .Where(x => x.CreationTime == dt && x.Name == "Grisha")
                            .ToList();

                        var res5 = session.Query<EventsWithDates>()
                            .Where(x => x.CreationTime == dt && x.Name == "Idan")
                            .Customize(x => x.WaitForNonStaleResults())
                            .ToList();

                        Assert.NotEmpty(res1);
                        Assert.NotEmpty(res2);
                        Assert.NotEmpty(res3);
                        Assert.NotEmpty(res4);
                        Assert.Empty(res5);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                        throw e;
                    }
                };
            }
        }
    }
}
