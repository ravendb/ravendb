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
        }

        [Fact]
        public void QueryOnDictionaryWithDateTimeAsKeyShouldWork()
        {
            var dt = new DateTime(1982, 11, 28);
            var dates = new List<object>() {7,dt,"Shalom",17.3};
            using (var store = NewRemoteDocumentStore(runInMemory:false,fiddler:true,databaseName:"mashu"))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new EventsWithDates() { CreationTime = dt, Events = new Dictionary<DateTime, string>() { { dt, "Tal was born" }, { new DateTime(1576, 8, 13), "Something happened" } } });
                    session.SaveChanges();
                    WaitForIndexing(store);
                    try
                    {
                        var res = session.Query<EventsWithDates>()
                            .Where(x => x.Events.Any(y => y.Key.In(dates)))
                            .ToList();
                        var res2 = session.Query<EventsWithDates>()
                                                    .Where(x => x.CreationTime.In(dates))
                                                    .Customize(x => x.WaitForNonStaleResults())
                                                    .ToList();
                        var res3 = session.Query<EventsWithDates>()
                            .Where(x => x.CreationTime == dt)
                            .Customize(x => x.WaitForNonStaleResults())
                            .ToList();
                        Assert.NotEmpty(res);
                        Assert.NotEmpty(res2);
                        Assert.NotEmpty(res3);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                        throw e;
                    }
                    //Assert.Equal(res.Entity, "Tal Weiss");
                };
            }
        }
    }
}
