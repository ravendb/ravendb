using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21760 : RavenTestBase
{
    public RavenDB_21760(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void TestIndexDefinitionWithReservedKeywords()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var eventSchedule1 = new EventSchedule()
                {
                    Id = "events/1/schedule"
                };
                
                var event1 = new Event()
                {
                    Id = "events/1", 
                    Name = "Cool event name",
                    @event = "Some event"
                };
                
                session.Store(eventSchedule1);
                session.Store(event1);
                
                session.SaveChanges();

                var index = new EventIndex();
                
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);

                var result = session.Query<EventIndex.Result, EventIndex>().ProjectInto<EventIndex.Result>().ToList();
                
                Assert.Equal(1, result.Count);
                Assert.Equal("Cool event name", result.First().EventName);
                Assert.Equal("Some event", result.First().@event);
            }
        }
    }
    
    public class EventIndex : AbstractIndexCreationTask<Event, EventIndex.Result>
    {
        public class Result
        {
            public string EventName { get; set; }
            public string @event { get; set; }
        }

        public EventIndex()
        {
            Map = events => from @event in events
                let schedule = LoadDocument<EventSchedule>($"{@event.Id}/schedule")
                select new Result
                {
                    EventName = @event.Name,
                    @event = @event.@event
                };
            
            StoreAllFields(FieldStorage.Yes);
        }
    }
    
    public class EventSchedule
    {
        public string Id { get; set; }
    }

    public class Event
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string @event { get; set; }
    }
}

