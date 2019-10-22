using System.Linq;
using FastTests;
using Raven.Client;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Entities
{
    public class CanSaveUpdateAndRead : RavenTestBase
    {
        public CanSaveUpdateAndRead(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_read_entity_name_after_update()
        {
            using(var store = GetDocumentStore())
            {
                using(var s =store.OpenSession())
                {
                    s.Store(new Event {Happy = true});
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Load<Event>("events/1-A").Happy = false;
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var e = s.Load<Event>("events/1-A");
                    var entityName = s.Advanced.GetMetadataFor(e)[Constants.Documents.Metadata.Collection];
                    Assert.Equal("Events", entityName);
                }
            }
        }

        [Fact]
        public void Can_read_entity_name_after_update_from_query()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Event { Happy = true });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Load<Event>("events/1-A").Happy = false;
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var events = s.Query<Event>().Customize(x=>x.WaitForNonStaleResults()).ToArray();
                    Assert.NotEmpty(events);
                }
            }
        }

        [Fact]
        public void Can_read_entity_name_after_update_from_query_after_entity_is_in_cache()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Event { Happy = true });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    s.Load<Event>("events/1-A");//load into cache
                }

                using (var s = store.OpenSession())
                {
                    s.Load<Event>("events/1-A").Happy = false;
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var events = s.Query<Event>().Customize(x => x.WaitForNonStaleResults()).ToArray();
                    Assert.NotEmpty(events);
                }
            }
        }

        private class Event
        {
            public string Id { get; set; }
            public bool Happy { get; set; }
        }
    }
}
