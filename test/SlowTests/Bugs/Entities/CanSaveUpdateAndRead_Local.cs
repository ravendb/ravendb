using FastTests;
using Raven.NewClient.Abstractions.Data;
using Xunit;

namespace SlowTests.Bugs.Entities
{
    public class CanSaveUpdateAndRead_Local : RavenNewTestBase
    {
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
                    s.Load<Event>("events/1").Happy = false;
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var e = s.Load<Event>("events/1");
                    var entityName = s.Advanced.GetMetadataFor(e)[Constants.Headers.RavenEntityName];
                    Assert.Equal("Events", entityName);
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
