using FastTests;
using Xunit;

namespace SlowTests.Bugs.Entities
{
    public class CanSaveUpdateAndRead_Local : RavenTestBase
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
                    var entityName = s.Advanced.GetMetadataFor(e)["Raven-Entity-Name"].Value<string>();
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
