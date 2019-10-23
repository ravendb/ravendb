using FastTests;
using Raven.Client;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Entities
{
    public class CanSaveUpdateAndRead_Local : RavenTestBase
    {
        public CanSaveUpdateAndRead_Local(ITestOutputHelper output) : base(output)
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

        private class Event
        {
            public string Id { get; set; }
            public bool Happy { get; set; }
        }
    }
}
