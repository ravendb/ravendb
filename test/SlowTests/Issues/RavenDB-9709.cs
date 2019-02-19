using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_9709 : RavenTestBase
    {
        [Fact]
        public void Private_property_setters_should_work()
        {
            const string value = "container-id";
            var data = new Document(value);

            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(data);
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var loaded = s.Load<Document>(data.Id);
                    Assert.Equal(value, loaded.Id);
                    Assert.Equal(value, loaded.Name);
                }
            }
        }

        private class Document
        {
            private Document()
            {
            }

            public Document(string id) : this()
            {
                Id = id;
                Name = id;
            }

            public string Id { get; private set; }
            public string Name { get; private set; }
        }
    }
}
