using System.IO;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12665 : RavenTestBase
    {
        [Fact]
        public void EntityShouldNotBeMarkedAsChangedWhenItContainsControlCharacters2()
        {
            var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                var str = ((char)1).ToString();

                var ent = new EntAttachment { Name = "ent", Value = str };
                session.Store(ent);

                session.SaveChanges();

                var numberOfRequests = session.Advanced.NumberOfRequests;

                session.SaveChanges();

                Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);
            }
        }

        [Fact]
        public void EntityShouldNotBeMarkedAsChangedWhenItContainsControlCharacters()
        {
            var store = GetDocumentStore();

            using (var session = store.OpenSession())
            using (var stream = new MemoryStream())
            {
                var ent = new EntAttachment { Name = "ent" };
                session.Store(ent);

                var str = ((char)1).ToString();
                session.Advanced.Attachments.Store(ent, str, stream);
                session.SaveChanges();

                var numberOfRequests = session.Advanced.NumberOfRequests;

                session.SaveChanges();

                Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);
            }
        }

        [Fact]
        public void CanHaveControlCharacterInId()
        {
            var store = GetDocumentStore();

            var id = ((char)1).ToString();
            using (var session = store.OpenSession())
            {
                session.Store(new EntAttachment { Name = "ent" });
                session.Store(new EntAttachment { Name = "ent", Id = id });

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var ent = session.Load<EntAttachment>(id);
                Assert.NotNull(ent);
            }
        }

        private class EntAttachment
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Value { get; set; }
        }
    }
}
