using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB845 : RavenTestBase
    {
        public RavenDB845(ITestOutputHelper output) : base(output)
        {
        }

        private class Foo
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public async Task LastModifiedDate_IsUpdated_Local()
        {
            using (var documentStore = GetDocumentStore())
            {
                await DoTest(documentStore);
            }
        }

        private async Task DoTest(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Foo { Id = "foos/1", Name = "A" });
                session.SaveChanges();
            }

            DateTime firstDate;
            using (var session = store.OpenSession())
            {
                var foo = session.Load<Foo>("foos/1");
                var metadata = session.Advanced.GetMetadataFor(foo);
                firstDate = DateTime.Parse(metadata.GetString(Constants.Documents.Metadata.LastModified));
            }

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddDays(1);
            using (var session = store.OpenSession())
            {
                var foo = session.Load<Foo>("foos/1");
                foo.Name = "B";
                session.SaveChanges();
            }

            DateTime secondDate;
            using (var session = store.OpenSession())
            {
                var foo = session.Load<Foo>("foos/1");
                var metadata = session.Advanced.GetMetadataFor(foo);
                secondDate = DateTime.Parse(metadata.GetString(Constants.Documents.Metadata.LastModified));
            }

            Assert.NotEqual(secondDate, firstDate);
        }
    }
}
