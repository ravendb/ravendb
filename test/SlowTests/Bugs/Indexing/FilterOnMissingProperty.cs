using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Indexing
{
    public class FilterOnMissingProperty : RavenTestBase
    {
        public FilterOnMissingProperty(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanFilter()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {
                    new IndexDefinition
                    {
                        Maps = { "from doc in docs where doc.Valid select new { doc.Name }" },
                        Name = "test"
                    }}));

                using (var session = store.OpenSession())
                {
                    session.Store(new { Valid = true, Name = "Oren" });

                    session.Store(new { Name = "Ayende " });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.DocumentQuery<dynamic>("test").WaitForNonStaleResults().ToArray();
                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                var errorsCount = db.IndexStore.GetIndexes().Sum(index => index.GetErrorCount());

                Assert.Equal(errorsCount, 0);
            }
        }
    }
}
