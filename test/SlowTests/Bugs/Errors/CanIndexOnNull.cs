using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Errors
{
    public class CanIndexOnNull : RavenTestBase
    {
        public CanIndexOnNull(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanIndexOnMissingProps()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {
                                                new IndexDefinition
                                                {
                                                    Maps = { "from doc in docs select new { doc.Type, doc.Houses.Wheels} " },
                                                    Name = "test"
                                                }}));

                using (var commands = store.Commands())
                {
                    for (var i = 0; i < 50; i++)
                    {
                        commands.Put("item/" + i, null, new { Type = "Car" }, null);
                    }
                }

                using (var s = store.OpenSession())
                {
                    s.Advanced.DocumentQuery<dynamic>("test")
                        .WaitForNonStaleResults()
                        .WhereGreaterThan("Wheels", 4)
                        .ToArray();

                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                var errorsCount = db.IndexStore.GetIndexes().Sum(index => index.GetErrorCount());

                Assert.Equal(errorsCount, 0);
            }
        }
    }
}
