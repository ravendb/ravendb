using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Bugs.Errors
{
    public class CanIndexOnNull : RavenTestBase
    {
        [Fact]
        public void CanIndexOnMissingProps()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexesOperation(new[] {
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
                        .WhereGreaterThan("Wheels_L_Range", 4)
                        .ToArray();

                }

                var db = GetDocumentDatabaseInstanceFor(store).Result;
                var errorsCount = db.IndexStore.GetIndexes().Sum(index => index.GetErrorCount());

                Assert.Equal(errorsCount, 0);
            }
        }
    }
}
