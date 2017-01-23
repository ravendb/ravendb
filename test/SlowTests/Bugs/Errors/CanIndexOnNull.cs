using System.Linq;
using FastTests;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Bugs.Errors
{
    public class CanIndexOnNull : RavenNewTestBase
    {
        [Fact]
        public void CanIndexOnMissingProps()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexOperation("test",
                                                new IndexDefinition
                                                {
                                                    Maps = { "from doc in docs select new { doc.Type, doc.Houses.Wheels} " }
                                                }));

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
                        .WhereGreaterThan("Wheels_Range", 4)
                        .ToArray();

                }

                var db = GetDocumentDatabaseInstanceFor(store).Result;
                var errorsCount = db.IndexStore.GetIndexes().Sum(index => index.GetErrors().Count);

                Assert.Equal(errorsCount, 0);
            }
        }
    }
}
