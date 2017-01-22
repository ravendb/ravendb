using FastTests;
using Raven.Client.Indexing;
using Raven.Json.Linq;
using System.Linq;
using Xunit;

namespace SlowTests.Bugs.Errors
{
    public class CanIndexOnNull : RavenTestBase
    {
        [Fact]
        public void CanIndexOnMissingProps()
        {
            using(var store = GetDocumentStore())
            {
                store.DatabaseCommands.PutIndex("test",
                                                new IndexDefinition
                                                {
                                                    Maps = { "from doc in docs select new { doc.Type, doc.Houses.Wheels} "}
                                                });

                for (int i = 0; i < 50; i++)
                {
                    store.DatabaseCommands.Put("item/" + i, null,
                                               new RavenJObject {{"Type", "Car"}}, new RavenJObject());
                }


                using(var s = store.OpenSession())
                {
                    s.Advanced.DocumentQuery<dynamic>("test")
                        .WaitForNonStaleResults()
                        .WhereGreaterThan("Wheels_Range", 4)
                        .ToArray();
                    
                }

                var db = GetDocumentDatabaseInstanceFor(store).Result;
                var errorsCount = db.IndexStore.GetIndexes().Sum(index => index.GetErrors().Count);

                Assert.Equal(errorsCount , 0);
            }
        }
    }
}
