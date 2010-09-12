using System.Threading;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
    public class StalenessWontAffectUnrelatedIndexes : LocalClientTest
    {
        [Fact]
        public void AddingUnrealtedDocumentWontChagneIndexStaleness()
        {
            using(var store = NewDocumentStore())
            {
                store.DatabaseCommands.PutIndex("Test", new IndexDefinition
                {
                    Map = "from doc in docs.Cops select new { doc.Badge }"
                });

                while(store.DatabaseCommands.Query("Test", new IndexQuery(), null).IsStale)
                    Thread.Sleep(100);

                store.DocumentDatabase.StopBackgroundWokers();

                using(var session = store.OpenSession())
                {
                    session.Store(new AutoDetectAnaylzersForQuery.Foo
                    {
                        Name = "blah"
                    });
                    session.SaveChanges();
                }


                Assert.False(store.DatabaseCommands.Query("Test", new IndexQuery(), null).IsStale);

            }
        }
    }
}