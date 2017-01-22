using FastTests;
using Raven.Client.Indexing;
using Xunit;
using System.Linq;

namespace SlowTests.Bugs.Indexing
{
    public class WithStartWith : RavenTestBase
    {
        [Fact]
        public void CanQueryDocumentsFilteredByMap()
        {
            using(var store = GetDocumentStore())
            {
                store.DatabaseCommands.PutIndex("test",
                                                new IndexDefinition
                                                {
                                                    Maps = { "from doc in docs let Name = doc[\"@metadata\"][\"Name\"] where Name.StartsWith(\"Raven\") select new { Name }" }                                                      
                                                });

                using(var s = store.OpenSession())
                {
                    var entity = new {Name = "Ayende"};
                    s.Store(entity);
                    s.Advanced.GetMetadataFor(entity)["Name"] = "RavenDB";
                    s.SaveChanges();
                }

                using(var s = store.OpenSession())
                {
                    Assert.Equal(1, s.Query<object>("test").Customize(x => x.WaitForNonStaleResults()).Count());
                }
            }
        }

    }
}
