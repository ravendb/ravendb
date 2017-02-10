using FastTests;
using Xunit;
using System.Linq;
using Raven.Client.Indexing;
using Raven.Client.Operations.Databases.Indexes;

namespace SlowTests.Bugs.Indexing
{
    public class WithStartWith : RavenNewTestBase
    {
        [Fact]
        public void CanQueryDocumentsFilteredByMap()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexOperation("test",
                    new IndexDefinition
                    {
                        Maps = { "from doc in docs let Name = doc[\"@metadata\"][\"Name\"] where Name.StartsWith(\"Raven\") select new { Name }" }
                    }));

                using (var s = store.OpenSession())
                {
                    var entity = new { Name = "Ayende" };
                    s.Store(entity);
                    s.Advanced.GetMetadataFor(entity)["Name"] = "RavenDB";
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    Assert.Equal(1, s.Query<object>("test").Customize(x => x.WaitForNonStaleResults()).Count());
                }
            }
        }

    }
}
