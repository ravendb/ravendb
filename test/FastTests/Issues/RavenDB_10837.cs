using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace FastTests.Issues
{

    public class Entity
    {
        public string Id;
        public string Name;
    }

    public class RavenDB_10837 : RavenTestBase
    {
        [Fact]
        public void CanWaitForIndex()
        {

            using (var store = GetDocumentStore())
            {
                IndexDefinition entityByName = new IndexDefinition
                {
                    Name = "EntityByName",
                    Maps = {"from doc in docs \r\nselect new \r\n{ \r\n    " +
                            "Tag = doc[\"@metadata\"][\"Raven-Entity-Name\"], \r\n    " +
                            "LastModified = (DateTime)doc[\"@metadata\"][\"Last-Modified\"],\r\n    " +
                            "LastModifiedTicks = ((DateTime)doc[\"@metadata\"][\"Last-Modified\"]).Ticks \r\n}"}
                };

                store.Maintenance.Send(new PutIndexesOperation(entityByName));

                using (var session = store.OpenSession())
                {
                    session.Store(new Entity
                    {
                        Id = "Entity/1"
                    });
                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var entities = session.Query<Entity>().Statistics(out QueryStatistics statistics).ToList();
                    Assert.NotEmpty(entities);
                }
            }

        }
    }
}
