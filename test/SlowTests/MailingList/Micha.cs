using System.Linq;
using FastTests;
using Raven.Client.Data;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class Micha : RavenTestBase
    {
        private class Entity
        {
            public string Label { get; set; }
        }

        private class EntityEntityIdPatch : AbstractIndexCreationTask<Entity>
        {
            public EntityEntityIdPatch()
            {
                Map = docs => from doc in docs
                              select new { doc.Label };
            }
        }

        [Fact]
        public void CanDeleteIndex()
        {
            using (var store = GetDocumentStore())
            {
                new EntityEntityIdPatch().Execute(store);

                WaitForIndexing(store);

                store.DatabaseCommands.UpdateByIndex("EntityEntityIdPatch",
                    new IndexQuery(),
                    new PatchRequest
                    {
                        Script = @"
this.EntityTypeId = this.EntityType;
delete this.EntityType
"
                    });

                var id = store.DatabaseCommands.GetIndex("EntityEntityIdPatch").IndexId;
                store.DatabaseCommands.DeleteIndex("EntityEntityIdPatch");

                Assert.False(store.DatabaseCommands.GetStatistics().Indexes.Any(x => x.IndexId == id));
            }
        }
    }
}
