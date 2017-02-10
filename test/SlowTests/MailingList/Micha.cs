using System.Linq;
using FastTests;
using Raven.Client.Data;
using Raven.Client.Indexes;
using Raven.Client.Operations.Databases;
using Raven.Client.Operations.Databases.Documents;
using Raven.Client.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class Micha : RavenNewTestBase
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

                store.Operations.Send(new PatchByIndexOperation("EntityEntityIdPatch",
                    new IndexQuery(store.Conventions),
                    new PatchRequest
                    {
                        Script = @"
this.EntityTypeId = this.EntityType;
delete this.EntityType
"
                    }));

                var id = store.Admin.Send(new GetIndexOperation("EntityEntityIdPatch")).IndexId;
                store.Admin.Send(new DeleteIndexOperation("EntityEntityIdPatch"));

                Assert.False(store.Admin.Send(new GetStatisticsOperation()).Indexes.Any(x => x.IndexId == id));
            }
        }
    }
}
