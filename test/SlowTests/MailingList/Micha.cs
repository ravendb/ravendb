using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
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

                store.Operations.Send(new PatchByIndexOperation(
                    new IndexQuery { Query = "FROM INDEX 'EntityEntityIdPatch'" },
                    new PatchRequest
                    {
                        Script = @"
this.EntityTypeId = this.EntityType;
delete this.EntityType
"
                    }));

                var id = store.Admin.Send(new GetIndexOperation("EntityEntityIdPatch")).Etag;
                store.Admin.Send(new DeleteIndexOperation("EntityEntityIdPatch"));

                Assert.False(store.Admin.Send(new GetStatisticsOperation()).Indexes.Any(x => x.Etag == id));
            }
        }
    }
}
