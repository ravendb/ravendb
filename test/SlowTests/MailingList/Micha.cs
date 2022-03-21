using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Micha : RavenTestBase
    {
        public Micha(ITestOutputHelper output) : base(output)
        {
        }

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

                Indexes.WaitForIndexing(store);

                store.Operations.Send(new PatchByQueryOperation(
                    new IndexQuery { Query = @"FROM INDEX 'EntityEntityIdPatch' UPDATE { 
this.EntityTypeId = this.EntityType;
delete this.EntityType
}" }));

                var Name = store.Maintenance.Send(new GetIndexOperation("EntityEntityIdPatch")).Name;
                store.Maintenance.Send(new DeleteIndexOperation("EntityEntityIdPatch"));

                Assert.False(store.Maintenance.Send(new GetStatisticsOperation()).Indexes.Any(x => x.Name == Name));
            }
        }
    }
}
