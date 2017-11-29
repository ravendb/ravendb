using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_7919 : RavenLowLevelTestBase
    {
        [Fact]
        public async Task Should_use_auto_index_even_if_idle_when_match_is_complete()
        {
            using (var database = CreateDocumentDatabase())
            {
                var indexId = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[]
                {
                    new AutoIndexField
                    {
                        Name = "FirstName",
                    },
                    new AutoIndexField
                    {
                        Name = "LastName",
                    }
                }));

                var autoIndex = database.IndexStore.GetIndex(indexId);

                autoIndex.SetState(IndexState.Idle);

                using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                {
                    var results = await database.QueryRunner.ExecuteQuery(new IndexQueryServerSide("from Users where LastName = 'Arek'"), context, null,
                        OperationCancelToken.None);
                }
            }
        }
    }
}
