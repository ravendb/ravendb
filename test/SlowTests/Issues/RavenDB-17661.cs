using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Documents.Revisions;
using Raven.Client.Documents.Commands;
using Raven.Server.Documents.Handlers.Admin;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17661 : RavenTestBase
    {
        public RavenDB_17661(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_Get_From_Revisions_Bin_When_Revisions_Are_Disabled()
        {
            using (var store = GetDocumentStore())
            {
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new GetRevisionsBinEntryCommand(long.MaxValue, int.MaxValue);
                    await store.GetRequestExecutor().ExecuteAsync(command, context);
                    Assert.Equal(0, command.Result.Results.Length);
                }
            }
        }

        [Fact]
        public async Task Can_Delete_From_Revisions_Bin_When_Revisions_Are_Disabled()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new RevisionsTests.DeleteRevisionsOperation(new AdminRevisionsHandler.Parameters
                {
                    DocumentIds = new[] { "non-existing-id" }
                }));
            }
        }
    }
}
