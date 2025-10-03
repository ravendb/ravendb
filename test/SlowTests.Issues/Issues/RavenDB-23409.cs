using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.TransactionsRecording;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_23409 : RavenTestBase
    {
        public RavenDB_23409(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Counters | RavenTestCategory.Voron, RavenArchitecture.AllX64)]
        public async Task CanCleanCounterTombstones()
        {
            await using var replayStream = typeof(RavenDB_23409).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB-23409.rec");
            Assert.NotNull(replayStream);
        
            using var store = GetDocumentStore();
            var command = new GetNextOperationIdCommand();
            await store.Commands().ExecuteAsync(command);
            var r = store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));
        }
    }
}
