using System.Threading.Tasks;
using Xunit.Abstractions;
using System;
using FastTests;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_21028 : RavenTestBase
    {
        
        public RavenDB_21028(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Smuggler)]
        public async Task WaitForCompletionShouldNotHangOnFailureDuringExport()
        {
            using var store = GetDocumentStore();
            await store.Maintenance.SendAsync(new CreateSampleDataOperation());

            var file = GetTempFileName();
            var op = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions { EncryptionKey = "fakeKey" }, file);

            await Assert.ThrowsAsync<RavenException>(async () => await op.WaitForCompletionAsync(TimeSpan.FromSeconds(10)));

        }

    }
}
