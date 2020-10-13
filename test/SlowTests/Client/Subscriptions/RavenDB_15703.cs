using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Smuggler;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_15703 : RavenTestBase
    {
        public RavenDB_15703(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task MustNotFailOnExportingSubscriptions()
        {
            using (var store = GetDocumentStore())
            {
                var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                {
                    OperateOnTypes = DatabaseItemType.Subscriptions
                }, NewDataPath());

                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }
        }
    }
}
