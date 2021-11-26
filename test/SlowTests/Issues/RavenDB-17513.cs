using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Smuggler;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17513 : RavenTestBase
    {
        public RavenDB_17513(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_Import_Subscriptions()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                for (var i = 0; i < 1024; i++)
                {
                    await store1.Subscriptions.CreateAsync<Query.Order>(
                        order => order.Lines.Any(x => x.Discount > 10)
                    );
                }

                var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions
                {
                    OperateOnTypes = DatabaseItemType.Subscriptions
                }, store2.Smuggler);
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                WaitForUserToContinueTheTest(store2);
            }
        }
    }
}
