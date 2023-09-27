using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Smuggler;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_15703 : RavenTestBase
    {
        public RavenDB_15703(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Subscriptions | RavenTestCategory.BackupExportImport | RavenTestCategory.Smuggler)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task MustNotFailOnExportingSubscriptions(Options options)
        {
            using (var store = GetDocumentStore(options))
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
