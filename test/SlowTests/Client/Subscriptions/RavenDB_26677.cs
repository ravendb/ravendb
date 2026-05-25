using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_26677 : RavenTestBase
    {
        public RavenDB_26677(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Subscriptions | RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport)]
        public async Task OngoingTasksEndpointMustNotFailAfterImportingSubscriptionsWithNonPositiveIds()
        {
            const int subscriptionCount = 50;

            var file = GetTempFileName();

            using (var source = GetDocumentStore())
            {
                for (int i = 0; i < subscriptionCount; i++)
                {
                    await source.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                    {
                        Name = $"Sub_{i:D3}",
                        Query = "from Users"
                    });
                }

                var export = await source.Smuggler.ExportAsync(
                    new DatabaseSmugglerExportOptions { OperateOnTypes = DatabaseItemType.Subscriptions },
                    file);
                await export.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }

            using (var targetServer = GetNewServer())
            using (var target = GetDocumentStore(new Options { Server = targetServer }))
            {
                var import = await target.Smuggler.ImportAsync(
                    new DatabaseSmugglerImportOptions { OperateOnTypes = DatabaseItemType.Subscriptions },
                    file);
                await import.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var subscriptions = await target.Subscriptions.GetSubscriptionsAsync(0, int.MaxValue);
                Assert.Equal(subscriptionCount, subscriptions.Count);
                Assert.Contains(subscriptions, x => x.SubscriptionId <= 0);

                var ongoingTasks = await Databases.GetOngoingTasks(target.Database, targetServer);
                Assert.Equal(subscriptionCount, ongoingTasks.Count);
            }
        }
    }
}
