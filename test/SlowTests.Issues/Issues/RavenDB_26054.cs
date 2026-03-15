using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_26054 : RavenTestBase
    {
        public RavenDB_26054(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Revisions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public async Task EnforceRevisionsConfigurationWithThrottling(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                await RevisionsHelper.SetupRevisionsAsync(store, modifyConfiguration: configuration =>
                    configuration.Collections["Users"].PurgeOnDelete = false);

                // Create 30 documents, each with 2 revisions (initial + 1 update)
                for (int i = 0; i < 30; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = $"User{i}" }, $"users/{i}");
                        await session.SaveChangesAsync();
                    }
                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<User>($"users/{i}");
                        user.Name = $"User{i}_updated";
                        await session.SaveChangesAsync();
                    }
                }

                // Change config to keep max 1 revision
                await RevisionsHelper.SetupRevisionsAsync(store, modifyConfiguration: configuration =>
                {
                    configuration.Collections["Users"].PurgeOnDelete = false;
                    configuration.Collections["Users"].MinimumRevisionsToKeep = 1;
                });

                // Enforce with MaxOpsPerSecond = 10
                var sw = Stopwatch.StartNew();
                var result = await store.Operations.SendAsync(
                    new EnforceRevisionsConfigurationOperation(new EnforceRevisionsConfigurationOperation.Parameters
                    {
                        MaxOpsPerSecond = 10
                    }));
                var operationResult = (EnforceConfigurationResult)await result.WaitForCompletionAsync();
                sw.Stop();

                // 30 doc IDs at 10/sec → should take > 1 second
                Assert.True(sw.Elapsed > TimeSpan.FromSeconds(1),
                    $"Expected operation to take more than 1 second due to throttling, but took {sw.Elapsed}");

                Assert.Equal(30, operationResult.ScannedDocuments);
                Assert.Equal(30, operationResult.RemovedRevisions);
            }
        }
    }
}
