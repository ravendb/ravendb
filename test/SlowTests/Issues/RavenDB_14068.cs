using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Orders;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Client.Json;
using Raven.Client.Util;
using Raven.Server.Documents.Expiration;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14068 : RavenTestBase
    {
        public RavenDB_14068(ITestOutputHelper output) : base(output)
        {
        }

        private async Task SetupExpiration(DocumentStore store)
        {
            var config = new ExpirationConfiguration
            {
                Disabled = false,
                DeleteFrequencyInSec = 100,
            };

            await ExpirationHelper.SetupExpiration(store, Server.ServerStore, config);
        }

        [Fact]
        public async Task WillCleanupAllExpiredDocumentsInSingleRun_EvenWhenMoreThanBatchSize()
        {
            var count = ExpiredDocumentsCleaner.BatchSize + 10;

            var company = new { Name = "Company Name" };

            using (var store = GetDocumentStore())
            {
                await SetupExpiration(store);

                var expiry = SystemTime.UtcNow.AddMinutes(5);
                var metadata = new Dictionary<string, object>
                {
                    [Constants.Documents.Metadata.Expires] = expiry.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite)
                };
                var metadata2 = new Dictionary<string, object>
                {
                    [Constants.Documents.Metadata.Expires] = expiry.AddMinutes(1).ToString(DefaultFormat.DateTimeOffsetFormatsToWrite)
                };

                using (var commands = store.Commands())
                {
                    for (var i = 0; i < count; i++)
                    {
                        await commands.PutAsync("companies/" + i, null, company, metadata);
                        await commands.PutAsync("companies-type2/" + i, null, company, metadata2);
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company2 = await session.LoadAsync<Company>("companies/" + (count - 1));
                    Assert.NotNull(company2);
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var expiredDocumentsCleaner = database.ExpiredDocumentsCleaner;
                await expiredDocumentsCleaner.CleanupExpiredDocs();

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(0, stats.CountOfDocuments);
            }
        }

        [Fact]
        public async Task CanDisableAndEnableExpirationAndRefresh()
        {
            using (var store = GetDocumentStore())
            {
                Assert.False(await DisableExpiration(store));
                Assert.False(await DisableRefresh(store));

                Assert.False(await DisableExpiration(store));
                Assert.False(await DisableRefresh(store));

                Assert.True(await EnableExpiration(store));
                Assert.True(await EnableExpiration(store));

                Assert.True(await EnableRefresh(store));
                Assert.True(await DisableExpiration(store));

                Assert.True(await EnableRefresh(store));
                Assert.True(await EnableExpiration(store));

                Assert.True(await EnableRefresh(store));
                Assert.True(await DisableRefresh(store));

                Assert.False(await DisableExpiration(store));
            }
        }

        private async Task<bool> EnableRefresh(IDocumentStore store)
        {
            var config = new RefreshConfiguration
            {
                Disabled = false,
                RefreshFrequencyInSec = 100,
            };

            await RefreshHelper.SetupExpiration(store, Server.ServerStore, config);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            return database.ExpiredDocumentsCleaner != null;
        }

        private async Task<bool> DisableRefresh(IDocumentStore store)
        {
            var config = new RefreshConfiguration
            {
                Disabled = true,
                RefreshFrequencyInSec = 100,
            };

            await RefreshHelper.SetupExpiration(store, Server.ServerStore, config);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            return database.ExpiredDocumentsCleaner != null;
        }

        private async Task<bool> EnableExpiration(IDocumentStore store)
        {
            var config = new ExpirationConfiguration
            {
                Disabled = false,
                DeleteFrequencyInSec = 100,
            };

            await ExpirationHelper.SetupExpiration(store, Server.ServerStore, config);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            return database.ExpiredDocumentsCleaner != null;
        }

        private async Task<bool> DisableExpiration(IDocumentStore store)
        {
            var config = new ExpirationConfiguration
            {
                Disabled = true,
                DeleteFrequencyInSec = 100,
            };

            await ExpirationHelper.SetupExpiration(store, Server.ServerStore, config);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            return database.ExpiredDocumentsCleaner != null;
        }
    }
}
