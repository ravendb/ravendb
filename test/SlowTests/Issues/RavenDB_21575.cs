using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Orders;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Expiration;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21575 : RavenTestBase
{
    public RavenDB_21575(ITestOutputHelper output) : base(output)
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

    [RavenFact(RavenTestCategory.ExpirationRefresh)]
    public async Task WillNotThrowOnAttemptToExpireAlreadyDeletedDocument()
    {
        using (var store = GetDocumentStore())
        {
            await SetupExpiration(store);

            var company = new Company
            {
                Name = "Company Name"
            };
            var expiry = DateTime.Now.ToUniversalTime(); 

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company, "companies/1");
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.Expires] = expiry.ToString(DefaultFormat.DateTimeFormatsToWrite);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company, "companies/1");
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.Expires] = expiry.AddMinutes(1).ToString(DefaultFormat.DateTimeFormatsToWrite);
                await session.SaveChangesAsync();
            }

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var expiredDocumentsCleaner = database.ExpiredDocumentsCleaner;
            await expiredDocumentsCleaner.CleanupExpiredDocs();

            using (var session = store.OpenAsyncSession())
            {
                var loaded = await session.LoadAsync<Company>("companies/1");

                Assert.Null(loaded);
            }
        }
    }
}
