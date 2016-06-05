//-----------------------------------------------------------------------
// <copyright file="Expiration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Server.Documents.Expiration;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Documents.Expiration
{
    public class Expiration : RavenTestBase
    {
        public Expiration()
        {
            SystemTime.UtcDateTime = () => DateTime.UtcNow;
        }

        protected async Task SetupExpiration(DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new ExpirationConfiguration
                {
                    Active = true,
                    DeleteFrequencySeconds = 100,
                }, Constants.Expiration.RavenExpirationConfiguration);

                await session.SaveChangesAsync();
            }
        }

        [Fact]
        public async Task CanAddEntityWithExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry()
        {
            using (var store = await GetDocumentStore())
            {
                await SetupExpiration(store);

                var company = new Company {Name = "Company Name"};
                var expiry = SystemTime.UtcNow.AddMinutes(5);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    session.Advanced.GetMetadataFor(company)[Constants.Expiration.RavenExpirationDate] = new RavenJValue(expiry.ToString(Default.DateTimeOffsetFormatsToWrite));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company2 = await session.LoadAsync<Company>(company.Id);
                    Assert.NotNull(company2);
                    var metadata = session.Advanced.GetMetadataFor(company2);
                    var expirationDate = metadata["Raven-Expiration-Date"];
                    Assert.NotNull(expirationDate);
                    var dateTime = expirationDate.Value<DateTime>();
                    Assert.Equal(DateTimeKind.Utc, dateTime.Kind);
                    Assert.Equal(expiry.ToString("O"), expirationDate.ToString());
                }

                SystemTime.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);

                (await GetDocumentDatabaseInstanceFor(store)).BundleLoader.ExpiredDocumentsCleaner.CleanupExpiredDocs();

                using (var session = store.OpenAsyncSession())
                {
                    var company2 = await session.LoadAsync<Company>(company.Id);
                    Assert.Null(company2);
                }
            }
        }

        [Fact]
        public async Task CanAddALotOfEntitiesWithSameExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry()
        {
            var company = new { Name = "Company Name" };
            var companyJson = RavenJObject.FromObject(company);
            const int count = 100;

            using (var store = await GetDocumentStore())
            {
                await SetupExpiration(store);

                var expiry = SystemTime.UtcNow.AddMinutes(5);
                var metadata = new RavenJObject
                {
                    [Constants.Expiration.RavenExpirationDate] =
                        new RavenJValue(expiry.ToString(Default.DateTimeOffsetFormatsToWrite))
                };
                var metadata2 = new RavenJObject
                {
                    [Constants.Expiration.RavenExpirationDate] =
                        new RavenJValue(expiry.AddMinutes(1).ToString(Default.DateTimeOffsetFormatsToWrite))
                };
                for (int i = 0; i < count; i++)
                {
                    await store.AsyncDatabaseCommands.PutAsync("companies/" + i, null, companyJson, metadata);
                    await store.AsyncDatabaseCommands.PutAsync("companies-type2/" + i, null, companyJson, metadata2);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company2 = await session.LoadAsync<Company>("companies/" + (count - 1));
                    Assert.NotNull(company2);
                }

                SystemTime.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);

                var expiredDocumentsCleaner =
                    (await GetDocumentDatabaseInstanceFor(store)).BundleLoader.ExpiredDocumentsCleaner;
                expiredDocumentsCleaner.CleanupExpiredDocs();

                var stats = await store.AsyncDatabaseCommands.GetStatisticsAsync();
                Assert.Equal(1, stats.CountOfDocuments);
            }
        }

        public override void Dispose()
        {
            SystemTime.UtcDateTime = null;
            base.Dispose();
        }
    }
}