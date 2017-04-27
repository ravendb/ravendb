//-----------------------------------------------------------------------
// <copyright file="Expiration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Server.Expiration;
using Raven.Client.Server.Operations;
using Raven.Client.Util;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Documents.Expiration
{
    public class Expiration : RavenTestBase
    {
        private static async Task SetupExpiration(DocumentStore store)
        {
            var config = new ExpirationConfiguration
            {
                Active = true,
                DeleteFrequencySeconds = 100,
            };
            await store.Admin.Server.SendAsync(new ConfigureExpirationOperation(config));
        }

        [Fact]
        public async Task CanAddEntityWithExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry()
        {
            using (var store = GetDocumentStore())
            {
                await SetupExpiration(store);

                var company = new Company { Name = "Company Name" };
                var expiry = SystemTime.UtcNow.AddMinutes(5);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[Constants.Documents.Expiration.ExpirationDate] = expiry.ToString(Default.DateTimeOffsetFormatsToWrite);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company2 = await session.LoadAsync<Company>(company.Id);
                    Assert.NotNull(company2);
                    var metadata = session.Advanced.GetMetadataFor(company2);
                    var expirationDate = metadata.GetString(Constants.Documents.Expiration.ExpirationDate);
                    Assert.NotNull(expirationDate);
                    var dateTime = DateTime.ParseExact(expirationDate, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                    Assert.Equal(DateTimeKind.Utc, dateTime.Kind);
                    Assert.Equal(expiry.ToString("O"), expirationDate);
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var expiredDocumentsCleaner = database.BundleLoader.ExpiredDocumentsCleaner;

                await expiredDocumentsCleaner.CleanupExpiredDocs();

                using (var session = store.OpenAsyncSession())
                {
                    var company2 = await session.LoadAsync<Company>(company.Id);
                    Assert.Null(company2);
                }
            }
        }

        [Theory]
        [InlineData(10)]
        [InlineData(100)]
        public async Task CanAddALotOfEntitiesWithSameExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry(int count)
        {
            var company = new { Name = "Company Name" };

            using (var store = GetDocumentStore())
            {
                await SetupExpiration(store);

                var expiry = SystemTime.UtcNow.AddMinutes(5);
                var metadata = new Dictionary<string, object>
                {
                    [Constants.Documents.Expiration.ExpirationDate] = expiry.ToString(Default.DateTimeOffsetFormatsToWrite)
                };
                var metadata2 = new Dictionary<string, object>
                {
                    [Constants.Documents.Expiration.ExpirationDate] = expiry.AddMinutes(1).ToString(Default.DateTimeOffsetFormatsToWrite)
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

                var database = await GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var expiredDocumentsCleaner = database.BundleLoader.ExpiredDocumentsCleaner;

                await expiredDocumentsCleaner.CleanupExpiredDocs();

                var stats = await store.Admin.SendAsync(new GetStatisticsOperation());
                Assert.Equal(0, stats.CountOfDocuments);
            }
        }
    }
}