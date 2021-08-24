//-----------------------------------------------------------------------
// <copyright file="ExpirationTests.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server.Documents.Expiration;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Expiration
{
    public class ExpirationTests : RavenTestBase
    {
        public ExpirationTests(ITestOutputHelper output) : base(output)
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
        public async Task CanAddEntityWithExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry()
        {
            var utcFormats = new Dictionary<string, DateTimeKind>
            {
                {DefaultFormat.DateTimeFormatsToRead[0], DateTimeKind.Utc},
                {DefaultFormat.DateTimeFormatsToRead[1], DateTimeKind.Unspecified},
                {DefaultFormat.DateTimeFormatsToRead[2], DateTimeKind.Local},
                {DefaultFormat.DateTimeFormatsToRead[3], DateTimeKind.Utc},
                {DefaultFormat.DateTimeFormatsToRead[4], DateTimeKind.Unspecified},
                {DefaultFormat.DateTimeFormatsToRead[5], DateTimeKind.Utc},
                {DefaultFormat.DateTimeFormatsToRead[6], DateTimeKind.Utc},
            };
            Assert.Equal(utcFormats.Count, DefaultFormat.DateTimeFormatsToRead.Length);

            foreach (var dateTimeFormat in utcFormats)
            {
                using (var store = GetDocumentStore())
                {
                    await SetupExpiration(store);

                    var company = new Company
                    {
                        Name = "Company Name"
                    };
                    var expiry = DateTime.Now; // intentionally local time
                    if (dateTimeFormat.Value == DateTimeKind.Utc)
                        expiry = expiry.ToUniversalTime();

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(company);
                        var metadata = session.Advanced.GetMetadataFor(company);
                        metadata[Constants.Documents.Metadata.Expires] = expiry.ToString(dateTimeFormat.Key);
                        await session.SaveChangesAsync();
                    }

                    using (var session = store.OpenAsyncSession())
                    {
                        var company2 = await session.LoadAsync<Company>(company.Id);
                        Assert.NotNull(company2);
                        var metadata = session.Advanced.GetMetadataFor(company2);
                        var expirationDate = metadata.GetString(Constants.Documents.Metadata.Expires);
                        Assert.NotNull(expirationDate);
                        var dateTime = DateTime.ParseExact(expirationDate, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                        Assert.Equal(dateTimeFormat.Value, dateTime.Kind);
                        Assert.Equal(expiry.ToString(dateTimeFormat.Key), expirationDate);
                    }

                    var database = await GetDocumentDatabaseInstanceFor(store);
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    var expiredDocumentsCleaner = database.ExpiredDocumentsCleaner;
                    await expiredDocumentsCleaner.CleanupExpiredDocs();

                    using (var session = store.OpenAsyncSession())
                    {
                        var company2 = await session.LoadAsync<Company>(company.Id);
                        Assert.Null(company2);
                    }
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

                var database = await GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var expiredDocumentsCleaner = database.ExpiredDocumentsCleaner;
                await expiredDocumentsCleaner.CleanupExpiredDocs();

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(0, stats.CountOfDocuments);
            }
        }

        [Fact]
        public async Task CanAddEntityWithExpiry_BeforeActivatingExpirtaion_WillNotBeAbleToReadItAfterExpiry()
        {
            using (var store = GetDocumentStore())
            {
                // Insert document with expiration before activating the expiration
                var company = new Company { Name = "Company Name" };
                var expires = SystemTime.UtcNow.AddMinutes(5);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[Constants.Documents.Metadata.Expires] = expires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    await session.SaveChangesAsync();
                }

                // Activate the expiration
                await SetupExpiration(store);

                var database = await GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var expiredDocumentsCleaner = database.ExpiredDocumentsCleaner;
                await expiredDocumentsCleaner.CleanupExpiredDocs();

                using (var session = store.OpenAsyncSession())
                {
                    var company2 = await session.LoadAsync<Company>(company.Id);
                    Assert.Null(company2);
                }
            }
        }

        [Fact]
        public async Task CanSetupExpirationAndRefresh()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var expires = SystemTime.UtcNow.AddMinutes(5);
                    var company = new Company { Name = "Company Name" };
                    await session.StoreAsync(company);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[Constants.Documents.Metadata.Expires] = expires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    metadata[Constants.Documents.Metadata.Refresh] = expires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    await session.SaveChangesAsync();
                }

                var database = await GetDatabase(store.Database);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var options = new ExpirationStorage.ExpiredDocumentsOptions(context, SystemTime.UtcNow.AddMinutes(10), true, 10);
                    
                    var expired = database.DocumentsStorage.ExpirationStorage.GetExpiredDocuments(options, out _, CancellationToken.None);
                    Assert.Equal(1, expired.Count);

                    var toRefresh = database.DocumentsStorage.ExpirationStorage.GetDocumentsToRefresh(options, out _, CancellationToken.None);
                    Assert.Equal(1, toRefresh.Count);
                }
            }
        }
        
        [Fact]
        public async Task CanRefreshFromClusterTransaction()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode =  TransactionMode.ClusterWide
                }))
                {
                    var expires = database.Time.GetUtcNow().AddMinutes(5);
                    var company = new Company { Name = "Company Name" };
                    await session.StoreAsync(company);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[Constants.Documents.Metadata.Refresh] = expires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    await session.SaveChangesAsync();
                }


                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    DateTime time = SystemTime.UtcNow.AddMinutes(10);
                    var options = new ExpirationStorage.ExpiredDocumentsOptions(context, time, true, 10);
                    var toRefresh = database.DocumentsStorage.ExpirationStorage.GetDocumentsToRefresh(options, out _, CancellationToken.None);
                    database.DocumentsStorage.ExpirationStorage.RefreshDocuments(context, toRefresh, time);
                }
            }
        }

        [Fact]
        public async Task ThrowsIfUsingWrongExpiresOrRefresh()
        {
            using (var store = GetDocumentStore())
            {
                var expires = SystemTime.UtcNow.AddMinutes(5);
                
                using (var session = store.OpenAsyncSession())
                {
                    var company = new Company { Name = "Company Name" };
                    await session.StoreAsync(company);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[Constants.Documents.Metadata.Expires] = expires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    metadata[Constants.Documents.Metadata.Refresh] = "test";
                    
                    var error = await Assert.ThrowsAsync<RavenException>(async () => await session.SaveChangesAsync());
                    Assert.Contains($"The expiration date format for document '{company.Id.ToLowerInvariant()}' is not valid", error.Message);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company = new Company { Name = "Company Name" };
                    await session.StoreAsync(company);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[Constants.Documents.Metadata.Expires] = "test";
                    metadata[Constants.Documents.Metadata.Refresh] = expires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);

                    var error = await Assert.ThrowsAsync<RavenException>(async () => await session.SaveChangesAsync());
                    Assert.Contains($"The expiration date format for document '{company.Id.ToLowerInvariant()}' is not valid", error.Message);
                }
            }
        }
    }
}
