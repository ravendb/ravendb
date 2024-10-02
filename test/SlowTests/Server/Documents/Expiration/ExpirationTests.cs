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
using Raven.Client.Documents.Operations.Refresh;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.DocumentsCompression;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Commands.Expiration;
using Raven.Server.Documents.Expiration;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Tests.Infrastructure.Extensions;
using Xunit;
using Xunit.Abstractions;
using Size = Sparrow.Size;

namespace SlowTests.Server.Documents.Expiration
{
    public class ExpirationTests : ClusterTestBase
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

        [RavenTheory(RavenTestCategory.ExpirationRefresh)]
        [InlineData([true])]
        [InlineData([false])]
        public async Task CanAddEntityWithExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry(bool compressed)
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
                using (var store = GetDocumentStore(new Options
                {
                    ModifyDatabaseRecord = record =>
                    {
                        if (compressed)
                        {
                            record.DocumentsCompression = new DocumentsCompressionConfiguration { CompressAllCollections = true, };
                        }
                    }
                }))
                {
                    await SetupExpiration(store);

                    var company = new Company
                    {
                        Name = "Company Name"
                    };
                    var now = DateTime.Now;// intentionally local time
                    var expiry = now;
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

                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    var expiredDocumentsCleaner = database.ExpiredDocumentsCleaner;
                    await expiredDocumentsCleaner.CleanupExpiredDocs(throwOnError: true);

                    using (var session = store.OpenAsyncSession())
                    {
                        var company2 = await session.LoadAsync<Company>(company.Id);
                        Assert.True(company2 == null,
                            $"company2 == null, dateTimeFormat.Key: {dateTimeFormat.Key}, dateTimeFormat.Value: {dateTimeFormat.Value}, " +
                            $"now: {now}, expiry: {expiry}, {database.Time.GetUtcNow()}");
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.ExpirationRefresh)]
        [InlineData(true, 10)]
        [InlineData(false, 10)]
        [InlineData(true, 100)]
        [InlineData(false, 100)]
        public async Task CanAddALotOfEntitiesWithSameExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry(bool compressed, int count)
        {
            var company = new { Name = "Company Name" };

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    if (compressed)
                    {
                        record.DocumentsCompression = new DocumentsCompressionConfiguration { CompressAllCollections = true, };
                    }
                }
            }))
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

        [RavenTheory(RavenTestCategory.ExpirationRefresh)]
        [InlineData([true])]
        [InlineData([false])]
        public async Task CanAddEntityWithExpiry_BeforeActivatingExpirtaion_WillNotBeAbleToReadItAfterExpiry(bool compressed)
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    if (compressed)
                    {
                        record.DocumentsCompression = new DocumentsCompressionConfiguration { CompressAllCollections = true, };
                    }
                }
            }))
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

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
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

        [RavenTheory(RavenTestCategory.ExpirationRefresh)]
        [InlineData([true])]
        [InlineData([false])]
        public async Task CanSetupExpirationAndRefresh(bool compressed)
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    if (compressed)
                    {
                        record.DocumentsCompression = new DocumentsCompressionConfiguration { CompressAllCollections = true, };
                    }
                }
            }))
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
                DatabaseTopology topology;
                string nodeTag;
                        
                using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                using (serverContext.OpenReadTransaction())
                {
                    topology = database.ServerStore.Cluster.ReadDatabaseTopology(serverContext, database.Name);
                    nodeTag = database.ServerStore.NodeTag;
                }
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var options = new BackgroundWorkParameters(context, SystemTime.UtcNow.AddMinutes(10), topology, nodeTag, 10, MaxItemsToProcess: long.MaxValue);
                    var totalCount = 0;

                    var expired = database.DocumentsStorage.ExpirationStorage.GetDocuments(options, ref totalCount, out _, CancellationToken.None);
                    Assert.Equal(1, expired.Count);

                    var toRefresh = database.DocumentsStorage.RefreshStorage.GetDocuments(options, ref totalCount, out _, CancellationToken.None);
                    Assert.Equal(1, toRefresh.Count);
                }
            }
        }
        
        [RavenTheory(RavenTestCategory.ExpirationRefresh)]
        [InlineData([true])]
        [InlineData([false])]
        public async Task CanRefreshFromClusterTransaction(bool compressed)
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    if (compressed)
                    {
                        record.DocumentsCompression = new DocumentsCompressionConfiguration { CompressAllCollections = true, };
                    }
                }
            }))
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

                DatabaseTopology topology;
                string nodeTag;
                        
                using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                using (serverContext.OpenReadTransaction())
                {
                    topology = database.ServerStore.Cluster.ReadDatabaseTopology(serverContext, database.Name);
                    nodeTag = database.ServerStore.NodeTag;
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    DateTime time = SystemTime.UtcNow.AddMinutes(10);
                    var options = new BackgroundWorkParameters(context, time, topology,nodeTag, 10, MaxItemsToProcess: long.MaxValue);
                    var totalCount = 0;

                    var toRefresh = database.DocumentsStorage.RefreshStorage.GetDocuments(options, ref totalCount, out _, CancellationToken.None);
                    database.DocumentsStorage.RefreshStorage.ProcessDocuments(context, toRefresh, time);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ExpirationRefresh)]
        [InlineData([true])]
        [InlineData([false])]
        public async Task ThrowsIfUsingWrongExpiresOrRefresh(bool compressed)
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    if (compressed)
                    {
                        record.DocumentsCompression = new DocumentsCompressionConfiguration { CompressAllCollections = true, };
                    }
                }
            }))
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
                    Assert.Contains($"The due date format for document '{company.Id.ToLowerInvariant()}' is not valid", error.Message);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company = new Company { Name = "Company Name" };
                    await session.StoreAsync(company);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[Constants.Documents.Metadata.Expires] = "test";
                    metadata[Constants.Documents.Metadata.Refresh] = expires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);

                    var error = await Assert.ThrowsAsync<RavenException>(async () => await session.SaveChangesAsync());
                    Assert.Contains($"The due date format for document '{company.Id.ToLowerInvariant()}' is not valid", error.Message);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ExpirationRefresh)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanPostAndGetDocumentsExpiration(Options options, bool compressed)
        {
            options.ModifyDatabaseRecord = record =>
            {
                if (compressed)
                {
                    record.DocumentsCompression = new DocumentsCompressionConfiguration { CompressAllCollections = true, };
                }
            };
            using (var store = GetDocumentStore(options))
            {
                var exConfig = new ExpirationConfiguration
                {
                    DeleteFrequencyInSec = 60,
                    Disabled = false
                };

                await store.Maintenance.SendAsync(new ConfigureExpirationOperation(exConfig));

                await store.Maintenance.ForTesting(() => new GetDocumentsExpirationConfigurationOperation()).AssertAllAsync((key, expirationConfiguration) =>
                {
                    Assert.NotNull(expirationConfiguration);
                    Assert.Equal(60, expirationConfiguration.DeleteFrequencyInSec);
                    Assert.False(expirationConfiguration.Disabled);
                });
            }
        }

        [RavenTheory(RavenTestCategory.ExpirationRefresh | RavenTestCategory.Sharding)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ShouldDeleteExpiredDocumentsForSharding(bool compressed)
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
            var cluster = await CreateRaftCluster(3, watcherCluster: true, leaderIndex: 0);

            var configuration = new ExpirationConfiguration
            {
                Disabled = false,
                DeleteFrequencyInSec = 100
            };

            var options = Sharding.GetOptionsForCluster(cluster.Leader, 3, 2, 3);
            options.ModifyDatabaseRecord += record =>
            {
                if (compressed)
                {
                    record.DocumentsCompression = new DocumentsCompressionConfiguration { CompressAllCollections = true, };
                }
            };

            using (var store = GetDocumentStore(options))
            {
                foreach (var dateTimeFormat in utcFormats)
                {
                    await store.Maintenance.SendAsync(new ConfigureExpirationOperation(configuration));

                    var expiry = DateTime.Now; // intentionally local time
                    if (dateTimeFormat.Value == DateTimeKind.Utc)
                        expiry = expiry.ToUniversalTime();

                    var numOfDocs = 20;

                    using (var session = store.OpenSession())
                    {
                        for (var i = 0; i < numOfDocs; i++)
                        {
                            var comp = new Company { Name = $"{i}" };
                            session.Store(comp, $"company/{i}");
                            var metadata = session.Advanced.GetMetadataFor(comp);
                            metadata[Constants.Documents.Metadata.Expires] = expiry.ToString(dateTimeFormat.Key);
                        }
                        session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 1, timeout: TimeSpan.FromSeconds(60));
                        session.SaveChanges();
                    }

                    var servers = await ShardingCluster.GetShardsDocumentDatabaseInstancesFor(store, cluster.Nodes);

                    while (Sharding.AllShardHaveDocs(servers) == false)
                    {
                        using (var session = store.OpenSession())
                        {
                            for (var i = numOfDocs; i < numOfDocs + 20; i++)
                            {
                                var comp = new Company { Name = $"{i}" };
                                session.Store(comp, $"company/{i}");
                                var metadata = session.Advanced.GetMetadataFor(comp);
                                metadata[Constants.Documents.Metadata.Expires] = expiry.ToString(dateTimeFormat.Key);
                            }
                            session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 1, timeout: TimeSpan.FromSeconds(60));
                            session.SaveChanges();
                        }

                        numOfDocs += 20;
                    }

                    for (var i = 0; i < numOfDocs; i++)
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            var comp = await session.LoadAsync<Company>($"company/{i}");
                            Assert.NotNull(comp);
                            var metadata = session.Advanced.GetMetadataFor(comp);
                            var expirationDate = metadata.GetString(Constants.Documents.Metadata.Expires);
                            Assert.NotNull(expirationDate);
                            var dateTime = DateTime.ParseExact(expirationDate, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                            Assert.Equal(dateTimeFormat.Value, dateTime.Kind);
                            Assert.Equal(expiry.ToString(dateTimeFormat.Key), expirationDate);
                        }
                    }

                    foreach (var kvp in servers)
                    {
                        foreach (var database in kvp.Value)
                        {
                            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);

                            ExpiredDocumentsCleaner cleaner = null;
                            Assert.True(WaitForValue(() =>
                            {
                                cleaner = database.ExpiredDocumentsCleaner;
                                return cleaner != null;
                            }, expectedVal: true));

                            await cleaner.CleanupExpiredDocs();
                        }
                    }

                    for (var i = 0; i < numOfDocs; i++)
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            var company = await session.LoadAsync<Company>($"company/{i}");
                            Assert.True(company == null,$"ID: company/{i}, format: {dateTimeFormat.Key}, total: {numOfDocs}");
                        }
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.ExpirationRefresh | RavenTestCategory.Configuration)]
        [RavenData(10, true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(10, false, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(5, true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(5, false, DatabaseMode = RavenDatabaseMode.All)]
        public async Task ExpirationWithMaxItemsToProcessConfiguredShouldWork(Options options, int batchSize, bool compressed)
        {
            using (var store = GetDocumentStore(options))
            {
                if (compressed)
                {
                    var documentsCompression = new DocumentsCompressionConfiguration(true, true);
                    store.Maintenance.Send(new UpdateDocumentsCompressionConfigurationOperation(documentsCompression));
                }
                // Insert documents with expiration before activating the expiration
                var expires = SystemTime.UtcNow.AddMinutes(5);
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var company = new Company { Name = "Company Name", Id = $"companies/{i}$companies/1" };
                        await session.StoreAsync(company);
                        var metadata = session.Advanced.GetMetadataFor(company);
                        metadata[Constants.Documents.Metadata.Expires] = expires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                        await session.SaveChangesAsync();
                    }
                }

                var config = new ExpirationConfiguration
                {
                    Disabled = false,
                    DeleteFrequencyInSec = (long)TimeSpan.FromMinutes(10).TotalSeconds,
                    MaxItemsToProcess = 9
                };

                var database = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "companies/1");
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);

                await ExpirationHelper.SetupExpiration(store, Server.ServerStore, config, database.Name);

                var expiredDocumentsCleaner = database.ExpiredDocumentsCleaner;
                await expiredDocumentsCleaner.CleanupExpiredDocs(batchSize);

                var count = 0;
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var company = await session.LoadAsync<Company>($"companies/{i}$companies/1");
                        if (company != null)
                            count++;
                    }
                }
                Assert.Equal(1, count);
            }
        }

        [RavenFact(RavenTestCategory.ExpirationRefresh)]
        public async Task Can_Expire_Large_Transactions()
        {
            using (var store = GetDocumentStore())
            {
                var expires = SystemTime.UtcNow.AddMinutes(5);
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var company = new Company { Name = "Company Name", Id = $"companies/{i}" };
                        await session.StoreAsync(company);
                        var metadata = session.Advanced.GetMetadataFor(company);
                        metadata[Constants.Documents.Metadata.Expires] = expires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                        await session.SaveChangesAsync();
                    }
                }

                var config = new ExpirationConfiguration
                {
                    Disabled = false,
                    DeleteFrequencyInSec = (long)TimeSpan.FromMinutes(10).TotalSeconds,
                };

                await ExpirationHelper.SetupExpiration(store, Server.ServerStore, config);

                var database = await GetDatabase(store.Database);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                database._maxTransactionSize = new Size(1, SizeUnit.Kilobytes);
                await database.ExpiredDocumentsCleaner.CleanupExpiredDocs();

                using (var session = store.OpenAsyncSession())
                {
                    var count = await session.Query<Company>().CountAsync();
                    Assert.Equal(0, count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ExpirationRefresh | RavenTestCategory.Configuration)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public async Task RefreshWithMaxItemsToProcessConfiguredShouldWork(Options options, bool compressed)
        {
            using (var store = GetDocumentStore(options))
            {
                if (compressed)
                {
                    var documentsCompression = new DocumentsCompressionConfiguration(true, true);
                    store.Maintenance.Send(new UpdateDocumentsCompressionConfigurationOperation(documentsCompression));
                }
                // Insert documents with refresh before activating the refresh
                var refresh = SystemTime.UtcNow.AddMinutes(5);
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var company = new Company { Name = "Company Name", Id = $"companies/{i}$companies/1" };
                        await session.StoreAsync(company);
                        var metadata = session.Advanced.GetMetadataFor(company);
                        metadata[Constants.Documents.Metadata.Refresh] = refresh.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                        await session.SaveChangesAsync();
                    }
                }

                var config = new RefreshConfiguration()
                {
                    Disabled = false,
                    RefreshFrequencyInSec = (long)TimeSpan.FromMinutes(10).TotalSeconds,
                    MaxItemsToProcess = 9
                };

                var database = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "companies/1");
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);

                await RefreshHelper.SetupExpiration(store, Server.ServerStore, config, database.Name);

                DatabaseTopology topology;
                string nodeTag;

                using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                using (serverContext.OpenReadTransaction())
                {
                    topology = database.ServerStore.Cluster.ReadDatabaseTopology(serverContext, database.Name);
                    nodeTag = database.ServerStore.NodeTag;
                }

                DateTime time = SystemTime.UtcNow.AddMinutes(10);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var refreshOptions = new BackgroundWorkParameters(context, time, topology, nodeTag, AmountToTake: 10, MaxItemsToProcess: 10);
                    var totalCount = 0;
                    var toRefresh = database.DocumentsStorage.RefreshStorage.GetDocuments(refreshOptions, ref totalCount, out _, CancellationToken.None);
                    Assert.Equal(10, totalCount);
                }

                var expiredDocumentsCleaner = database.ExpiredDocumentsCleaner;
                await expiredDocumentsCleaner.RefreshDocs();

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var refreshOptions = new BackgroundWorkParameters(context, time, topology, nodeTag, AmountToTake: 10, MaxItemsToProcess: 10);
                    var totalCount = 0;
                    var toRefresh = database.DocumentsStorage.RefreshStorage.GetDocuments(refreshOptions, ref totalCount, out _, CancellationToken.None);
                    Assert.Equal(1, totalCount);
                }
            }
        }
    }
}
