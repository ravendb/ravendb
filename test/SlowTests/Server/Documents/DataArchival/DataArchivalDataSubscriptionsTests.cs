using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using SlowTests.Core.Utils.Entities;
using Sparrow;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.DataArchival;


    public class DataArchivalDataSubscriptionsTests: RavenTestBase
    {
        public DataArchivalDataSubscriptionsTests(ITestOutputHelper output) : base(output)
        {
        }

        private async Task SetupDataArchival(IDocumentStore store)
        {
            var config = new DataArchivalConfiguration {Disabled = false, ArchiveFrequencyInSec = 100};

            await DataArchivalHelper.SetupDataArchival(store, Server.ServerStore, config);
        }
        
        
    
        [Fact]
        public async Task DataSubscriptionWillOperateOnlyOnArchivedDocuments()
        {
            using (var store = GetDocumentStore())
            {
                List<Company> companies = new(); 
                
                // Insert document with archive time before activating the archival
                var company = new Company {Name = "Company Name", Address1 = "Dabrowskiego 6"};
                var retires = SystemTime.UtcNow.AddMinutes(5);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    await session.SaveChangesAsync();
                }
                
                // Set-up the subscription and run the worker
                var subsId = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = "from Companies",
                    Name = "Created",
                    ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior.ArchivedOnly
                });
                var worker = store.Subscriptions.GetSubscriptionWorker<Company>(subsId);
                var t = worker.Run(batch => companies.AddRange(batch.Items.Select(item => item.Result)));
                
                WaitForValue(() => companies.Count, 1, 5000);
                Assert.Equal(0, companies.Count);

                // Activate the archival manually
                await SetupDataArchival(store);
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var documentsArchiver = database.DataArchivist;
                await documentsArchiver.ArchiveDocs();

                await AssertWaitForCountAsync(() => Task.FromResult(companies), 1);
                Assert.Equal(1, companies.Count());
            }
        }
        
        [Fact]
        public async Task DataSubscriptionWillOperateOnlyOnArchivedDocuments_AfterChangingDefaultBehavior()
        {
            Options options = new()
            {
                ModifyDatabaseRecord = dr =>
                {
                    dr.Settings[RavenConfiguration.GetKey(x => x.Subscriptions.ArchivedDataProcessingBehavior)] = "ArchivedOnly";
                }
            };
            
            using (var store = GetDocumentStore(options))
            {
                List<Company> companies = new(); 
                
                // Insert document with archive time before activating the archival
                var company = new Company {Name = "Company Name", Address1 = "Dabrowskiego 6"};
                var retires = SystemTime.UtcNow.AddMinutes(5);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    await session.SaveChangesAsync();
                }
                
                // Set-up the subscription and run the worker
                var subsId = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = "from Companies",
                    Name = "Created",
                });
                var worker = store.Subscriptions.GetSubscriptionWorker<Company>(subsId);
                var t = worker.Run(batch => companies.AddRange(batch.Items.Select(item => item.Result)));
                
                WaitForValue(() => companies.Count, 1, 5000);
                Assert.Equal(0, companies.Count);

                // Activate the archival manually
                await SetupDataArchival(store);
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var documentsArchiver = database.DataArchivist;
                await documentsArchiver.ArchiveDocs();

                await AssertWaitForCountAsync(() => Task.FromResult(companies), 1);
                Assert.Equal(1, companies.Count());
            }
        }
        
        
        [Fact]
        public async Task DataSubscriptionWillOperateOnlyOnUnarchivedDocuments()
        {
            using (var store = GetDocumentStore())
            {
                List<Company> companies = new(); 
                
                // Insert document with archive time before activating the archival
                var company = new Company {Name = "Company Name", Address1 = "Dabrowskiego 6"};
                var retires = SystemTime.UtcNow.AddMinutes(5);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    await session.SaveChangesAsync();
                }
                
                // Set-up the subscription and run the worker
                var subsId = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = "from Companies",
                    Name = "Created"
                });
                var worker = store.Subscriptions.GetSubscriptionWorker<Company>(subsId);
                var t = worker.Run(batch => companies.AddRange(batch.Items.Select(item => item.Result)));

                WaitForValue(() => companies.Count, 1, 5000);
                Assert.Equal(companies.Count, 1);
                
                companies.Clear();

                // Activate the archival manually
                await SetupDataArchival(store);
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var documentsArchiver = database.DataArchivist;
                await documentsArchiver.ArchiveDocs();

                WaitForValue(() => companies.Count, 1, 5000);
                Assert.Equal(companies.Count, 0);
            }
        }
        
        [Fact]
        public async Task DataSubscriptionWillOperateOnlyOnBothArchivedAndUnarchivedDocuments()
        {
            using (var store = GetDocumentStore())
            {
                List<Company> companies = new(); 
                
                // Insert document with archive time before activating the archival
                var company = new Company {Name = "Company Name", Address1 = "Dabrowskiego 6"};
                var retires = SystemTime.UtcNow.AddMinutes(5);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    await session.SaveChangesAsync();
                }
                
                // Set-up the subscription and run the worker
                var subsId = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = "from Companies",
                    Name = "Created",
                    ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior.IncludeArchived
                });
                var worker = store.Subscriptions.GetSubscriptionWorker<Company>(subsId);
                var t = worker.Run(batch => companies.AddRange(batch.Items.Select(item => item.Result)));

                WaitForValue(() => companies.Count, 1, 5000);
                Assert.Equal(companies.Count, 1);
                
                companies.Clear();

                // Activate the archival manually
                await SetupDataArchival(store);
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var documentsArchiver = database.DataArchivist;
                await documentsArchiver.ArchiveDocs();

                await AssertWaitForCountAsync(() => Task.FromResult(companies), 1);
                Assert.Equal(companies.Count, 1);
            }
        }
        
        [Fact]
        public async Task ArchivedDocumentsDataSubscription_BehaviorWillBeTheSameAfterRestart()
        {
            using (var store = GetDocumentStore())
            {
                // Set-up the subscription
                var subsId = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = "from Companies", Name = "Created", ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior.ArchivedOnly
                });

                // Disable the subscription
                var ongoingTask = (OngoingTaskSubscription)store.Maintenance.Send(new GetOngoingTaskInfoOperation(subsId, OngoingTaskType.Subscription));
                Assert.False(ongoingTask.Disabled);
                store.Maintenance.Send(new ToggleOngoingTaskStateOperation(ongoingTask.TaskId, OngoingTaskType.Subscription, true));
                ongoingTask = (OngoingTaskSubscription)store.Maintenance.Send(new GetOngoingTaskInfoOperation(subsId, OngoingTaskType.Subscription));
                Assert.True(ongoingTask.Disabled);

                // Enable the subscription
                await store.Subscriptions.EnableAsync("Created");
                ongoingTask = (OngoingTaskSubscription)store.Maintenance.Send(new GetOngoingTaskInfoOperation(subsId, OngoingTaskType.Subscription));
                Assert.False(ongoingTask.Disabled);
                
                Assert.Equal(ArchivedDataProcessingBehavior.ArchivedOnly,(await store.Subscriptions.GetSubscriptionStateAsync("Created")).ArchivedDataProcessingBehavior);
            }
        }
        
        
        [Fact]
        public async Task DataSubscriptionsArchivedBehaviorIsPersisted_DatabaseConfigurationChangeWontAffectExistingSubscription()
        {
            Options options = new()
            {
                ModifyDatabaseRecord = dr =>
                {
                    dr.Settings[RavenConfiguration.GetKey(x => x.Subscriptions.ArchivedDataProcessingBehavior)] = "ArchivedOnly";
                }
            };
            
            using (var store = GetDocumentStore(options))
            {
                List<Company> companies = new(); 
                
                // Insert document with archive time before activating the archival
                var company = new Company {Name = "Company Name", Address1 = "Dabrowskiego 6"};
                var retires = SystemTime.UtcNow.AddMinutes(5);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    await session.SaveChangesAsync();
                }
                
                // Set-up the subscription and run the worker
                var subsId = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = "from Companies",
                    Name = "Created",
                });
                var worker = store.Subscriptions.GetSubscriptionWorker<Company>(subsId);
                var t = worker.Run(batch => companies.AddRange(batch.Items.Select(item => item.Result)));
                
                WaitForValue(() => companies.Count, 1, 5000);
                Assert.Equal(0, companies.Count);

                // Activate the archival manually
                await SetupDataArchival(store);
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var dataArchivist = database.DataArchivist;
                await dataArchivist.ArchiveDocs();

                await AssertWaitForCountAsync(() => Task.FromResult(companies), 1);
                Assert.Equal(1, companies.Count);
                
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                record.Settings[RavenConfiguration.GetKey(x => x.Subscriptions.ArchivedDataProcessingBehavior)] = "ExcludeArchived";
                store.Maintenance.Server.Send(new UpdateDatabaseOperation(record, record.Etag));
                
                
                var result = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));
                Assert.True(result.Success);
                Assert.True(result.Disabled);
                
                //wait until disabled databases unload, this is an immediate operation
                Assert.True(await WaitUntilDatabaseHasState(store, TimeSpan.FromSeconds(30), isLoaded: false));
                
                result = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, false));
                Assert.True(result.Success);
                Assert.False(result.Disabled);
                
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }
                
                WaitForValue(() => companies.Count, 2, 5000);
                Assert.Equal(1, companies.Count);

                
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                    await session.SaveChangesAsync();
                }
                
                // Activate the archival manually
                database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                dataArchivist = database.DataArchivist;
                await dataArchivist.ArchiveDocs();
                
                await AssertWaitForCountAsync(() => Task.FromResult(companies), 2, 5000);
                Assert.Equal(2, companies.Count);
            }
        }

        private static async Task<bool> WaitUntilDatabaseHasState(DocumentStore store, TimeSpan timeout, bool isLoaded)
        {
            var requestExecutor = store.GetRequestExecutor();
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var shouldContinue = true;
                var timeoutTask = Task.Delay(timeout);
                while (shouldContinue && timeoutTask.IsCompleted == false)
                {
                    try
                    {
                        var databaseIsLoadedCommand = new IsDatabaseLoadedCommand();
                        await requestExecutor.ExecuteAsync(databaseIsLoadedCommand, context);
                        shouldContinue = databaseIsLoadedCommand.Result.IsLoaded != isLoaded;
                        await Task.Delay(100);
                    }
                    catch (OperationCanceledException)
                    {
                        //OperationCanceledException is thrown if the database is currently shutting down
                    }
                }

                return timeoutTask.IsCompleted == false;
            }
        }
    }

