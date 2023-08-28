using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Util;
using Raven.Server.Config;
using SlowTests.Core.Utils.Entities;
using Sparrow;
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
                    dr.Settings[RavenConfiguration.GetKey(x => x.Subscriptions.DefaultArchivedDataProcessingBehavior)] = "ArchivedOnly";
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
    }

