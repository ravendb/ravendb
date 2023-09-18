using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json;
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


public class DataArchivalDataSubscriptionsTests(ITestOutputHelper output) : RavenTestBase(output)
{
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
                Query = "from Companies", Name = "Created", ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior.ArchivedOnly
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
            var subsId = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions {Query = "from Companies", Name = "Created",});
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
            var subsId = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions {Query = "from Companies", Name = "Created"});
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
                Query = "from Companies", Name = "Created", ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior.IncludeArchived
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

            Assert.Equal(ArchivedDataProcessingBehavior.ArchivedOnly, (await store.Subscriptions.GetSubscriptionStateAsync("Created")).ArchivedDataProcessingBehavior);
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
            var subsId = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions {Query = "from Companies", Name = "Created",});
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


    private class SubscriptionTryoutOperation : RavenCommand<string>, IOperation<string>
    {
        private readonly SubscriptionTryout _tryout;

        internal SubscriptionTryoutOperation(SubscriptionTryout tryout)
        {
            _tryout = tryout;
            ResponseType = RavenCommandResponseType.Raw;
        }

        public RavenCommand<string> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return this;
        }

        public override bool IsReadRequest { get; } = false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName(nameof(SubscriptionTryout.ChangeVector));
                        writer.WriteString(_tryout.ChangeVector);
                        writer.WritePropertyName(nameof(SubscriptionTryout.Query));
                        writer.WriteString(_tryout.Query);
                        writer.WritePropertyName(nameof(SubscriptionTryout.ArchivedDataProcessingBehavior));
                        if (_tryout.ArchivedDataProcessingBehavior is null)
                            writer.WriteNull();
                        else
                            writer.WriteString(_tryout.ArchivedDataProcessingBehavior.ToString());
                        writer.WriteEndObject();
                    }
                }, DocumentConventions.Default)
            };

            var sb = new StringBuilder($"{node.Url}/databases/{node.Database}/subscriptions/try?pageSize=10");

            url = sb.ToString();

            return request;
        }

        public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
        {
            Result = new StreamReader(stream).ReadToEnd();
        }
    }
    
    [Fact]
    public async Task DataSubscriptionTryoutResultsAreConsistentWithCurrentArchivedDataBehavior()
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
            // Insert document with archive time before activating the archival
            var company1 = new Company {Name = "Company Name 1", Address1 = "Dabrowskiego 6"};
            var company2 = new Company {Name = "Company Name 2", Address1 = "Dabrowskiego 6"};
            var company3 = new Company {Name = "Company Name 3", Address1 = "Dabrowskiego 6"};
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company1);
                var metadata = session.Advanced.GetMetadataFor(company1);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }
            // Add more documents that'll be left unarchived
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company2);
                await session.StoreAsync(company3);
                await session.SaveChangesAsync();
            }

            // Activate the archival manually
            await SetupDataArchival(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();
            
            var result = store.Operations.Send(new SubscriptionTryoutOperation(new SubscriptionTryout
            {
                Query = "from Companies",
                ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior.ArchivedOnly
            }));

            Assert.Contains("Company Name 1", result);
            Assert.DoesNotContain("Company Name 2", result);
            Assert.DoesNotContain("Company Name 3", result);
            
            result = store.Operations.Send(new SubscriptionTryoutOperation(new SubscriptionTryout
            {
                Query = "from Companies",
                ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior.ExcludeArchived
            }));
            
            Assert.DoesNotContain("Company Name 1", result);
            Assert.Contains("Company Name 2", result);
            Assert.Contains("Company Name 3", result);
            
            result = store.Operations.Send(new SubscriptionTryoutOperation(new SubscriptionTryout
            {
                Query = "from Companies",
                ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior.IncludeArchived
            }));
            
            Assert.Contains("Company Name 1", result);
            Assert.Contains("Company Name 2", result);
            Assert.Contains("Company Name 3", result);
            
            // default from configuration - ArchivedOnly
            result = store.Operations.Send(new SubscriptionTryoutOperation(new SubscriptionTryout
            {
                Query = "from Companies",
            }));
            
            Assert.Contains("Company Name 1", result);
            Assert.DoesNotContain("Company Name 2", result);
            Assert.DoesNotContain("Company Name 3", result);
        }
    }
}

