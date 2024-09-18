﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Attachments.Retired;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.ServerWide.Context;
using SlowTests.Client.Attachments;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Attachments;

public abstract class RetiredAttachmentsHolder<TSettings> : RetiredAttachmentsHolderBase
    where TSettings : ICloudBackupSettings
{
    public TSettings Settings;

    protected RetiredAttachmentsHolder(ITestOutputHelper output) : base(output)
    {
    }

    public abstract IAsyncDisposable CreateCloudSettings([CallerMemberName] string caller = null);
    protected abstract Task<List<FileInfoDetails>> GetBlobsFromCloudAndAssertForCount(TSettings settings, int expected, int timeout = 120_000);
    public abstract Task DeleteObjects(TSettings s3Settings);
    public abstract Task PutRetireAttachmentsConfiguration(DocumentStore store, TSettings settings, List<string> collections = null, string database = null);

    public Action<RetiredAttachmentsConfiguration> ModifyRetiredAttachmentsConfig = null;


    //TODO: egor this should be generic method (and class :) )
    //public static async Task PutRetireAttachmentsConfiguration2(DocumentStore store, AzureSettings settings, List<string> collections = null, string database = null)
    //{
    //    if (collections == null)
    //        collections = new List<string> { "Orders" };
    //    if (string.IsNullOrEmpty(database))
    //        database = store.Database;
    //    await store.Maintenance.ForDatabase(database).SendAsync(new ConfigureRetireAttachmentsOperation(new RetireAttachmentsConfiguration()
    //    {
    //        AzureSettings = settings,
    //        Disabled = false,

    //        RetirePeriods = collections.ToDictionary(x => x, x => TimeSpan.FromMinutes(3)),

    //        RetireFrequencyInSec = 1000
    //    }));
    //}

    public async ValueTask DisposeAttachmentsAndDeleteObjects()
    {
        foreach (var attachment in Attachments)
        {
            if (attachment.Stream == null)
                continue;

            try
            {
                await attachment.Stream.DisposeAsync();
            }
            catch
            {
                // ignored
            }
        }

        await DeleteObjects(Settings);
    }

    public async Task PopulateDocsWithRandomAttachments(DocumentStore store, int size, List<(string Id, string Collection)> ids, int attachmentsPerDoc, int start = 0)
    {
        // put attachments
        foreach (var (id, collection) in ids)
        {
            for (int i = 0; i < attachmentsPerDoc; i++)
            {
                var rnd = new Random();
                var b = new byte[size];
                rnd.NextBytes(b);

                var profileStream = new MemoryStream(b);
                var name = $"test_{i + start}.png";
                await store.Operations.SendAsync(new PutAttachmentOperation(id, name, profileStream, "image/png"));

                profileStream.Position = 0;
                Attachments.Add(new RetiredAttachment()
                {
                    Name = name,
                    DocumentId = id,
                    Collection = collection,
                    Stream = profileStream,
                    ContentType = "image/png"
                });
            }
        }
    }

    // add attachments storage key, RetiredKey, and hash to holder
    public void GetStorageAttachmentsMetadataFromAllAttachments(DocumentDatabase database)
    {
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        using (var _documentInfoHelper = new DocumentInfoHelper(context))

        {
            foreach (var attachment in database.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context))
            {
                using (var docId = _documentInfoHelper.GetDocumentId(attachment.Key))
                {
                    var t = Attachments.FirstOrDefault(x => x.DocumentId.ToLowerInvariant() == docId && x.Name == attachment.Name);
                    Assert.NotNull(t);
                    Attachments.Remove(t);
                    t.Key = attachment.Key;
                    t.Hash = attachment.Base64Hash.ToString();
                    t.RetireAt = attachment.RetiredAt;
                    //TODO: egor I can use getcollecton method here
                    t.RetiredKey =
                        $"{Settings.RemoteFolderName}/{t.Collection}/{Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(attachment.Key))}";
                    //  $"{Settings.RemoteFolderName}/{database.Name}/{t.Collection}/{Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(attachment.Key))}";
                    Attachments.Add(t);
                }
            }

            Attachments.GroupBy(x => x.RetiredKey).ToList().ForEach(x =>
            {
                Assert.Single(x);
            });
        }
    }

    public async Task AssertAllRetiredAttachments(IDocumentStore store, List<FileInfoDetails> cloudObjects, int size)
    {
        foreach (var attachment in Attachments)
        {
            Assert.Contains(cloudObjects, x => x.FullPath.Contains(attachment.RetiredKey));

            attachment.Stream.Position = 0;
            await GetAndCompareRetiredAttachment(store, attachment.DocumentId, attachment.Name, attachment.Hash, attachment.ContentType, attachment.Stream, size);
        }
    }

    protected async Task CanUploadRetiredAttachmentToCloudAndGetInternal(int attachmentsCount, int size, List<string> collections = null)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
            var ids = new List<(string Id, string Collection)>();

            using (var store = GetDocumentStore())
            {
                await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc, collections);
            }
        }
    }

    protected async Task CanUploadRetiredAttachmentToCloudAndGetInternal(int attachmentsCount, int size, DocumentStore store, int docsCount,
        List<(string Id, string Collection)> ids, int attachmentsPerDoc, List<string> collections = null, RavenServer server = null)
    {
        await PutRetireAttachmentsConfiguration(store, Settings, collections);
        await CreateDocs(store, docsCount, ids, collections);
        await PopulateDocsWithRandomAttachments(store, size, ids, attachmentsPerDoc);

        var database = await Databases.GetDocumentDatabaseInstanceFor(server ?? Server, store);
        GetStorageAttachmentsMetadataFromAllAttachments(database);
        Assert.Equal(attachmentsCount, Attachments.Count);

        // move in time & start retire
        database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
      //  var sp = Stopwatch.StartNew();
        //Console.WriteLine("Start Retire Attachments");
        await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
        //Console.WriteLine($"Elapsed: {sp.ElapsedMilliseconds}ms");
        var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);

        await AssertAllRetiredAttachments(store, cloudObjects, size);
    }

    protected async Task CanUploadRetiredAttachmentToCloudAndDeleteInternal(int attachmentsCount, int size, List<string> collections = null, bool storageOnly = false)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = RetiredAttachmentsHolderBase.GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
            var ids = new List<(string Id, string Collection)>();
            using (var store = GetDocumentStore())
            {
                await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc, collections);
                foreach (var attachment in Attachments)
                {
                    await store.Operations.SendAsync(new DeleteRetiredAttachmentOperation(attachment.DocumentId, attachment.Name, storageOnly: storageOnly));
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                if (storageOnly == false)
                {
                    await GetBlobsFromCloudAndAssertForCount(Settings, 0);
                }
                else
                {
                    await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount);
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var c = database.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).Count();
                    Assert.Equal(0, c);
                }
            }
        }
    }

    protected async Task CanUploadRetiredAttachmentsToCloudAndGetInBulkInternal(int attachmentsCount, int size, int attachmentsPerDoc, List<string> collections = null)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = attachmentsCount / attachmentsPerDoc;
            var ids = new List<(string Id, string Collection)>();

            using (var store = GetDocumentStore())
            {
                await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc, collections);
                await AssertGetRetiredAttachmentsInBulk(store);
            }
        }
    }
    protected async Task AssertGetRetiredAttachmentsInBulk(DocumentStore store)
    {
        var attachmentRequests = new List<AttachmentRequest>();
        foreach (var attachment in Attachments)
        {
            attachmentRequests.Add(new AttachmentRequest(attachment.DocumentId, attachment.Name));
        }
        var attachmentsEnumerator = await store.Operations.SendAsync(new GetRetiredAttachmentsOperation(attachmentRequests));

        while (attachmentsEnumerator.MoveNext())
        {
            var current = attachmentsEnumerator.Current;
            Assert.NotNull(current);

            var tuple = Attachments.FirstOrDefault(x => x.DocumentId == current.Details.DocumentId && x.Name == current.Details.Name);
            Assert.NotNull(tuple);
            tuple.Stream.Position = 0;

            Assert.True(AttachmentsStreamTests.CompareStreams(current.Stream, tuple.Stream));
            current.Stream?.Dispose();
        }
    }

    protected async Task CanUploadRetiredAttachmentsToCloudAndDeleteInBulkInternal(int attachmentsCount, int size, int attachmentsPerDoc, List<string> collections = null)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = attachmentsCount / attachmentsPerDoc;
            var ids = new List<(string Id, string Collection)>();

            using (var store = GetDocumentStore())
            {
                await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc, collections);
                await AssertDeleteRetiredAttachmentsInBulk(store);
            }
        }
    }
    protected async Task AssertDeleteRetiredAttachmentsInBulk(DocumentStore store)
    {
        var attachmentRequests = new List<AttachmentRequest>();
        foreach (var attachment in Attachments)
        {
            attachmentRequests.Add(new AttachmentRequest(attachment.DocumentId, attachment.Name));
        }
        await store.Operations.SendAsync(new DeleteRetiredAttachmentsOperation(attachmentRequests));

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
        await GetBlobsFromCloudAndAssertForCount(Settings, 0);
    }


    protected async Task CanEtlRetiredAttachmentsToDestinationInternal(int attachmentsCount, int size)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = RetiredAttachmentsHolderBase.GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
            var ids = new List<(string Id, string Collection)>();
            using (var store = GetDocumentStore())
            using (var replica = GetDocumentStore())
            {
                await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc);
                var taskName = "etl-test";
                var csName = "cs-test";

                var configuration = new RavenEtlConfiguration
                {
                    ConnectionStringName = csName,
                    Name = taskName,
                    Transforms = { new Transformation { Name = "S1", Collections = { "Orders" } } }
                };

                var connectionString = new RavenConnectionString { Name = csName, TopologyDiscoveryUrls = replica.Urls, Database = replica.Database, };

                var etlDone = Etl.WaitForEtlToComplete(store);

                Etl.AddEtl(store, configuration, connectionString);

                etlDone.Wait(TimeSpan.FromSeconds(15));

                await PutRetireAttachmentsConfiguration(replica, Settings);

                await AssertGetRetiredAttachmentsInBulk(replica);
            }
        }
    }

    protected async Task CanBackupRetiredAttachmentsInternal(int attachmentsCount, int size)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
            var ids = new List<(string Id, string Collection)>();
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore())
            {
                await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc);

                var config = Backup.CreateBackupConfiguration(backupPath);
                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                // restore the database with a different name
                var restoredDatabaseName = GetDatabaseName();

                using (Backup.RestoreDatabase(store,
                           new RestoreBackupConfiguration { BackupLocation = Directory.GetDirectories(backupPath).First(), DatabaseName = restoredDatabaseName }))
                {
                    var stats = await GetDatabaseStatisticsAsync(store, restoredDatabaseName);
                    Assert.Equal(docsCount, stats.CountOfDocuments); // the marker
                    Assert.Equal(attachmentsCount, stats.CountOfAttachments); // the marker

                    var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
                    using var store2 = new DocumentStore() { Database = restoredDatabaseName, Urls = store.Urls }.Initialize();
                    await AssertAllRetiredAttachments(store2, cloudObjects, size);
                }
            }

        }
    }
    protected async Task CanExternalReplicateRetiredAttachmentAndThenUploadToCloudAndGet(int attachmentsCount, int size)
    {
        using (var store1 = GetDocumentStore())
        using (var store2 = GetDocumentStore())
        {

            //await SetupReplicationAsync(store2, store1);
            await using (var holder = CreateCloudSettings())
            {
                int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                var ids = new List<(string Id, string Collection)>();
                List<string> collections = null;
                await PutRetireAttachmentsConfiguration(store1, Settings);
                await CreateDocs(store1, docsCount, ids);
                await PopulateDocsWithRandomAttachments(store1, size, ids, attachmentsPerDoc);

                await SetupReplicationAsync(store1, store2);
                //Console.WriteLine(store1.Urls.First());


                await EnsureReplicatingAsync(store1, store2);

                var database2 = (await GetDocumentDatabaseInstanceForAsync(store2.Database));
                GetStorageAttachmentsMetadataFromAllAttachments(database2);
                await PutRetireAttachmentsConfiguration(store2, Settings);

                Assert.Equal(attachmentsCount, Attachments.Count);

                // move in time & start retire
                database2.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database2.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
                await AssertAllRetiredAttachments(store2, cloudObjects, size);


                //TODO: egor what happens if no RetireAttachmentsConfiguration on store2?
                //TODO: egor setup s2->s1 replication & after I retire the attachment on s2, I should see it on s1?
            }
        }
    }
    protected async Task CanUploadRetiredAttachmentToCloudAndDeleteInTheSameTimeInternal(int attachmentsCount, int size, int attachmentsPerDoc)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = attachmentsCount / attachmentsPerDoc;
            var ids = new List<(string Id, string Collection)>();
            using (var store = GetDocumentStore())
            {
                List<string> collections = null;
                await PutRetireAttachmentsConfiguration(store, Settings, collections);
                await RetiredAttachmentsHolderBase.CreateDocs(store, docsCount, ids, collections);

                await PopulateDocsWithRandomAttachments(store, size, ids, attachmentsPerDoc / 2, start: 0);

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                GetStorageAttachmentsMetadataFromAllAttachments(database);

                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(5);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount / 2);

                var list = Attachments.ToList();
                var t1 = Task.Run(async () =>
                {
                    await PopulateDocsWithRandomAttachments(store, size, ids, attachmentsPerDoc / 2, start: 1000);
                });
                var t2 = Task.Run(async () =>
                {
                    foreach (var attachment in list)
                    {
                        await store.Operations.SendAsync(new DeleteRetiredAttachmentOperation(attachment.DocumentId, attachment.Name));
                    }
                });
                // TODO: egor check if this is bad since I might skip deletes in the retire sender, but adds I always do in order... del, retire, del, retire, del

                await Task.WhenAll(t1, t2);

                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                GetStorageAttachmentsMetadataFromAllAttachments(database);
                var list2 = Attachments.ToList();
                list2.RemoveAll(x => list.Contains(x));
                foreach (var attachment in list2)
                {
                    await store.Operations.SendAsync(new DeleteRetiredAttachmentOperation(attachment.DocumentId, attachment.Name));
                }

                await WaitAndAssertForValueAsync(async () =>
                {
                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                    var objs = await GetBlobsFromCloudAndAssertForCount(Settings, 0, 1000);
                    return objs.Count;
                }, 0);
            }
        }
    }
    protected async Task ShouldAddRetireAtToAttachmentMetadataInternal()
    {
        await using (var holder = CreateCloudSettings())
        {
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings);

                var id = "Orders/3";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        Id = id,
                        OrderedAt = new DateTime(2024, 1, 1),
                        ShipVia = $"Shippers/2",
                        Company = $"Companies/2"
                    });

                    await session.SaveChangesAsync();
                }

                using var profileStream = new MemoryStream([1, 2, 3]);
                await store.Operations.SendAsync(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                {
                    ctx.OpenReadTransaction();
                    database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.Initialize(ctx);
                    var totalCounnt = 0;
                    var toRetire = database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.GetDocuments(new BackgroundWorkParameters(ctx, DateTime.MaxValue, database.ReadDatabaseRecord(), "A", int.MaxValue), ref totalCounnt, out var _, default);
                    Assert.Equal(1, toRetire.Count);
                }

                var attachment = await store.Operations.SendAsync(new GetAttachmentOperation(id, "test.png", AttachmentType.Document, null));
                Assert.NotNull(attachment.Details.RetireAt);
            }
        }
    }
    protected async Task ShouldThrowUsingRegularAttachmentsApiOnRetiredAttachmentInternal()
    {
        await using (var holder = CreateCloudSettings())
        {
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings);

                var id = "Orders/3";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        Id = id,
                        OrderedAt = new DateTime(2024, 1, 1),
                        ShipVia = $"Shippers/2",
                        Company = $"Companies/2"
                    });

                    await session.SaveChangesAsync();
                }

                using var profileStream = new MemoryStream([1, 2, 3]);
                await store.Operations.SendAsync(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var expiredDocumentsCleaner = database.RetireAttachmentsSender;
                await expiredDocumentsCleaner.RetireAttachments(int.MaxValue, int.MaxValue);
                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, 1);
                // Assert.Contains($"{Settings.RemoteFolderName}/{store.Database}/Orders/b3JkZXJzLzMeZB50ZXN0LnBuZx5FY0RubTNIRGwyek5EQUxSTVE0bEZzQ08zSjJMYjFmTTFvRFdPazJPY3RvPR5pbWFnZS9wbmc=", cloudObjects[0].FullPath);
                Assert.Contains($"{Settings.RemoteFolderName}/Orders/b3JkZXJzLzMeZB50ZXN0LnBuZx5FY0RubTNIRGwyek5EQUxSTVE0bEZzQ08zSjJMYjFmTTFvRFdPazJPY3RvPR5pbWFnZS9wbmc=", cloudObjects[0].FullPath);

                await Assert.ThrowsAsync(typeof(RavenException),
                    async () => await store.Operations.SendAsync(new GetAttachmentOperation(id, "test.png", AttachmentType.Document, null)));
                await Assert.ThrowsAsync(typeof(RavenException),
                    async () => await store.Operations.SendAsync(new DeleteAttachmentOperation(id, "test.png")));
                await Assert.ThrowsAsync(typeof(RavenException),
                    async () => await store.Operations.SendAsync(new GetAttachmentsOperation(new List<AttachmentRequest> { new(id, "test.png") }, AttachmentType.Document)));
            }
        }
    }
    protected async Task CanUploadRetiredAttachmentToCloudInClusterAndDeleteInternal(int attachmentsCount, int size)
    {
        var srcDb = GetDatabaseName();
        var srcRaft = await CreateRaftCluster(3);
        var leader = srcRaft.Leader;
        var srcNodes = await CreateDatabaseInCluster(srcDb, 3, leader.WebUrl);
        using (var src = new DocumentStore { Urls = srcNodes.Servers.Select(s => s.WebUrl).ToArray(), Database = srcDb, }.Initialize())
        {
            DocumentStore store = (DocumentStore)src;
            await using (var holder = CreateCloudSettings())
            {
                int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                var ids = new List<(string Id, string Collection)>();
                await PutRetireAttachmentsConfiguration(store, Settings);
                await CreateDocs(store, docsCount, ids);
                await PopulateDocsWithRandomAttachments(store, size, ids, attachmentsPerDoc);
                Assert.Equal(true, await WaitForChangeVectorInClusterAsync(srcNodes.Servers, srcDb));

                int count = 0;
                DocumentDatabase database = null;
                var retired = await WaitForValueAsync(async () =>
                {
                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)).ConfigureAwait(false);
                    var f = record.Topology.AllNodes.FirstOrDefault();
                    var srv = srcRaft.Nodes.FirstOrDefault(x => x.ServerStore.NodeTag == f);
                    database = await Databases.GetDocumentDatabaseInstanceFor(srv, store);

                    GetStorageAttachmentsMetadataFromAllAttachments(database);
                    Assert.Equal(attachmentsCount, Attachments.Count);

                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    //Console.WriteLine("RETIREATTACHMENTS 1");

                    count += await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                    //Console.WriteLine("RETIREATTACHMENTS 2");

                    return count;
                }, attachmentsCount, interval: 1000);


                Assert.Equal(attachmentsCount, retired);

                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);

                await AssertAllRetiredAttachments(store, cloudObjects, size);
                foreach (var attachment in Attachments)
                {
                    await store.Operations.SendAsync(new DeleteRetiredAttachmentOperation(attachment.DocumentId, attachment.Name));
                }

                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                await GetBlobsFromCloudAndAssertForCount(Settings, 0);
                Assert.Equal(true, await WaitForChangeVectorInClusterAsync(srcNodes.Servers, srcDb));
            }
        }
    }

    protected async Task CanUploadRetiredAttachmentToCloudIfItAlreadyExistsInternal()
    {
        await using (var holder = CreateCloudSettings())
        {
            using var server = GetNewServer();

            using (var store = GetDocumentStore())
            using (var store2 = GetDocumentStore(options: new Options()
            {
                Server = server,
                ModifyDatabaseName = x => store.Database
            }))
            {
                await PutRetireAttachmentsConfiguration(store, Settings);
                await PutRetireAttachmentsConfiguration(store2, Settings);

                var id = "Orders/3";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        Id = id,
                        OrderedAt = new DateTime(2024, 1, 1),
                        ShipVia = $"Shippers/2",
                        Company = $"Companies/2"
                    });

                    await session.SaveChangesAsync();
                }
                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        Id = id,
                        OrderedAt = new DateTime(2024, 1, 1),
                        ShipVia = $"Shippers/2",
                        Company = $"Companies/2"
                    });

                    await session.SaveChangesAsync();
                }
                using var profileStream = new MemoryStream([1, 2, 3]);
                await store.Operations.SendAsync(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));
                profileStream.Position = 0;

                await store2.Operations.SendAsync(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var expiredDocumentsCleaner = database.RetireAttachmentsSender;
                await expiredDocumentsCleaner.RetireAttachments(int.MaxValue, int.MaxValue);
                List<FileInfoDetails> cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, 1);
                // Assert.Contains($"{Settings.RemoteFolderName}/{store.Database}/Orders/b3JkZXJzLzMeZB50ZXN0LnBuZx5FY0RubTNIRGwyek5EQUxSTVE0bEZzQ08zSjJMYjFmTTFvRFdPazJPY3RvPR5pbWFnZS9wbmc=", cloudObjects[0].FullPath);
                Assert.Contains($"{Settings.RemoteFolderName}/Orders/b3JkZXJzLzMeZB50ZXN0LnBuZx5FY0RubTNIRGwyek5EQUxSTVE0bEZzQ08zSjJMYjFmTTFvRFdPazJPY3RvPR5pbWFnZS9wbmc=", cloudObjects[0].FullPath);

                await GetAndCompareRetiredAttachment(store, id, "test.png", "EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", "image/png", profileStream, 3);

                await WaitForTaskDelayIfNeeded();

                var database2 = await Databases.GetDocumentDatabaseInstanceFor(server, store2);
                database2.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var expiredDocumentsCleaner2 = database2.RetireAttachmentsSender;
                await expiredDocumentsCleaner2.RetireAttachments(int.MaxValue, int.MaxValue);

                List<FileInfoDetails> cloudObjects2 = await GetBlobsFromCloudAndAssertForCount(Settings, 1);
                // Assert.Contains($"{Settings.RemoteFolderName}/{store2.Database}/Orders/b3JkZXJzLzMeZB50ZXN0LnBuZx5FY0RubTNIRGwyek5EQUxSTVE0bEZzQ08zSjJMYjFmTTFvRFdPazJPY3RvPR5pbWFnZS9wbmc=", cloudObjects2[0].FullPath);
                Assert.Contains($"{Settings.RemoteFolderName}/Orders/b3JkZXJzLzMeZB50ZXN0LnBuZx5FY0RubTNIRGwyek5EQUxSTVE0bEZzQ08zSjJMYjFmTTFvRFdPazJPY3RvPR5pbWFnZS9wbmc=", cloudObjects2[0].FullPath);

                Assert.NotEqual(cloudObjects[0].LastModified, cloudObjects2[0].LastModified);

                await GetAndCompareRetiredAttachment(store2, id, "test.png", "EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", "image/png", profileStream, 3);
            }

        }
    }

    protected virtual Task WaitForTaskDelayIfNeeded()
    {
        return Task.CompletedTask;
        // noop
    }

    protected async Task UploadRetiredAttachmentToCloudThenManuallyDeleteAndGetShouldThrowInternal()
    {
        await using (var holder = CreateCloudSettings())
        {
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings);

                var id = "Orders/3";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        Id = id,
                        OrderedAt = new DateTime(2024, 1, 1),
                        ShipVia = $"Shippers/2",
                        Company = $"Companies/2"
                    });

                    await session.SaveChangesAsync();
                }

                using var profileStream = new MemoryStream([1, 2, 3]);
                await store.Operations.SendAsync(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var expiredDocumentsCleaner = database.RetireAttachmentsSender;
                await expiredDocumentsCleaner.RetireAttachments(int.MaxValue, int.MaxValue);
                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, 1);
                //  Assert.Contains($"{Settings.RemoteFolderName}/{store.Database}/Orders/b3JkZXJzLzMeZB50ZXN0LnBuZx5FY0RubTNIRGwyek5EQUxSTVE0bEZzQ08zSjJMYjFmTTFvRFdPazJPY3RvPR5pbWFnZS9wbmc=", cloudObjects[0].FullPath);
                Assert.Contains($"{Settings.RemoteFolderName}/Orders/b3JkZXJzLzMeZB50ZXN0LnBuZx5FY0RubTNIRGwyek5EQUxSTVE0bEZzQ08zSjJMYjFmTTFvRFdPazJPY3RvPR5pbWFnZS9wbmc=", cloudObjects[0].FullPath);

                await DeleteObjects(Settings);

                var e = await Assert.ThrowsAsync<RavenException>(async () => await RetiredAttachmentsHolderBase.GetAndCompareRetiredAttachment(store, id, "test.png", "EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", "image/png", profileStream, 3));
                AssertUploadRetiredAttachmentToCloudThenManuallyDeleteAndGetShouldThrowInternal(e);
            }
        }
    }

    protected abstract void AssertUploadRetiredAttachmentToCloudThenManuallyDeleteAndGetShouldThrowInternal(RavenException e);

    protected async Task CanDeleteRetiredAttachmentFromCloudWhenItsNotExistsInCloudInternal()
    {
        await using (var holder = CreateCloudSettings())
        {
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings);

                var id = "Orders/3";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        Id = id,
                        OrderedAt = new DateTime(2024, 1, 1),
                        ShipVia = $"Shippers/2",
                        Company = $"Companies/2"
                    });

                    await session.SaveChangesAsync();
                }

                using var profileStream = new MemoryStream([1, 2, 3]);
                await store.Operations.SendAsync(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var expiredDocumentsCleaner = database.RetireAttachmentsSender;
                await expiredDocumentsCleaner.RetireAttachments(int.MaxValue, int.MaxValue);
                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, 1);

                // Assert.Contains($"{Settings.RemoteFolderName}/{store.Database}/Orders/b3JkZXJzLzMeZB50ZXN0LnBuZx5FY0RubTNIRGwyek5EQUxSTVE0bEZzQ08zSjJMYjFmTTFvRFdPazJPY3RvPR5pbWFnZS9wbmc=", cloudObjects[0].FullPath);
                Assert.Contains($"{Settings.RemoteFolderName}/Orders/b3JkZXJzLzMeZB50ZXN0LnBuZx5FY0RubTNIRGwyek5EQUxSTVE0bEZzQ08zSjJMYjFmTTFvRFdPazJPY3RvPR5pbWFnZS9wbmc=", cloudObjects[0].FullPath);

                await DeleteObjects(Settings);
                await GetBlobsFromCloudAndAssertForCount(Settings, 0);

                await store.Operations.SendAsync(new DeleteRetiredAttachmentOperation(id, "test.png"));

                await expiredDocumentsCleaner.RetireAttachments(int.MaxValue, int.MaxValue);
                await GetBlobsFromCloudAndAssertForCount(Settings, 0);
                var retired = await store.Operations.SendAsync(new GetRetiredAttachmentOperation(id, "test.png"));
                Assert.Null(retired);
            }
        }
    }

    protected async Task CanUploadRetiredAttachmentToCloudInClusterAndGetInternal(int attachmentsCount, int size)
    {
        var srcDb = GetDatabaseName();
        var srcRaft = await CreateRaftCluster(3);
        var leader = srcRaft.Leader;
        var srcNodes = await CreateDatabaseInCluster(srcDb, 3, leader.WebUrl);
        var mentorNode = srcNodes.Servers.First(s => s != leader);
        var mentorTag = mentorNode.ServerStore.NodeTag;
        using (var src = new DocumentStore { Urls = srcNodes.Servers.Select(s => s.WebUrl).ToArray(), Database = srcDb, }.Initialize())
        {
            DocumentStore store = (DocumentStore)src;
            await using (var holder = CreateCloudSettings())
            {
                int docsCount = RetiredAttachmentsHolderBase.GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                var ids = new List<(string Id, string Collection)>();

                await PutRetireAttachmentsConfiguration(store, Settings);
                await RetiredAttachmentsHolderBase.CreateDocs(store, docsCount, ids);
                await PopulateDocsWithRandomAttachments(store, size, ids, attachmentsPerDoc);
                Assert.Equal(true, await WaitForChangeVectorInClusterAsync(srcNodes.Servers, srcDb));


                foreach (var node in srcRaft.Nodes)
                {
                    var database = await Databases.GetDocumentDatabaseInstanceFor(node, store);
                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    {
                        ctx.OpenReadTransaction();
                        database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.Initialize(ctx);
                        var totalCount = 0;

                        var toRetire = database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.GetDocuments(new BackgroundWorkParameters(ctx, DateTime.MaxValue, new DatabaseRecord
                        {
                            Topology = new DatabaseTopology()
                            {
                                Members = [node.ServerStore.NodeTag]
                            },
                            RetiredAttachments = database.ReadDatabaseRecord().RetiredAttachments
                        }, database.ServerStore.NodeTag, int.MaxValue), ref totalCount, out var _, default);
                        Assert.Equal(attachmentsCount, toRetire.Count);
                    }
                }

                int count = 0;
                var retired = await WaitForValueAsync(async () =>
                {
                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)).ConfigureAwait(false);
                    var f = record.Topology.AllNodes.FirstOrDefault();
                    var srv = srcRaft.Nodes.FirstOrDefault(x => x.ServerStore.NodeTag == f);
                    var database = await Databases.GetDocumentDatabaseInstanceFor(srv, store);

                    GetStorageAttachmentsMetadataFromAllAttachments(database);
                    Assert.Equal(attachmentsCount, Attachments.Count);

                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    //Console.WriteLine("RETIREATTACHMENTS 1");

                    count += await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                    //Console.WriteLine("RETIREATTACHMENTS 2");

                    return count;
                }, attachmentsCount, interval: 1000);



                Assert.Equal(attachmentsCount, retired);



                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);

                await AssertAllRetiredAttachments(store, cloudObjects, size);
                // //Console.WriteLine("SLEEEEEEEEEEEEEEEEP");
                //Thread.Sleep(int.MaxValue);
                Assert.Equal(true, await WaitForChangeVectorInClusterAsync(srcNodes.Servers, srcDb));

                // TODO: egor retire tree will be only cleaned on the node that run the retire background work.
                //foreach (var node in srcRaft.Nodes)
                //{
                //    var database = await Databases.GetDocumentDatabaseInstanceFor(node, store);
                //    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                //    {
                //        ctx.OpenReadTransaction();
                //        database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.Initialize(ctx);
                //        var totalCount = 0;

                //        var toRetire = database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.GetDocuments(new BackgroundWorkParameters(ctx, DateTime.MaxValue, new DatabaseRecord
                //        {
                //            Topology = new DatabaseTopology()
                //            {
                //                Members = [node.ServerStore.NodeTag]
                //            },
                //            RetireAttachments = database.ReadDatabaseRecord().RetireAttachments
                //        }, database.ServerStore.NodeTag, int.MaxValue), ref totalCount, out var _, default);
                //        Assert.Equal(0, toRetire.Count);
                //    }
                //}
            }
        }
    }

    protected async Task CanUploadRetiredAttachmentToCloudInClusterAndGet2Internal(int attachmentsCount, int size)
    {
        var srcDb = GetDatabaseName();
        var srcRaft = await CreateRaftCluster(3);
        var leader = srcRaft.Leader;
        //Console.WriteLine(leader.WebUrl);
        var srcNodes = await CreateDatabaseInCluster(srcDb, 3, leader.WebUrl);
        var mentorNode = srcNodes.Servers.First(s => s != leader);
        var mentorTag = mentorNode.ServerStore.NodeTag;
        using (var src = new DocumentStore { Urls = srcNodes.Servers.Select(s => s.WebUrl).ToArray(), Database = srcDb, }.Initialize())
        {
            DocumentStore store = (DocumentStore)src;
            await using (var holder = CreateCloudSettings())
            {
                int docsCount = RetiredAttachmentsHolderBase.GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                var ids = new List<(string Id, string Collection)>();

                await PutRetireAttachmentsConfiguration(store, Settings);
                await RetiredAttachmentsHolderBase.CreateDocs(store, docsCount, ids);
                await PopulateDocsWithRandomAttachments(store, size, ids, attachmentsPerDoc);
                Assert.Equal(true, await WaitForChangeVectorInClusterAsync(srcNodes.Servers, srcDb));

                int count = 0;
                DatabaseOutgoingReplicationHandler halt = null;

                var retired = await WaitForValueAsync(async () =>
                {
                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)).ConfigureAwait(false);
                    var f = record.Topology.AllNodes.FirstOrDefault();
                    var srv = srcRaft.Nodes.FirstOrDefault(x => x.ServerStore.NodeTag == f);
                    var database = await Databases.GetDocumentDatabaseInstanceFor(srv, store);

                    GetStorageAttachmentsMetadataFromAllAttachments(database);
                    Assert.Equal(attachmentsCount, Attachments.Count);

                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    //Console.WriteLine("RETIREATTACHMENTS 1");

                    // last run
                    var toHalt = srcRaft.Nodes.FirstOrDefault(x => x.ServerStore.NodeTag != srv.ServerStore.NodeTag).ServerStore.NodeTag;

                    if (halt == null)
                    {
                        // 1st
                        halt = database.ReplicationLoader.OutgoingHandlers.FirstOrDefault(x =>
                        {
                            if (x.Destination is InternalReplication node)
                            {
                                if (node.NodeTag == toHalt)
                                {
                                    return true;
                                }
                            }
                            return false;

                        });


                        halt.ForTestingPurposesOnly().DebugWaitAndRunReplicationOnce = new ManualResetEventSlim();
                    }
                    else
                    {
                        halt.ForTestingPurposes.DebugWaitAndRunReplicationOnce = null;
                        halt = database.ReplicationLoader.OutgoingHandlers.FirstOrDefault(x =>
                        {
                            if (x.Destination is InternalReplication node)
                            {
                                if (node.NodeTag == toHalt)
                                {
                                    return true;
                                }
                            }
                            return false;

                        });
                        halt.ForTestingPurposesOnly().DebugWaitAndRunReplicationOnce = new ManualResetEventSlim();
                    }


                    //haltedQ.Enqueue(halt);

                    //var z = database.ReplicationLoader.OutgoingHandlers.FirstOrDefault(x=>x.Destination.NodeTag);
                    count += await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                    //Console.WriteLine("RETIREATTACHMENTS 2");
                    //if (count == attachmentsCount)
                    //{
                    //    haltedQ.TryPeek(out var res);
                    //    if (res != halt)
                    //    {

                    //    }
                    //}
                    return count;
                }, attachmentsCount, interval: 1000);


                Assert.NotNull(halt);
                //Console.WriteLine($"Halted: {halt.FromToString}");
                Assert.Equal(attachmentsCount, retired);

                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);

                await AssertAllRetiredAttachments(store, cloudObjects, size);
                // //Console.WriteLine("SLEEEEEEEEEEEEEEEEP");
                //Thread.Sleep(int.MaxValue);
                Assert.Equal(true, await WaitForChangeVectorInClusterAsync(srcNodes.Servers, srcDb));
            }
        }
    }
    protected async Task CanUploadRetiredAttachmentToCloudFromBackupAndGet(int attachmentsCount, int size)
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
            var ids = new List<(string Id, string Collection)>();
            List<string> collections = null;
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings, collections);
                await CreateDocs(store, docsCount, ids, collections);
                await PopulateDocsWithRandomAttachments(store, size, ids, attachmentsPerDoc);


                var config = Backup.CreateBackupConfiguration(backupPath);
                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                // restore the database with a different name
                var restoredDatabaseName = GetDatabaseName();

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(backupPath).First(),
                    DatabaseName = restoredDatabaseName
                }))
                {
                    var stats = await GetDatabaseStatisticsAsync(store, restoredDatabaseName);
                    Assert.Equal(docsCount, stats.CountOfDocuments); // the marker
                    var database2 = (await GetDocumentDatabaseInstanceForAsync(restoredDatabaseName));

                    GetStorageAttachmentsMetadataFromAllAttachments(database2);
                    Assert.Equal(attachmentsCount, Attachments.Count);
                    // move in time & start retire
                    database2.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database2.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                    var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
                    using (var restored = new DocumentStore { Urls = store.Urls, Database = restoredDatabaseName }.Initialize())
                    {
                        await AssertAllRetiredAttachments(restored, cloudObjects, size);
                    }
                }
            }
        }
    }

    protected async Task CanExportImportWithRetiredAttachmentInternal(int attachmentsCount, int size)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
            var ids = new List<(string Id, string Collection)>();
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, store1, docsCount, ids, attachmentsPerDoc);

                var exportFile = GetTempFileName();

                var exportOperation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile);
                await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                var operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);

                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                var destinationRecord = await store2.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store2.Database));
                Assert.False(destinationRecord.RetiredAttachments.Disabled);

                var stats = await GetDatabaseStatisticsAsync(store2, store2.Database);
                Assert.Equal(docsCount, stats.CountOfDocuments); // the marker
                Assert.Equal(attachmentsCount, stats.CountOfAttachments); // the marker

                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
                await AssertAllRetiredAttachments(store2, cloudObjects, size);
            }
        }
    }

    protected async Task CanIndexWithRetiredAttachmentInternal(int attachmentsCount, int size)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
            var ids = new List<(string Id, string Collection)>();
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings);
                await CreateDocs(store, docsCount, ids);
                await PopulateDocsWithRandomAttachments(store, size, ids, attachmentsPerDoc);

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                GetStorageAttachmentsMetadataFromAllAttachments(database);
                Assert.Equal(attachmentsCount, Attachments.Count);

                var index = new MultipleAttachmentsIndex();
                await index.ExecuteAsync(store);
                await Indexes.WaitForIndexingAsync(store);
                using (var session = store.OpenSession())
                {
                    var res = session.Advanced.RawQuery<Order>("from index 'MultipleAttachmentsIndex' as o where o.AttachmentFlags != null").WaitForNonStaleResults().ToList();
                    Assert.Equal(docsCount, res.Count);

                    var res2 = session.Advanced.RawQuery<Order>("from index 'MultipleAttachmentsIndex' as o where o.AttachmentFlags != 'Retired'").WaitForNonStaleResults().ToList();
                    Assert.Equal(docsCount, res2.Count);

                    var res3 = session.Advanced.RawQuery<Order>("from index 'MultipleAttachmentsIndex' as o where o.AttachmentFlags == 'None'").WaitForNonStaleResults().ToList();
                    Assert.Equal(docsCount, res3.Count);
                }

                // move in time & start retire
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);

                await AssertAllRetiredAttachments(store, cloudObjects, size);

                await Indexes.WaitForIndexingAsync(store);

                using (var session = store.OpenSession())
                {
                    var res = session.Advanced.RawQuery<Order>("from index 'MultipleAttachmentsIndex' as o where o.AttachmentRetiredAt != null").WaitForNonStaleResults().ToList();
                    Assert.Equal(docsCount, res.Count);
                }

                using (var session = store.OpenSession())
                {
                    var res = session.Advanced.RawQuery<Order>("from index 'MultipleAttachmentsIndex' as o where o.AttachmentFlags == 'Retired'").WaitForNonStaleResults().ToList();
                    Assert.Equal(docsCount, res.Count);

                    var res2 = session.Advanced.RawQuery<Order>("from index 'MultipleAttachmentsIndex' as o where o.AttachmentFlags != 'Retired'").WaitForNonStaleResults().ToList();
                    Assert.Equal(0, res2.Count);
                }
            }
        }
    }


    protected async Task CanEtlWithRetiredAttachmentAndRetireOnDestinationInternal(int attachmentsCount, int size)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
            var ids = new List<(string Id, string Collection)>();
            using (var store = GetDocumentStore())
            using (var replica = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings);
                await CreateDocs(store, docsCount, ids);
                await PopulateDocsWithRandomAttachments(store, size, ids, attachmentsPerDoc);

                //var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                //GetStorageAttachmentsMetadataFromAllAttachments(database);
                //Assert.Equal(attachmentsCount, Attachments.Count);

                var taskName = "etl-test";
                var csName = "cs-test";

                var configuration = new RavenEtlConfiguration
                {
                    ConnectionStringName = csName,
                    Name = taskName,
                    Transforms = { new Transformation { Name = "S1", Collections = { "Orders" } } }
                };

                var connectionString = new RavenConnectionString { Name = csName, TopologyDiscoveryUrls = replica.Urls, Database = replica.Database, };

                var etlDone = Etl.WaitForEtlToComplete(store);

                Etl.AddEtl(store, configuration, connectionString);

                etlDone.Wait(TimeSpan.FromSeconds(15));
                //Thread.Sleep(int.MaxValue);

                var database2 = (await GetDocumentDatabaseInstanceForAsync(replica.Database));
                GetStorageAttachmentsMetadataFromAllAttachments(database2);

                Assert.True(Attachments.TrueForAll(x => x.RetireAt != null), "Attachments.TrueForAll(x => x.RetireAt != null)");


                await PutRetireAttachmentsConfiguration(replica, Settings);

                Assert.Equal(attachmentsCount, Attachments.Count);

                // move in time & start retire
                database2.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database2.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
                await AssertAllRetiredAttachments(replica, cloudObjects, size);




                //var destinationRecord = await replica.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(replica.Database));
                //Assert.False(destinationRecord.RetireAttachments.Disabled);

                //var stats = await GetDatabaseStatisticsAsync(replica, replica.Database);
                //Assert.Equal(docsCount, stats.CountOfDocuments); // the marker
                //Assert.Equal(attachmentsCount, stats.CountOfAttachments); // the marker

                //var cloudObjects = await GetBlobsFromS3AndAssertForCount(Settings, attachmentsCount, 15_000);
                //await AssertAllRetiredAttachments(replica, cloudObjects, size);
            }
        }
    }

    protected class MultipleAttachmentsIndex : AbstractIndexCreationTask<Order>
    {
        public class Result
        {
            public string CompanyName { get; set; }
            public string AttachmentName { get; set; }
            public string AttachmentContentType { get; set; }
            public string AttachmentHash { get; set; }
            public long AttachmentSize { get; set; }
            public string AttachmentContent { get; set; }
            public AttachmentFlags AttachmentFlags { get; set; }
            public DateTime? AttachmentRetiredAt { get; set; }
            public Stream AttachmentStream { get; set; }
        }
        public MultipleAttachmentsIndex()
        {
            Map = orders => from o in orders
                let attachments = LoadAttachments(o)
                from attachment in attachments
                select new Result
                {
                    CompanyName = o.OrderedAt.ToString(),
                    AttachmentName = attachment.Name,
                    AttachmentContentType = attachment.ContentType,
                    AttachmentHash = attachment.Hash,
                    AttachmentSize = attachment.Size,
                    AttachmentContent = attachment.GetContentAsString(),
                    //AttachmentStream = attachment.GetContentAsStream(),
                    AttachmentFlags = attachment.Flags,
                    AttachmentRetiredAt = attachment.RetiredAt
                };
        }
    }
}
