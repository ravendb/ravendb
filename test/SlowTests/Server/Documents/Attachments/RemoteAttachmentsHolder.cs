using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Attachments.Remote;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.ServerWide.Context;
using Xunit;

namespace SlowTests.Server.Documents.Attachments;

public abstract class RemoteAttachmentsHolder<TSettings> : RemoteAttachmentsHolderBase
    where TSettings : IRemoteAttachmentsSettings
{
    public TSettings Settings;

    protected RemoteAttachmentsHolder(ITestOutputHelper output) : base(output)
    {
    }

    public abstract IAsyncDisposable CreateCloudSettings([CallerMemberName] string caller = null);
    protected abstract Task<List<FileInfoDetails>> GetBlobsFromCloudAndAssertForCount(TSettings settings, int expected, int timeout = 120_000);
    protected abstract Task OverwriteBlobInCloudWithDummyStream(TSettings settings, FileInfoDetails file);
    public abstract Task DeleteObjects(TSettings settings);
    public abstract Task<string> PutRemoteAttachmentsConfiguration(IDocumentStore store, TSettings settings, List<string> collections = null, string database = null, string id = null);
    public abstract TSettings GetCloudSetting(string remoteFolderName, [CallerMemberName] string caller = null);

    public Action<RemoteAttachmentsConfiguration> ModifyRemoteAttachmentsConfig = null;

    protected void AddAttachmentToAttachments(DocumentInfoHelper documentInfoHelper, Attachment attachment)
    {
        using (var docId = documentInfoHelper.GetDocumentId(attachment.Key))
        {
            var t = Attachments.FirstOrDefault(x =>
                x.DocumentId.ToLowerInvariant() == docId && x.Name == attachment.Name &&
                (x.RemoteParameters.IsLocalStorageAttachment()) &&
                x.Hash == attachment.Base64Hash.ToString());
            Assert.NotNull(t);
            Attachments.Remove(t);
            t.Key = attachment.Key;
            t.Hash = attachment.Base64Hash.ToString();
            t.RemoteParameters = new RemoteAttachmentParameters(attachment.RemoteParameters.Identifier, attachment.RemoteParameters.At);
            t.RemoteKey = $"{Settings.RemoteFolderName}/{t.Hash}";
            Attachments.Add(t);
        }
    }

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

    public async Task PopulateDocsWithRandomAttachments(DocumentStore store, string identifier, int size, List<(string Id, string Collection)> ids, int attachmentsPerDoc, int start = 0, bool remote = true)
    {
        // put attachments
        foreach (var (id, collection) in ids)
        {
            for (int i = 0; i < attachmentsPerDoc; i++)
            {
                // make sure we have unique hashes
                HashSet<string> seen = new HashSet<string>();
                byte[] b = new byte[size];
                string key;

                do
                {
                    RandomNumberGenerator.Fill(b);
                    key = Convert.ToBase64String(b);
                } while (!seen.Add(key)); // repeats if not unique

                var profileStream = new MemoryStream(b);
                var name = $"test_{i + start}.png";

                RemoteAttachmentParameters remoteParameters = null;
                if (remote)
                {
                    remoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3));
                }

                await store.Operations.SendAsync(new PutAttachmentOperation(id, new StoreAttachmentParameters(name, profileStream)
                {
                    RemoteParameters = remoteParameters,
                    ContentType = "image/png"
                }));
                profileStream.Position = 0;
                using AttachmentResult a = await store.Operations.SendAsync(new GetAttachmentOperation(id, name, AttachmentType.Document, null));

                Attachments.Add(new RemoteAttachment()
                {
                    Name = name,
                    DocumentId = id,
                    Stream = profileStream,
                    ContentType = "image/png",
                    Hash = a.Details.Hash,
                    RemoteParameters = remoteParameters
                });
            }
        }
    }

    // add attachments storage key, RemoteKey, and hash to holder
    public void GetStorageAttachmentsMetadataFromAllAttachments(DocumentDatabase database, IRemoteAttachmentsSettings settings = null)
    {
        settings ??= Settings;

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
                    t.RemoteParameters = attachment.RemoteParameters;
                    t.RemoteKey =
                        $"{settings.RemoteFolderName}/{t.Hash}";
                    Attachments.Add(t);
                }
            }

            Attachments.GroupBy(x => x.RemoteKey).ToList().ForEach(x =>
            {
                Assert.Single(x);
            });
        }
    }

    public async Task AssertAllRemoteAttachments(IDocumentStore store, List<FileInfoDetails> cloudObjects, int size, string identifier)
    {
        foreach (var attachment in Attachments)
        {
            Assert.Contains(cloudObjects, x => x.FullPath.Contains(attachment.RemoteKey));

            attachment.Stream.Position = 0;
            await GetAndCompareRemoteAttachment(store, attachment.DocumentId, attachment.Name, attachment.Hash, attachment.ContentType, attachment.Stream, size, identifier);
        }
    }

    protected async Task CanUploadRemoteAttachmentToCloudAndGetInternal(int attachmentsCount, int size, List<string> collections = null)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
            var ids = new List<(string Id, string Collection)>();

            using (var store = GetDocumentStore())
            {
                var identifier = await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc, collections);
            }
        }
    }

    protected async Task<string> CanUploadRemoteAttachmentToCloudAndGetInternal(int attachmentsCount, int size, DocumentStore store, int docsCount,
        List<(string Id, string Collection)> ids, int attachmentsPerDoc, List<string> collections = null, RavenServer server = null)
    {
        var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections);
        await CreateDocs(store, docsCount, ids, collections);
        await PopulateDocsWithRandomAttachments(store, identifier,size, ids, attachmentsPerDoc);

        var database = await Databases.GetDocumentDatabaseInstanceFor(server ?? Server, store);
        GetStorageAttachmentsMetadataFromAllAttachments(database);
        Assert.Equal(attachmentsCount, Attachments.Count);

        // move in time & start remote
        database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
        await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

        var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
        await AssertAllRemoteAttachments(store, cloudObjects, size, identifier);

        var stats = store.Maintenance.Send(new GetDetailedStatisticsOperation());
        Assert.Equal(attachmentsCount, stats.CountOfRemoteAttachments);

        return identifier;
    }

    protected async Task CanUploadRemoteAttachmentToCloudAndDeleteInternal(int attachmentsCount, int size, List<string> collections = null)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = RemoteAttachmentsHolderBase.GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
            var ids = new List<(string Id, string Collection)>();
            using (var store = GetDocumentStore())
            {
                var identifier = await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc, collections);
                foreach (var attachment in Attachments)
                {
                    await store.Operations.SendAsync(new DeleteAttachmentOperation(attachment.DocumentId, attachment.Name));
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var c = database.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).Count();
                    Assert.Equal(0, c);
                }
            }
        }
    }

    protected async Task CanUploadRemoteAttachmentsToCloudAndGetInBulkInternal(int attachmentsCount, int size, int attachmentsPerDoc, List<string> collections = null)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = attachmentsCount / attachmentsPerDoc;
            var ids = new List<(string Id, string Collection)>();

            using (var store = GetDocumentStore())
            {
                var identifier = await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc, collections);
                await AssertGetRemoteAttachmentsInBulk(store, size, identifier);
            }
        }
    }
    protected async Task AssertGetRemoteAttachmentsInBulk(DocumentStore store, long size, string identifier, RemoteAttachmentFlags flags = RemoteAttachmentFlags.Remote)
    {
        var attachmentRequests = new List<AttachmentRequest>();
        foreach (var attachment in Attachments)
        {
            attachmentRequests.Add(new AttachmentRequest(attachment.DocumentId, attachment.Name));
        }
        var attachmentsEnumerator = await store.Operations.SendAsync(new GetAttachmentsOperation(attachmentRequests, AttachmentType.Document));

        var attachmentsCount = 0;
        while (attachmentsEnumerator.MoveNext())
        {
            AttachmentEnumeratorResult current = attachmentsEnumerator.Current;
            Assert.NotNull(current);

            var tuple = Attachments.FirstOrDefault(x => x.DocumentId == current.Details.DocumentId && x.Name == current.Details.Name);
            Assert.NotNull(tuple);
            tuple.Stream.Position = 0;


            await CompareAttachment(tuple.Name, tuple.Hash, tuple.ContentType, tuple.Stream, size, identifier, flags, current.Details, current.Stream);
            await current.Stream.DisposeAsync();

            attachmentsCount++;
        }

        Assert.Equal(attachmentRequests.Count, attachmentsCount);
    }

    protected async Task CanUploadRemoteAttachmentsToCloudAndDeleteInBulkInternal(int attachmentsCount, int size, int attachmentsPerDoc, List<string> collections = null)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = attachmentsCount / attachmentsPerDoc;
            var ids = new List<(string Id, string Collection)>();

            using (var store = GetDocumentStore())
            {
                var identifier = await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc, collections);
                await AssertDeleteRemoteAttachmentsInBulk(store, attachmentsCount);
            }
        }
    }

    protected async Task AssertDeleteRemoteAttachmentsInBulk(DocumentStore store, int attachmentsCount)
    {
        var attachmentRequests = new List<AttachmentRequest>();
        foreach (var attachment in Attachments)
        {
            attachmentRequests.Add(new AttachmentRequest(attachment.DocumentId, attachment.Name));
        }
        await store.Operations.SendAsync(new DeleteAttachmentsOperation(attachmentRequests));

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
        await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var count = database.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).Count();
            Assert.Equal(0, count);
        }
    }

    protected async Task CanEtlRemoteAttachmentsToDestinationInternal(int attachmentsCount, int size)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = RemoteAttachmentsHolderBase.GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
            var ids = new List<(string Id, string Collection)>();
            using (var store = GetDocumentStore())
            using (var replica = GetDocumentStore())
            {
                var identifier = await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc);
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
                await etlDone.WaitAsync(TimeSpan.FromSeconds(15));

                var replicaDb = await Databases.GetDocumentDatabaseInstanceFor(replica);
                var val3 = WaitForValue(() =>
                {
                    using (replicaDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var c = replicaDb.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).Count();

                        return c;
                    }
                }, attachmentsCount, 30_000);
                Assert.Equal(attachmentsCount, val3);

                foreach (var remote in Attachments)
                {
                    var e = await Assert.ThrowsAsync<RavenException>(async () =>
                        await replica.Operations.SendAsync(new GetAttachmentOperation(remote.DocumentId, remote.Name, AttachmentType.Document, null)));
                    Assert.Contains($"Cannot perform 'GetAttachmentOperation' for remote attachment '{remote.Name}' on document '{remote.DocumentId}' because the database does not have a RemoteAttachmentsConfiguration configured.", e.Message);
                }

                var identifier2 = await PutRemoteAttachmentsConfiguration(replica, Settings);
                await AssertGetRemoteAttachmentsInBulk(replica, size, identifier2, RemoteAttachmentFlags.Remote);
            }
        }
    }

    protected async Task CanBackupRemoteAttachmentsInternal(int attachmentsCount, int size)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
            var ids = new List<(string Id, string Collection)>();
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore())
            {
                var identifier = await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc);

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
                    await AssertAllRemoteAttachments(store2, cloudObjects, size, identifier);
                }
            }

        }
    }

    protected async Task CanExternalReplicateRemoteAttachmentAndThenUploadToCloudAndGet(int attachmentsCount, int size)
    {
        using (var store1 = GetDocumentStore())
        using (var store2 = GetDocumentStore())
        {
            await using (var holder = CreateCloudSettings())
            {
                int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                var ids = new List<(string Id, string Collection)>();
                var identifier1 = await PutRemoteAttachmentsConfiguration(store1, Settings);
                await CreateDocs(store1, docsCount, ids);
                await PopulateDocsWithRandomAttachments(store1, identifier1, size, ids, attachmentsPerDoc);

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                var database2 = (await GetDocumentDatabaseInstanceForAsync(store2.Database));
                GetStorageAttachmentsMetadataFromAllAttachments(database2);
                var identifier2 = await PutRemoteAttachmentsConfiguration(store2, Settings);
                Assert.Equal(identifier1, identifier2);
                Assert.Equal(attachmentsCount, Attachments.Count);
                // move in time & start remote
                database2.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database2.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
                await AssertAllRemoteAttachments(store2, cloudObjects, size, identifier2);
            }
        }
    }

    protected async Task CanExternalReplicateRemoteAttachmentAndThenUploadToCloudAndGet2(int attachmentsCount, int size)
    {
        using (var store1 = GetDocumentStore())
        using (var store2 = GetDocumentStore())
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
            var ids = new List<(string Id, string Collection)>();
            var identifier1 = await PutRemoteAttachmentsConfiguration(store1, Settings);
            await CreateDocs(store1, docsCount, ids);
            await PopulateDocsWithRandomAttachments(store1, identifier1, size, ids, attachmentsPerDoc);

            await SetupReplicationAsync(store1, store2);
            await EnsureReplicatingAsync(store1, store2);

            var database2 = (await GetDocumentDatabaseInstanceForAsync(store2.Database));
            GetStorageAttachmentsMetadataFromAllAttachments(database2);

            Assert.Null(database2.RemoteAttachmentsSender);
            var settings = GetCloudSetting("RemoteAttachments", $"{store2.Database}-{Guid.NewGuid()}");

            // create different config with other identifier
            GetStorageAttachmentsMetadataFromAllAttachments(database2, settings);
            var identifier2 = await PutRemoteAttachmentsConfiguration(store2, settings, id: "my-cool-identifier322");
            Assert.Equal("my-cool-identifier322", identifier2);
            Assert.NotEqual(identifier1, identifier2);

            Assert.Equal(attachmentsCount, Attachments.Count);

            Assert.True(await WaitForValueAsync(() => database2.RemoteAttachmentsSender != null, true));
            // move in time & start remote

            var remoteAt = Attachments.First().RemoteParameters.At;

            // this will delete the remote entry from remote tree because the configuration identifier doesn't exist
            database2.Time.UtcDateTime = () => remoteAt.AddMinutes(1);
            var processed = await database2.RemoteAttachmentsSender!.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
            // the item is with Deleted status and got removed from the remote tree, but no upload happened because of different identifier
            Assert.Equal(attachmentsCount, processed);
            await GetBlobsFromCloudAndAssertForCount(Settings, 0, 15_000);

            var config1 = await store2.Maintenance.SendAsync(new GetRemoteAttachmentsConfigurationOperation());
            var config2 = await store1.Maintenance.SendAsync(new GetRemoteAttachmentsConfigurationOperation());
            config1.Destinations[identifier1] = config2.Destinations[identifier1];
            Assert.Equal(2, config1.Destinations.Count);
            await store2.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(config1));

            processed = await database2.RemoteAttachmentsSender!.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
            // even after adding appropriate identifier, the item was removed from remote tree, so no upload will happen
            Assert.Equal(0, processed);

            // we "touch" the attachments inorder to re-add them to remote tree
            var operation = await store2
                .Operations
                .SendAsync(new PatchByQueryOperation(new IndexQuery
                {
                    Query =
                        $$"""
                          from @all_docs 
                          update { 
                              var attachmentsList = this["@metadata"]["@attachments"];
                              for (let i = 0; i < attachmentsList.length; i++){
                                      attachments(this, attachmentsList[i].Name).remote(attachmentsList[i].RemoteParameters.Identifier, attachmentsList[i].RemoteParameters.At);
                           
                              }
                          }
                          """,
                    QueryParameters = new Raven.Client.Parameters
                    {
                        { "storageIdentifier", identifier1 }
                    }
                }));
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(10));

            processed = await database2.RemoteAttachmentsSender!.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
            // even after adding appropriate identifier, the item was removed from remote tree, so no upload will happen
            Assert.Equal(attachmentsCount, processed);

            GetStorageAttachmentsMetadataFromAllAttachments(database2);

            var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
            await AssertAllRemoteAttachments(store2, cloudObjects, size, identifier1);
        }
    }

    protected async Task CanUploadRemoteAttachmentToCloudAndDeleteInTheSameTimeInternal(int attachmentsCount, int size, int attachmentsPerDoc)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = attachmentsCount / attachmentsPerDoc;
            var ids = new List<(string Id, string Collection)>();
            using (var store = GetDocumentStore())
            {
                List<string> collections = null;
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections);
                await RemoteAttachmentsHolderBase.CreateDocs(store, docsCount, ids, collections);

                await PopulateDocsWithRandomAttachments(store, identifier,size, ids, attachmentsPerDoc / 2, start: 0);

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                GetStorageAttachmentsMetadataFromAllAttachments(database);

                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(5);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount / 2);

                var list = Attachments.ToList();
                var t1 = Task.Run(async () =>
                {
                    await PopulateDocsWithRandomAttachments(store, identifier,size, ids, attachmentsPerDoc / 2, start: 1000);
                });
                var t2 = Task.Run(async () =>
                {
                    foreach (var attachment in list)
                    {
                        await store.Operations.SendAsync(new DeleteAttachmentOperation(attachment.DocumentId, attachment.Name));
                    }
                });

                await Task.WhenAll(t1, t2);

                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                GetStorageAttachmentsMetadataFromAllAttachments(database);
                var list2 = Attachments.ToList();
                list2.RemoveAll(x => list.Contains(x));
                foreach (var attachment in list2)
                {
                    await store.Operations.SendAsync(new DeleteAttachmentOperation(attachment.DocumentId, attachment.Name));
                }

                await WaitAndAssertForValueAsync(async () =>
                {
                    await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                    var objs = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 1000); // we don't delete the attachments from cloud
                    return objs.Count;
                }, attachmentsCount);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var count = database.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).Count();
                    Assert.Equal(0, count);
                }
            }
        }
    }

    protected async Task ShouldAddRemoteAtToAttachmentMetadataInternal()
    {
        await using (var holder = CreateCloudSettings())
        {
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);

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
                await store.Operations.SendAsync(new PutAttachmentOperation(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" }));

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var totalCount = 0;
                    var dbRecord = database.ReadDatabaseRecord();
                    var toRemote = database.DocumentsStorage.AttachmentsStorage.RemoteAttachmentsStorage.GetDocuments(new BackgroundWorkParameters(ctx, DateTime.MaxValue, dbRecord.Topology, "A", int.MaxValue), ref totalCount, out var _, default);
                    Assert.Equal(1, toRemote.Count);
                }

                var attachment = await store.Operations.SendAsync(new GetAttachmentOperation(id, "test.png", AttachmentType.Document, null));
                Assert.NotNull(attachment.Details.RemoteParameters);
            }
        }
    }

    protected async Task ShouldNotThrowUsingRegularAttachmentsApiOnRemoteAttachmentInternal()
    {
        await using (var holder = CreateCloudSettings())
        {
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);

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
                await store.Operations.SendAsync(new PutAttachmentOperation(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" }));

                var a = await store.Operations.SendAsync(new GetAttachmentOperation(id, "test.png", AttachmentType.Document, null));
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var expiredDocumentsCleaner = database.RemoteAttachmentsSender;
                await expiredDocumentsCleaner.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, 1);
                Assert.Contains($"{Settings.RemoteFolderName}/{a.Details.Hash}", cloudObjects[0].FullPath);

                await store.Operations.SendAsync(new GetAttachmentOperation(id, "test.png", AttachmentType.Document, null));
                await store.Operations.SendAsync(new GetAttachmentsOperation(new List<AttachmentRequest> { new(id, "test.png") }, AttachmentType.Document));
                await store.Operations.SendAsync(new DeleteAttachmentOperation(id, "test.png"));
            }
        }
    }

    protected async Task CanUploadRemoteAttachmentToCloudInClusterAndDeleteInternal(int attachmentsCount, int size)
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
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);
                await CreateDocs(store, docsCount, ids);
                await PopulateDocsWithRandomAttachments(store, identifier,size, ids, attachmentsPerDoc);
                Assert.Equal(true, await WaitForChangeVectorInClusterAsync(srcNodes.Servers, srcDb));

                int count = 0;
                DocumentDatabase database = null;
                var remote = await WaitForValueAsync(async () =>
                {
                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)).ConfigureAwait(false);
                    var f = record.Topology.AllNodes.FirstOrDefault();
                    var srv = srcRaft.Nodes.FirstOrDefault(x => x.ServerStore.NodeTag == f);
                    database = await Databases.GetDocumentDatabaseInstanceFor(srv, store);

                    GetStorageAttachmentsMetadataFromAllAttachments(database);
                    Assert.Equal(attachmentsCount, Attachments.Count);

                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    count += await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                    return count;
                }, attachmentsCount, interval: 1000);


                Assert.Equal(attachmentsCount, remote);

                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);

                await AssertAllRemoteAttachments(store, cloudObjects, size, identifier);
                foreach (var attachment in Attachments)
                {
                    await store.Operations.SendAsync(new DeleteAttachmentOperation(attachment.DocumentId, attachment.Name));
                }

                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount); // we never delete remote attachments from cloud, so we should have 1 attachment in cloud
                Assert.True(await WaitForChangeVectorInClusterAsync(srcNodes.Servers, srcDb), $"Change vector didn't reach all nodes in cluster for {srcDb}");
            }
        }
    }

    protected async Task CanUploadRemoteAttachmentToCloudIfItAlreadyExists_ShouldNotOverwriteInternal(bool overwriteWithDummy)
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
                var identifier1 = await PutRemoteAttachmentsConfiguration(store, Settings);
                var identifier2 = await PutRemoteAttachmentsConfiguration(store2, Settings);

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
                await store.Operations.SendAsync(new PutAttachmentOperation(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier1, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" }));
                profileStream.Position = 0;
                await store2.Operations.SendAsync(new PutAttachmentOperation(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier2, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" }));

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var expiredDocumentsCleaner = database.RemoteAttachmentsSender;
                await expiredDocumentsCleaner.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                List<FileInfoDetails> cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, 1);
                Assert.Contains($"{Settings.RemoteFolderName}/EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", cloudObjects[0].FullPath);

                await GetAndCompareRemoteAttachment(store, id, "test.png", "EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", "image/png", profileStream, 3, identifier1);
                await WaitForTaskDelayIfNeeded();

                if (overwriteWithDummy)
                {
                    var cloudObject = cloudObjects[0];
                    await OverwriteBlobInCloudWithDummyStream(Settings, cloudObject);
                    cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, 1);
                    using (var stream = new MemoryStream(Encoding.UTF8.GetBytes("EGOR")))
                    {
                        // we should have dummy stream in cloud now but the size is same since its saved in attachment metadata
                        await GetAndCompareRemoteAttachment(store, id, "test.png", "EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", "image/png", stream, 3, identifier1);
                        await WaitForTaskDelayIfNeeded();
                    }
                }

                var database2 = await Databases.GetDocumentDatabaseInstanceFor(server, store2);
                database2.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var expiredDocumentsCleaner2 = database2.RemoteAttachmentsSender;
                await expiredDocumentsCleaner2.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                List<FileInfoDetails> cloudObjects2 = await GetBlobsFromCloudAndAssertForCount(Settings, 1);
                Assert.Contains($"{Settings.RemoteFolderName}/EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", cloudObjects2[0].FullPath);

                if (overwriteWithDummy)
                {
                    Assert.NotEqual(cloudObjects[0].LastModified, cloudObjects2[0].LastModified);
                }
                else
                {
                    // should be the same attachment, not a new one
                    Assert.Equal(cloudObjects[0].LastModified, cloudObjects2[0].LastModified);
                }

                await GetAndCompareRemoteAttachment(store, id, "test.png", "EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", "image/png", profileStream, 3, identifier1);
                await GetAndCompareRemoteAttachment(store2, id, "test.png", "EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", "image/png", profileStream, 3, identifier2);
            }

        }
    }

    protected virtual Task WaitForTaskDelayIfNeeded()
    {
        return Task.CompletedTask;
        // noop
    }

    protected async Task UploadRemoteAttachmentToCloudThenManuallyDeleteAndGetShouldThrowInternal()
    {
        await using (var holder = CreateCloudSettings())
        {
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);

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
                await store.Operations.SendAsync(new PutAttachmentOperation(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" }));

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var expiredDocumentsCleaner = database.RemoteAttachmentsSender;
                await expiredDocumentsCleaner.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, 1);
                Assert.Contains($"{Settings.RemoteFolderName}/EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", cloudObjects[0].FullPath);

                await DeleteObjects(Settings);

                var e = await Assert.ThrowsAsync<RavenException>(async () => await RemoteAttachmentsHolderBase.GetAndCompareRemoteAttachment(store, id, "test.png", "EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", "image/png", profileStream, 3, identifier));
                AssertUploadRemoteAttachmentToCloudThenManuallyDeleteAndGetShouldThrowInternal(e);
            }
        }
    }

    protected abstract void AssertUploadRemoteAttachmentToCloudThenManuallyDeleteAndGetShouldThrowInternal(RavenException e);

    protected async Task CanDeleteRemoteAttachmentFromCloudWhenItsNotExistsInCloudInternal()
    {
        await using (var holder = CreateCloudSettings())
        {
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);

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
                await store.Operations.SendAsync(new PutAttachmentOperation(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" }));

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var expiredDocumentsCleaner = database.RemoteAttachmentsSender;
                await expiredDocumentsCleaner.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, 1);
                Assert.Contains($"{Settings.RemoteFolderName}/EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", cloudObjects[0].FullPath);

                await DeleteObjects(Settings);
                await GetBlobsFromCloudAndAssertForCount(Settings, 0);

                await store.Operations.SendAsync(new DeleteAttachmentOperation(id, "test.png"));

                await expiredDocumentsCleaner.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                await GetBlobsFromCloudAndAssertForCount(Settings, 0);
                var remote = await store.Operations.SendAsync(new GetAttachmentOperation(id, "test.png", AttachmentType.Document, null));
                Assert.Null(remote);
            }
        }
    }

    protected async Task CanUploadRemoteAttachmentToCloudInClusterAndGetInternal(int attachmentsCount, int size)
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
                int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                var ids = new List<(string Id, string Collection)>();

                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);
                await CreateDocs(store, docsCount, ids);
                await PopulateDocsWithRandomAttachments(store, identifier,size, ids, attachmentsPerDoc);
                Assert.Equal(true, await WaitForChangeVectorInClusterAsync(srcNodes.Servers, srcDb));


                foreach (var node in srcRaft.Nodes)
                {
                    var database = await Databases.GetDocumentDatabaseInstanceFor(node, store);
                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var totalCount = 0;

                        var toRemote = database.DocumentsStorage.AttachmentsStorage.RemoteAttachmentsStorage.GetDocuments(new BackgroundWorkParameters(ctx, DateTime.MaxValue, new DatabaseTopology()
                            {
                                Members = [node.ServerStore.NodeTag]
                            }
                       , database.ServerStore.NodeTag, int.MaxValue), ref totalCount, out var _, default);
                        Assert.Equal(attachmentsCount, toRemote.Count);
                    }
                }

                int count = 0;
                var remote = await WaitForValueAsync(async () =>
                {
                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)).ConfigureAwait(false);
                    var f = record.Topology.AllNodes.FirstOrDefault();
                    var srv = srcRaft.Nodes.FirstOrDefault(x => x.ServerStore.NodeTag == f);
                    var database = await Databases.GetDocumentDatabaseInstanceFor(srv, store);

                    GetStorageAttachmentsMetadataFromAllAttachments(database);
                    Assert.Equal(attachmentsCount, Attachments.Count);

                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);

                    count += await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                    return count;
                }, attachmentsCount, interval: 1000);



                Assert.Equal(attachmentsCount, remote);



                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);

                await AssertAllRemoteAttachments(store, cloudObjects, size, identifier);

                Assert.Equal(true, await WaitForChangeVectorInClusterAsync(srcNodes.Servers, srcDb));
            }
        }
    }

    protected async Task CanUploadRemoteAttachmentToCloudInClusterAndGet2Internal(int attachmentsCount, int size)
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
                int docsCount = RemoteAttachmentsHolderBase.GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                var ids = new List<(string Id, string Collection)>();

                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);
                await RemoteAttachmentsHolderBase.CreateDocs(store, docsCount, ids);
                await PopulateDocsWithRandomAttachments(store, identifier,size, ids, attachmentsPerDoc);
                Assert.Equal(true, await WaitForChangeVectorInClusterAsync(srcNodes.Servers, srcDb));

                int count = 0;
                DatabaseOutgoingReplicationHandler halt = null;

                var remote = await WaitForValueAsync(async () =>
                {
                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)).ConfigureAwait(false);
                    var f = record.Topology.AllNodes.FirstOrDefault();
                    var srv = srcRaft.Nodes.FirstOrDefault(x => x.ServerStore.NodeTag == f);
                    var database = await Databases.GetDocumentDatabaseInstanceFor(srv, store);

                    GetStorageAttachmentsMetadataFromAllAttachments(database);
                    Assert.Equal(attachmentsCount, Attachments.Count);

                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);

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


                    count += await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                    return count;
                }, attachmentsCount, interval: 1000);


                Assert.NotNull(halt);
                Assert.Equal(attachmentsCount, remote);

                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);

                await AssertAllRemoteAttachments(store, cloudObjects, size, identifier);
                Assert.Equal(true, await WaitForChangeVectorInClusterAsync(srcNodes.Servers, srcDb));
            }
        }
    }
    protected async Task CanUploadRemoteAttachmentToCloudFromBackupAndGet(int attachmentsCount, int size)
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
            var ids = new List<(string Id, string Collection)>();
            List<string> collections = null;
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections);
                await CreateDocs(store, docsCount, ids, collections);
                await PopulateDocsWithRandomAttachments(store, identifier,size, ids, attachmentsPerDoc);


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

                    // Wait for RemoteAttachmentsSender to be initialized
                    Assert.True(await WaitForValueAsync(() => database2.RemoteAttachmentsSender != null, true, timeout: 15_000),
                        "RemoteAttachmentsSender was not initialized after restore");

                    // move in time & start remote
                    database2.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database2.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                    var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
                    using (var restored = new DocumentStore { Urls = store.Urls, Database = restoredDatabaseName }.Initialize())
                    {
                        await AssertAllRemoteAttachments(restored, cloudObjects, size, identifier);
                    }
                }
            }
        }
    }

    protected async Task CanExportImportWithRemoteAttachmentInternal(int attachmentsCount, int size)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
            var ids = new List<(string Id, string Collection)>();
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var identifier = await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, store1, docsCount, ids, attachmentsPerDoc);

                var exportFile = GetTempFileName();

                var exportOperation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile);
                await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                var operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);

                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                var destinationRecord = await store2.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store2.Database));
                Assert.False(destinationRecord.RemoteAttachments.Destinations.First().Value.Disabled);

                var stats = await GetDatabaseStatisticsAsync(store2, store2.Database);
                Assert.Equal(docsCount, stats.CountOfDocuments); // the marker
                Assert.Equal(attachmentsCount, stats.CountOfAttachments); // the marker

                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
                await AssertAllRemoteAttachments(store2, cloudObjects, size, identifier);
            }
        }
    }

    protected async Task CanIndexWithRemoteAttachmentInternal(int attachmentsCount, int size)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
            var ids = new List<(string Id, string Collection)>();
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);
                await CreateDocs(store, docsCount, ids);
                await PopulateDocsWithRandomAttachments(store, identifier,size, ids, attachmentsPerDoc);


                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                GetStorageAttachmentsMetadataFromAllAttachments(database);
                Assert.Equal(attachmentsCount, Attachments.Count);

                var index = new MultipleAttachmentsIndex();
                await index.ExecuteAsync(store);
                WaitForUserToContinueTheTest(store);

                await Indexes.WaitForIndexingAsync(store);
                using (var session = store.OpenSession())
                {
                    var res = session.Advanced.RawQuery<Order>("from index 'MultipleAttachmentsIndex' as o where o.AttachmentFlags != null").WaitForNonStaleResults().ToList();
                    Assert.Equal(docsCount, res.Count);

                    var res2 = session.Advanced.RawQuery<Order>("from index 'MultipleAttachmentsIndex' as o where o.AttachmentFlags != 'Remote'").WaitForNonStaleResults().ToList();
                    Assert.Equal(docsCount, res2.Count);

                    var res3 = session.Advanced.RawQuery<Order>("from index 'MultipleAttachmentsIndex' as o where o.AttachmentFlags == 'None'").WaitForNonStaleResults().ToList();
                    Assert.Equal(docsCount, res3.Count);
                }

                // move in time & start remote
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);

                await AssertAllRemoteAttachments(store, cloudObjects, size, identifier);

                await Indexes.WaitForIndexingAsync(store);

                using (var session = store.OpenSession())
                {
                    var res = session.Advanced.RawQuery<Order>("from index 'MultipleAttachmentsIndex' as o where o.AttachmentRemoteAt != null").WaitForNonStaleResults().ToList();
                    Assert.Equal(docsCount, res.Count);
                }

                using (var session = store.OpenSession())
                {
                    var res = session.Advanced.RawQuery<Order>("from index 'MultipleAttachmentsIndex' as o where o.AttachmentFlags == 'Remote'").WaitForNonStaleResults().ToList();
                    Assert.Equal(docsCount, res.Count);

                    var res2 = session.Advanced.RawQuery<Order>("from index 'MultipleAttachmentsIndex' as o where o.AttachmentFlags != 'Remote'").WaitForNonStaleResults().ToList();
                    Assert.Equal(0, res2.Count);
                }
            }
        }
    }


    protected async Task CanEtlWithRemoteAttachmentAndRemoteOnDestinationInternal(int attachmentsCount, int size)
    {
        await using (var holder = CreateCloudSettings())
        {
            int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
            var ids = new List<(string Id, string Collection)>();
            using (var store = GetDocumentStore())
            using (var replica = GetDocumentStore())
            {
                var identifier1 = await PutRemoteAttachmentsConfiguration(store, Settings);
                await CreateDocs(store, docsCount, ids);
                await PopulateDocsWithRandomAttachments(store, identifier1,size, ids, attachmentsPerDoc);

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

                await etlDone.WaitAsync(TimeSpan.FromSeconds(15));

                var database2 = (await GetDocumentDatabaseInstanceForAsync(replica.Database));
                GetStorageAttachmentsMetadataFromAllAttachments(database2);

                Assert.True(Attachments.TrueForAll(x => x.RemoteParameters != null), "Attachments.TrueForAll(x => x.RemoteParameters != null)");


                var identifier2 = await PutRemoteAttachmentsConfiguration(replica, Settings);

                Assert.Equal(attachmentsCount, Attachments.Count);

                // move in time & start remote
                database2.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database2.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
                await AssertAllRemoteAttachments(replica, cloudObjects, size, identifier2);
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
            public RemoteAttachmentFlags AttachmentFlags { get; set; }
            public DateTime? AttachmentRemoteAt { get; set; }
            public Stream AttachmentStream { get; set; }
            public string RemoteIdentifier { get; set; }
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
                    AttachmentFlags = attachment.RemoteFlags,
                    AttachmentRemoteAt = attachment.RemoteAt,
                    RemoteIdentifier = attachment.RemoteIdentifier
                };
        }
    }
}
