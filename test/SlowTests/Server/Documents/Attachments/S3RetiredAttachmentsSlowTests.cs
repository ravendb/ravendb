using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Client;
using Orders;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using SlowTests.Client.Attachments;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Attachments
{
    public class S3RetiredAttachmentsSlowTests : RetiredAttachmentsS3Base
    {
        //TODO: egor add retired attachment => delete config => replication should throw

        public S3RetiredAttachmentsSlowTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanPutAttachmentThenAddRetiredConfigAndNewAttachment(bool retireExistingAttachments)
        {
            using (var store = GetDocumentStore())
            {
                var id = "Orders/3";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Query.Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/2", Company = $"Companies/2" });

                    await session.SaveChangesAsync();
                }

                using var profileStream = new MemoryStream([1, 2, 3]);
                await store.Operations.SendAsync(new PutAttachmentOperation(id, new StoreAttachmentParameters("test.png", profileStream) { /*RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)),*/ ContentType = "image/png" }));

                var res = await store.Operations.SendAsync(new GetAttachmentOperation(id, "test.png", AttachmentType.Document, null));
                Assert.Equal("test.png", res.Details.Name);
                Assert.Null(res.Details.RetireParameters);
                await using (var holder = CreateCloudSettings())
                {
                    var identifier = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);

                    using var profileStream2 = new MemoryStream([3, 2, 1]);

                    await store.Operations.SendAsync(new PutAttachmentOperation(id, new StoreAttachmentParameters("test2.png", profileStream2) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" }));
                    var res2 = await store.Operations.SendAsync(new GetAttachmentOperation(id, "test2.png", AttachmentType.Document, null));
                    Assert.Equal("test2.png", res2.Details.Name);

                    Assert.Equal(RetiredAttachmentFlags.None, res2.Details.RetireParameters.Flags);
                    Assert.NotNull(res2.Details.RetireParameters.At);

                    var res3 = await store.Operations.SendAsync(new GetAttachmentOperation(id, "test.png", AttachmentType.Document, null));
                    Assert.Equal("test.png", res3.Details.Name);
                    Assert.Null(res3.Details.RetireParameters);

                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);

                    if (retireExistingAttachments)
                    {
                        profileStream.Position = 0;
                        await store.Operations.SendAsync(new PutAttachmentOperation(id, new StoreAttachmentParameters("test.png", profileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" }));
                        database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                        await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                        await GetBlobsFromCloudAndAssertForCount(Settings, 2, 15_000);

                        var res4 = await store.Operations.SendAsync(new GetAttachmentOperation(res3.Details.DocumentId, res3.Details.Name, AttachmentType.Document, null));
                        Assert.Equal("test.png", res4.Details.Name);
                        Assert.Equal(RetiredAttachmentFlags.Retired, res4.Details.RetireParameters.Flags);
                        Assert.NotNull(res4.Details.RetireParameters.At);
                    }
                    else
                    {
                        database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                        await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                        await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);

                        var res4 = await store.Operations.SendAsync(new GetAttachmentOperation(res3.Details.DocumentId, res3.Details.Name, AttachmentType.Document, null));
                        Assert.Equal("test.png", res4.Details.Name);
                        Assert.Null(res4.Details.RetireParameters);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanCrudAttachmentWhenHaveRetiredAttachment()
        {
            var attachmentsCount = 1;
            var size = 3;
            await using (var holder = CreateCloudSettings())
            {
                using (var store = GetDocumentStore())
                {
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();

                    var identifier = await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc, null);

                    var data = Attachments.FirstOrDefault();
                    Assert.NotNull(data);

                    using (var profileStream = new MemoryStream(new byte[] { 3, 2, 2 }))
                    {
                        // retire of this attachment should happen in baseline + 40 mins
                        var result = store.Operations.Send(new PutAttachmentOperation(data.DocumentId, new StoreAttachmentParameters("profile.png", profileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" }));
                        Assert.Equal("profile.png", result.Name);
                        Assert.Equal(data.DocumentId, result.DocumentId);
                        Assert.Equal("image/png", result.ContentType);
                        Assert.Equal("bucfDXJ3eWRJYpgggJrnskJtMuMyFohjO2GHATxTmUs=", result.Hash);
                        Assert.Equal(3, result.Size);
                    }

                    var names = new List<string>() { data.Name, "profile.png" }.OrderBy(x => x).ToList();
                    using (var session = store.OpenSession())
                    {
                        var doc = session.Load<Order>(data.DocumentId);
                        var metadata = session.Advanced.GetMetadataFor(doc);
                        Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                        var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                        Assert.Equal(2, attachments.Length);
                        foreach (var name in names)
                        {
                            var a = attachments.First(x => x.GetString(nameof(AttachmentName.Name)) == name);
                            Assert.NotNull(a);

                            if (name == data.Name)
                            {
                                Assert.Equal(3, a.GetLong(nameof(AttachmentName.Size)));
                                Assert.Equal(data.ContentType, a.GetString(nameof(AttachmentName.ContentType)));
                                Assert.Equal(data.Hash, a.GetString(nameof(AttachmentName.Hash)));
                            }
                            else
                            {
                                Assert.Equal(3, a.GetLong(nameof(AttachmentName.Size)));
                                Assert.Equal("image/png", a.GetString(nameof(AttachmentName.ContentType)));
                                Assert.Equal("bucfDXJ3eWRJYpgggJrnskJtMuMyFohjO2GHATxTmUs=", a.GetString(nameof(AttachmentName.Hash)));
                            }
                        }

                        // this would put a Delete retired attachment task in the queue, that should happen immediately
                        session.Advanced.Attachments.Delete(doc, data.Name);
                        session.SaveChanges();
                    }

                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    var key = string.Empty;
                    GetToRetireAttachmentsCount(database, 1, infos =>
                    {
                        if (infos == null)
                            return;

                        key = infos.First().LowerId.ToString();
                    });

                    await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);

                    Assert.Equal("\u0012conf-identifier-s3\0\u001eorders/0\u001ed\u001eprofile.png\u001ebucfDXJ3eWRJYpgggJrnskJtMuMyFohjO2GHATxTmUs=\u001eimage/png", key);

                    using (var session = store.OpenSession())
                    {
                        var doc = session.Load<Order>(data.DocumentId);
                        var metadata = session.Advanced.GetMetadataFor(doc);
                        Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                        var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                        Assert.Equal(1, attachments.Length);
                        var a = attachments.FirstOrDefault();
                        Assert.NotNull(a);
                        Assert.Equal(3, a.GetLong(nameof(AttachmentName.Size)));
                        Assert.Equal("image/png", a.GetString(nameof(AttachmentName.ContentType)));
                        Assert.Equal("bucfDXJ3eWRJYpgggJrnskJtMuMyFohjO2GHATxTmUs=", a.GetString(nameof(AttachmentName.Hash)));

                        session.Advanced.Attachments.Delete(doc, "profile.png");
                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession())
                    {
                        var doc = session.Load<Order>(data.DocumentId);
                        var metadata = session.Advanced.GetMetadataFor(doc);
                        Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.Flags));
                        Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.Attachments));
                    }

                    GetToRetireAttachmentsCount(await Databases.GetDocumentDatabaseInstanceFor(Server, store), 0);
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanPutInRetiredAttachmentAndDeleteTheDocBeforeRetirement()
        {
            await using (var holder = CreateCloudSettings())
            {
                using (var store = GetDocumentStore())
                {

                    //TODO: egor test with this config will make exceptions (in _threads.exception need to add test for that !
                    //await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                    //{
                    //    S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                    //    Disabled = false,
                    //    RetirePeriods = new Dictionary<string, TimeSpan>() { { "Orders", TimeSpan.FromMinutes(3) }, { "Products", TimeSpan.FromMilliseconds(322228) } },
                    //    RetireFrequencyInSec = 1000
                    //}));
                    var identifier = await PutRetireAttachmentsConfiguration(store, Settings);
                    var docId = "Orders/3";
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order { Id = docId, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/2", Company = $"Companies/2" });

                        await session.SaveChangesAsync();
                    }

                    using var profileStream = new MemoryStream([1, 2, 3]);
                    await store.Operations.SendAsync(
                        new PutAttachmentOperation(docId,
                            new StoreAttachmentParameters("test.png", profileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" }));
                    var res = await store.Operations.SendAsync(new GetAttachmentOperation(docId, "test.png", AttachmentType.Document, null));
                    Assert.Equal("test.png", res.Details.Name);

                    DocumentDatabase database = null;
         
                    var key = string.Empty;
                    database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    GetToRetireAttachmentsCount(database, 1, infos =>
                    {
                        if (infos == null)
                            return;

                        key = infos.First().LowerId.ToString();
                    });

                    Assert.Equal($"\u0012conf-identifier-s3\0\u001eorders/3\u001ed\u001etest.png\u001eEcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=\u001eimage/png", key);

                    PatchOperation operation = new PatchOperation(id: docId, changeVector: null, patch: new PatchRequest
                    {
                        Script = @$"
                                    del('{docId}');
                                 "
                    }, patchIfMissing: null);
                    await store.Operations.SendAsync(operation);

                    using (var s = store.OpenAsyncSession())
                    {
                        var q = await s.Query<Order>().ToListAsync();

                        Assert.Equal(0, q.Count);
                    }

                    GetToRetireAttachmentsCount(database, 0);
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanPutInRetiredAttachmentAndDeleteTheDocBeforeRetirementInCloud()
        {
            var srcDb = GetDatabaseName();
            var srcRaft = await CreateRaftCluster(3);
            var leader = srcRaft.Leader;
            var srcNodes = await CreateDatabaseInCluster(srcDb, 3, leader.WebUrl);
            var mentorNode = srcNodes.Servers.First(s => s != leader);
            using (DocumentStore store = (DocumentStore)new DocumentStore { Urls = srcNodes.Servers.Select(s => s.WebUrl).ToArray(), Database = srcDb, }.Initialize())
            {
                await using (var holder = CreateCloudSettings())
                {
                    //TODO: egor test with this config will make exceptions (in _threads.exception need to add test for that !
                    //await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                    //{
                    //    S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                    //    Disabled = false,
                    //    RetirePeriods = new Dictionary<string, TimeSpan>() { { "Orders", TimeSpan.FromMinutes(3) }, { "Products", TimeSpan.FromMilliseconds(322228) } },
                    //    RetireFrequencyInSec = 1000
                    //}));
                    var identifier = await PutRetireAttachmentsConfiguration(store, Settings);
                    var docId = "Orders/3";
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order { Id = docId, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/2", Company = $"Companies/2" });

                        await session.SaveChangesAsync();
                    }

                    using var profileStream = new MemoryStream([1, 2, 3]);
                    await store.Operations.SendAsync(

                    new PutAttachmentOperation(docId,
                        new StoreAttachmentParameters("test.png", profileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" }));
                    var res = await store.Operations.SendAsync(new GetAttachmentOperation(docId, "test.png", AttachmentType.Document, null));
                    Assert.Equal("test.png", res.Details.Name);
                    Assert.Equal(true, await WaitForChangeVectorInClusterAsync(srcNodes.Servers, srcDb));

                    DocumentDatabase database = null;
                    foreach (var node in srcRaft.Nodes)
                    {
                        database = await Databases.GetDocumentDatabaseInstanceFor(node, store);

                        var key = string.Empty;
                        GetToRetireAttachmentsCount(database, 1, infos =>
                        {
                            var arr = infos?.ToArray();
                            if (arr == null || arr.Length == 0)
                                return;

                            key = arr.First().LowerId.ToString();
                        });
                        Assert.Equal("\u0012conf-identifier-s3\0\u001eorders/3\u001ed\u001etest.png\u001eEcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=\u001eimage/png", key);
                    }

                    PatchOperation operation = new PatchOperation(id: docId, changeVector: null, patch: new PatchRequest
                    {
                        Script = @$"
                                    del('{docId}');
                                 "
                    }, patchIfMissing: null);
                    await store.Operations.SendAsync(operation);

                    Assert.Equal(true, await WaitForChangeVectorInClusterAsync(srcNodes.Servers, srcDb));
                    using (var s = store.OpenAsyncSession())
                    {
                        var q = await s.Query<Order>().ToListAsync();

                        Assert.Equal(0, q.Count);
                    }

                    foreach (var node in srcRaft.Nodes)
                    {
                        database = await Databases.GetDocumentDatabaseInstanceFor(node, store);
                        GetToRetireAttachmentsCount(database, 0);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task DeletingDocumentWithRetiredAttachmentShouldKeepRetiredAttachmentByDefault()
        {
            var attachmentsCount = 1;
            var size = 3;
            await using (var holder = CreateCloudSettings())
            {
                using (var store = GetDocumentStore())
                {
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();

                    var identifier = await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc, null);

                    foreach (var docId in Attachments.Select(x => x.DocumentId).ToList().Distinct().ToList())
                    {
                        PatchOperation operation = new PatchOperation(id: docId, changeVector: null, patch: new PatchRequest
                        {
                            Script = @$"
                                    del('{docId}');
                                 "
                        }, patchIfMissing: null);
                        var res = await store.Operations.SendAsync(operation);
                    }

                    using (var s = store.OpenAsyncSession())
                    {
                        var q = await s.Query<Order>().ToListAsync();

                        Assert.Equal(0, q.Count);
                    }

                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    GetToRetireAttachmentsCount(database, 0);
                    await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task DeletingDocumentWithRetiredAttachmentShouldKeepRetiredAttachmentByDefaultInCluster()
        {
            var attachmentsCount = 1;
            var size = 3;
            var srcDb = GetDatabaseName();
            var srcRaft = await CreateRaftCluster(3);
            var leader = srcRaft.Leader;
            var srcNodes = await CreateDatabaseInCluster(srcDb, 3, leader.WebUrl);
            var mentorNode = srcNodes.Servers.First(s => s != leader);
            var mentorTag = mentorNode.ServerStore.NodeTag;
            using (DocumentStore store = (DocumentStore)new DocumentStore { Urls = srcNodes.Servers.Select(s => s.WebUrl).ToArray(), Database = srcDb, }.Initialize())
            {
                await using (var holder = CreateCloudSettings())
                {
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();

                    var identifier = await PutRetireAttachmentsConfiguration(store, Settings);
                    await CreateDocs(store, docsCount, ids);
                    await PopulateDocsWithRandomAttachments(store, identifier,size, ids, attachmentsPerDoc);
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

                        count += await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                        return count;
                    }, attachmentsCount, interval: 1000);

                    Assert.Equal(attachmentsCount, retired);

                    var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
                    await AssertAllRetiredAttachments(store, cloudObjects, size, identifier);

                    var stores = srcNodes.Servers.Select(s => new DocumentStore { Urls = new string[1] { $"{s.WebUrl}" }, Database = srcDb, Conventions = new DocumentConventions { DisableTopologyUpdates = true } }.Initialize()).ToList();
                    try
                    {
                        var l = Attachments.Select(x => x.DocumentId).ToList().Distinct().ToList();
                        for (int i = 0; i < l.Count; i++)
                        {
                            var docId = l[i];
                  
                            PatchOperation operation = new PatchOperation(id: docId, changeVector: null, patch: new PatchRequest
                            {
                                Script = @$"
                                    del('{docId}');
                                 "
                            }, patchIfMissing: null);
                            var index = i % stores.Count;
                            var s = stores[++index];
                            var res = await store.Operations.SendAsync(operation);
                        }
                        Assert.Equal(true, await WaitForChangeVectorInClusterAsync(srcNodes.Servers, srcDb));
                        using (var s = store.OpenAsyncSession())
                        {
                            var q = await s.Query<Order>().ToListAsync();

                            Assert.Equal(0, q.Count);

                        }

                        foreach (var node in srcRaft.Nodes)
                        {
                            database = await Databases.GetDocumentDatabaseInstanceFor(node, store);
                            GetToRetireAttachmentsCount(database, 0);
                        }

                        await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);

                    }
                    finally
                    {
                        foreach (var s in stores)
                        {
                            s.Dispose();
                        }
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task DeletingAttachmentShouldRemoveFromRetireTree()
        {
            await using (var holder = CreateCloudSettings())
            {
                int docsCount = GetDocsAndAttachmentCount(1, out int attachmentsPerDoc);
                var ids = new List<(string Id, string Collection)>();
                List<string> collections = null;

                using (var store = GetDocumentStore())
                {
                    var identifier = await PutRetireAttachmentsConfiguration(store, Settings, collections);
                    await CreateDocs(store, docsCount, ids, collections);
                    await PopulateDocsWithRandomAttachments(store, identifier,3, ids, attachmentsPerDoc);

                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    GetStorageAttachmentsMetadataFromAllAttachments(database);
                    Assert.Equal(1, Attachments.Count);

                    GetToRetireAttachmentsCount(database, 1);
                    var attachment = Attachments[0];
                    await store.Operations.SendAsync(new DeleteAttachmentOperation(attachment.DocumentId, attachment.Name));
                    GetToRetireAttachmentsCount(database, 0);
                }
            }
        }

        public static void GetToRetireAttachmentsCount(DocumentDatabase database, int expected, Action<Queue<AbstractBackgroundWorkStorage.DocumentExpirationInfo>> action = null)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                DatabaseRecord dbRecord;
                string nodeTag;

                using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                using (serverContext.OpenReadTransaction())
                {
                    dbRecord = database.ServerStore.Cluster.ReadDatabase(serverContext, database.Name);
                    nodeTag = database.ServerStore.NodeTag;
                }

                var options = new BackgroundWorkParameters(context, DateTime.MaxValue, dbRecord, nodeTag, int.MaxValue);
                // need to sort the list so current checked node is first in topology, since only the "first topology node is checked in GetDocuments() method
                options.DatabaseRecord.Topology.Members = options.DatabaseRecord.Topology.Members.OrderByDescending(x => x == nodeTag).ToList();

                var totalCount = 0;

                using (database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.Initialize(context))
                {
                    var expired = database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.GetDocuments(options, ref totalCount, out _,
                        CancellationToken.None);

                    Assert.Equal(expected, totalCount);

                    if (expected == 0)
                    {
                        Assert.Null(expired);
                    }
                    else
                    {
                        Assert.Equal(expected, expired.Count);
                    }

                    action?.Invoke(expired);
                }
            }
        }

        [AmazonS3RetryTheory]
        [InlineData(1, new byte[] { 1, 2, 3, 4, 5 })]
    //    [InlineData(5, new byte[] { 1, 2, 3, 4, 5 })]
        public async Task CanRetireIdenticalAttachmentOnTwoDocuments_OnlyOneInCloud_AndGetFromBoth(int count, byte[] arr)
        {
            // Pseudocode:
            // 1. Create cloud settings and document store.
            // 2. Put retire attachments configuration.
            // 3. Create two documents.
            // 4. Add the same attachment (same content, name, content-type) to both documents.
            // 5. Retire both attachments (move time forward and trigger retirement).
            // 6. Assert only one blob in cloud.
            // 7. Assert we can get the retired attachment from both documents.

            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRetireAttachmentsConfiguration(store, Settings);

                var contentType = "image/png";
                var attachmentBytes = arr;
                var docIds = new List<string>();

                // Create two documents
                using (var stream1 = new MemoryStream(attachmentBytes))
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < count; i++)
                    {
                        var id = $"Orders/{i}";
                        docIds.Add(id);
                        await session.StoreAsync(new Order { Id = id });
                        await session.SaveChangesAsync();

                        await store.Operations.SendAsync(new PutAttachmentOperation(id, new StoreAttachmentParameters($"shared_{i}.png", stream1)
                        {
                            RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)),
                            ContentType = contentType
                        }));
                        stream1.Position = 0;
                    }

                }

                // Move time forward and retire attachments
                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                // Assert only one blob in cloud
                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);


                using (var ms1 = new MemoryStream())
                {
                    var retired1 = await store.Operations.SendAsync(new GetAttachmentOperation($"Orders/0", $"shared_0.png", AttachmentType.Document, null));
                    await retired1.Stream.CopyToAsync(ms1);
                    Assert.Equal(attachmentBytes, ms1.ToArray());
                    Assert.Equal($"shared_0.png", retired1.Details.Name);
                    Assert.Equal(contentType, retired1.Details.ContentType);
                    Assert.Equal(RetiredAttachmentFlags.Retired, retired1.Details.RetireParameters.Flags);
                    for (int i = 1; i < count; i++)
                    {
                        var retired2 = await store.Operations.SendAsync(new GetAttachmentOperation($"Orders/{i}", $"shared_{i}.png", AttachmentType.Document, null));

                        Assert.Equal($"shared_{i}.png", retired2.Details.Name);
                        Assert.Equal(contentType, retired2.Details.ContentType);
                        Assert.Equal(RetiredAttachmentFlags.Retired, retired2.Details.RetireParameters.Flags);

                        ms1.Position = 0;

                        // Compare content
                        using (var ms2 = new MemoryStream())
                        {

                            await retired2.Stream.CopyToAsync(ms2);
                            Assert.Equal(ms1.ToArray(), ms2.ToArray());
                            Assert.Equal(attachmentBytes, ms2.ToArray());
                        }
                    }

                }
            }
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        //TODO: egor in the future need optimizations so I can do a lot faster[InlineData(256)]
        public async Task CanUploadRetiredAttachmentToS3AndGet(int attachmentsCount, int size)
        {
            await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(64, 3)]
        //[InlineData(128, 3)]
        //TODO: egor in the future need optimizations so I can do a lot faster[InlineData(256)]
        public async Task CanUploadRetiredAttachmentFromDifferentCollectionsToS3AndGet(int attachmentsCount, int size)
        {
            var collections = new List<string> { "Orders", "Products" };
            Assert.True(attachmentsCount > 32, "this test meant to have more than 32 attachments so we will have more than one document");
            await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, collections: collections);
        }

        [AmazonS3RetryTheory]
        [InlineData(64, 3)]
        public async Task CanUploadRetiredAttachmentFromDifferentCollectionsToS3AndDelete(int attachmentsCount, int size)
        {
            Assert.True(attachmentsCount > 32, "this test meant to have more than 32 attachments so we will have more than one document");
            var collections = new List<string> { "Orders", "Products" };
            await CanUploadRetiredAttachmentToCloudAndDeleteInternal(attachmentsCount, size, collections);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRetiredAttachmentToS3AndDelete(int attachmentsCount, int size)
        {
            await CanUploadRetiredAttachmentToCloudAndDeleteInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(16, 3, 4)]
        public async Task CanUploadRetiredAttachmentToS3AndDeleteInTheSameTime(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            await CanUploadRetiredAttachmentToCloudAndDeleteInTheSameTimeInternal(attachmentsCount, size, attachmentsPerDoc);
        }

        [AmazonS3RetryFact]
        public async Task ShouldAddRetireAtToAttachmentMetadataUsingS3Configuration()
        {
            await ShouldAddRetireAtToAttachmentMetadataInternal();
        }

        [AmazonS3RetryFact]
        public async Task ShouldNotThrowUsingRegularAttachmentsApiOnRetiredAttachmentToS3()
        {
            await ShouldNotThrowUsingRegularAttachmentsApiOnRetiredAttachmentInternal();
        }

        [AmazonS3RetryTheory]
        [InlineData(3, 3, 1)]
      //  [InlineData(16, 3, 4)]
        public async Task CanUploadRetiredAttachmentsFromDifferentCollectionsToS3AndGetInBulk(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            var collections = new List<string> { "Orders", "Products" };
            await CanUploadRetiredAttachmentsToCloudAndGetInBulkInternal(attachmentsCount, size, attachmentsPerDoc, collections);
        }

        [AmazonS3RetryTheory]
        [InlineData(3, 3, 1)]
 //       [InlineData(16, 3, 4)]
        public async Task CanUploadRetiredAttachmentsToS3AndGetInBulk(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            await CanUploadRetiredAttachmentsToCloudAndGetInBulkInternal(attachmentsCount, size, attachmentsPerDoc);
        }
        [AmazonS3RetryTheory]
        [InlineData(3, 3, 1)]
     //   [InlineData(16, 3, 4)]
        public async Task CanUploadRetiredAttachmentsToS3AndDeleteInBulk(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            await CanUploadRetiredAttachmentsToCloudAndDeleteInBulkInternal(attachmentsCount, size, attachmentsPerDoc);
        }

        [AmazonS3RetryFact]
        public async Task CanUploadRetiredAttachmentToS3IfItAlreadyExists_ShouldNotOverwrite()
        {
            await CanUploadRetiredAttachmentToCloudIfItAlreadyExists_ShouldNotOverwriteInternal();
        }

        [AmazonS3RetryFact]
        public async Task UploadRetiredAttachmentToS3ThenManuallyDeleteAndGetShouldThrow()
        {
            await UploadRetiredAttachmentToCloudThenManuallyDeleteAndGetShouldThrowInternal();
        }

        [AmazonS3RetryFact]
        public async Task CanDeleteRetiredAttachmentFromS3WhenItsNotExistsInS3()
        {
            await CanDeleteRetiredAttachmentFromCloudWhenItsNotExistsInCloudInternal();
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRetiredAttachmentToS3InClusterAndGet(int attachmentsCount, int size)
        {
            await CanUploadRetiredAttachmentToCloudInClusterAndGetInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRetiredAttachmentToS3InClusterAndGet2(int attachmentsCount, int size)
        {
            await CanUploadRetiredAttachmentToCloudInClusterAndGet2Internal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRetiredAttachmentToS3InClusterAndDelete(int attachmentsCount, int size)
        {
            await CanUploadRetiredAttachmentToCloudInClusterAndDeleteInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRetiredAttachmentToS3FromBackupAndGet(int attachmentsCount, int size)
        {

            await CanUploadRetiredAttachmentToCloudFromBackupAndGet(attachmentsCount, size);
        }

        [AmazonS3RetryTheory(Skip = "TODO EGOR RavenDB-24604")]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanExternalReplicateRetiredAttachmentAndThenUploadToS3AndGet(int attachmentsCount, int size)
        {
            await CanExternalReplicateRetiredAttachmentAndThenUploadToCloudAndGet(attachmentsCount, size);
        }

        [AmazonS3RetryTheory(Skip = "TODO EGOR RavenDB-24604")]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task ExternalReplicationOfRetiredAttachmentToExternalDatabaseShouldUnwrap(int attachmentsCount, int size)
        {
            using (var store1 = GetDocumentStore())
            {
                await using (var holder = CreateCloudSettings())
                {
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();
                    var identifier = await PutRetireAttachmentsConfiguration(store1, Settings);
                    await CreateDocs(store1, docsCount, ids);
                    await PopulateDocsWithRandomAttachments(store1, identifier, size, ids, attachmentsPerDoc);

                    var database = (await GetDocumentDatabaseInstanceForAsync(store1.Database));

                    // move in time & start retire
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                    var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
                    await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);

                    GetStorageAttachmentsMetadataFromAllAttachments(database, Settings);
                    await AssertAllRetiredAttachments(store1, cloudObjects, size, identifier);

                    using var store2 = GetDocumentStore();
                    await SetupReplicationAsync(store1, store2);
                    await EnsureReplicatingAsync(store1, store2);

                    foreach (var retired in Attachments)
                    {
                        Assert.Contains(cloudObjects, x => x.FullPath.Contains(retired.RetiredKey));
                        retired.Stream.Position = 0;
                        var attachment = await store2.Operations.SendAsync(new GetAttachmentOperation(retired.DocumentId, retired.Name, AttachmentType.Document, null));
                        Assert.NotNull(attachment);
                        Assert.Equal(retired.Hash, attachment.Details.Hash);
                        Assert.Equal(retired.ContentType, attachment.Details.ContentType);
                        Assert.Equal(retired.Name, attachment.Details.Name);
                        Assert.Equal(size, attachment.Details.Size);
                        Assert.Equal(RetiredAttachmentFlags.None, attachment.Details.RetireParameters.Flags);
                        Assert.NotNull(attachment.Details.RetireParameters.At);
                        using var retiredStream = new MemoryStream();
                        await attachment.Stream.CopyToAsync(retiredStream);
                        retired.Stream.Position = 0;
                        retiredStream.Position = 0;
                        await AttachmentsStreamTests.CompareStreamsAsync(retired.Stream, retiredStream);
                    }
                }
            }
        }

        [AmazonS3RetryTheory(Skip = "TODO EGOR RavenDB-24604")]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task AddRetiredAttachmentThenExternalReplicateToDatabaseWithoutRetiredConfig_ShouldUnwrap(int attachmentsCount, int size)
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await using (var holder = CreateCloudSettings())
                {
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();
                    var identifier1 = await PutRetireAttachmentsConfiguration(store1, Settings);
                    await CreateDocs(store1, docsCount, ids);
                    await PopulateDocsWithRandomAttachments(store1, identifier1, size, ids, attachmentsPerDoc);

                    await SetupReplicationAsync(store1, store2);
                    await EnsureReplicatingAsync(store1, store2);

                    var database2 = (await GetDocumentDatabaseInstanceForAsync(store2.Database));

                    // I create new settings for destination database, so each db upload to different folder
                    var settings = Etl.GetS3Settings(nameof(RetiredAttachments), $"{store2.Database}-{Guid.NewGuid()}");
                    GetStorageAttachmentsMetadataFromAllAttachments(database2, settings);

                    Assert.Equal(attachmentsCount, Attachments.Count);

                    // I don't have retire attachments config. but as in other background task features, I populate the retire attachment tree after replicating it
                    GetToRetireAttachmentsCount(database2, attachmentsCount);

                    try
                    {
                        var identifier2 = await PutRetireAttachmentsConfiguration(store2, settings);
                        // move in time & start retire
                        database2.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                        await database2.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                        var cloudObjects = await GetBlobsFromCloudAndAssertForCount(settings, attachmentsCount, 15_000);
                        await AssertAllRetiredAttachments(store2, cloudObjects, size, identifier2);

                        // on store 1 the attachments are still not retired, so we cannot get them
                        await GetBlobsFromCloudAndAssertForCount(Settings, 0, 15_000);

                        var database1 = (await GetDocumentDatabaseInstanceForAsync(store1.Database));

                        using (database1.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var attachments = database1.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();
                            Assert.Equal(attachmentsCount, attachments.Count);
                            Assert.All(attachments, attachment => Assert.True(attachment.RetireParameters.Flags == RetiredAttachmentFlags.None));
                        }

                        // replicate retired attachments to source
                        await SetupReplicationAsync(store2, store1);
                        await EnsureReplicatingAsync(store2, store1);

                        using (database1.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var attachments = database1.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();
                            Assert.Equal(attachmentsCount, attachments.Count);

                            await Assert.AllAsync(attachments, async attachment =>
                            {
                                Assert.True(attachment.RetireParameters.Flags == RetiredAttachmentFlags.None);
                                // we cannot receive it using source retired attachment configuration
                                var a = Attachments.FirstOrDefault(x => x.Key == attachment.Key);
                                Assert.NotNull(a);
                                attachment.Stream = a.Stream;

                                // this sends GetAttachmentOperation and compares the result
                                await GetAndCompareRetiredAttachment(store1, a.DocumentId, attachment.Name, attachment.Base64Hash.ToString(), attachment.ContentType, (MemoryStream)attachment.Stream, size, identifier1, RetiredAttachmentFlags.None);
                            });

                            // update the retired attachments configuration to be same as destination
                            // we still didn't retire the attachments on source, so we cannot get them
                            await GetBlobsFromCloudAndAssertForCount(Settings, 0, 15_000);

                            // move in time & start retire
                            database1.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                            await database1.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                            await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
                            await Assert.AllAsync(attachments, async attachment =>
                            {
                                // we loaded retired attachment from storage it doesn't have stream, so we populate it from the one we saved in test, so we can compare
                                var a = Attachments.FirstOrDefault(x => x.Key == attachment.Key);
                                Assert.NotNull(a);
                                attachment.Stream = a.Stream;

                                // this sends GetAttachmentOperation and compares the result
                                await GetAndCompareRetiredAttachment(store1, a.DocumentId, attachment.Name, attachment.Base64Hash.ToString(), attachment.ContentType, (MemoryStream)attachment.Stream, size, identifier1);
                            });
                        }
                    }
                    finally
                    {
                        await DeleteObjects(settings);
                    }
                }
            }
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        public async Task CanBackupRetiredAttachments(int attachmentsCount, int size)
        {
            await CanBackupRetiredAttachmentsInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanExportImportWithRetiredAttachment(int attachmentsCount, int size)
        {
            await CanExportImportWithRetiredAttachmentInternal(attachmentsCount, size);
        }


        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanIndexWithRetiredAttachment(int attachmentsCount, int size)
        {
            await CanIndexWithRetiredAttachmentInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory(Skip = "TODO EGOR RavenDB-24604")]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanEtlWithRetiredAttachmentAndRetireOnDestination(int attachmentsCount, int size)
        {
            await CanEtlWithRetiredAttachmentAndRetireOnDestinationInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory(Skip = "TODO EGOR RavenDB-24604")]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanEtlRetiredAttachmentsToDestination(int attachmentsCount, int size)
        {
            await CanEtlRetiredAttachmentsToDestinationInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory(Skip = "TODO EGOR RavenDB-24604")]
        [InlineData(1, 3, true)]
        [InlineData(1, 3, false)]
     //   [InlineData(64, 3, true)]
      //  [InlineData(64, 3, false)]
        public async Task CanEtlDeletedRetiredAttachmentsToDestination(int attachmentsCount, int size, bool retireOnReplica)
        {
            await using (var holder = CreateCloudSettings())
            {
                int docsCount = RetiredAttachmentsHolderBase.GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                var ids = new List<(string Id, string Collection)>();
                using (var store = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_source"
                }))
                using (var replica = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_destination"
                }))
                {
                    var identifier1 = await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc);
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

                    var res = Etl.AddEtl(store, configuration, connectionString);

                    etlDone.Wait(TimeSpan.FromSeconds(15));
                    var replicaDb = await Databases.GetDocumentDatabaseInstanceFor(replica);
                    var val4 = WaitForValue(() =>
                    {
                        using (replicaDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var c = replicaDb.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).Count();
                            return c;
                        }
                    }, attachmentsCount, 30_000);
                    Assert.Equal(attachmentsCount, val4);
                    var identifier2 = await PutRetireAttachmentsConfiguration(replica, Settings);

                    await AssertGetRetiredAttachmentsInBulk(replica, size, identifier2, RetiredAttachmentFlags.None);

                    if (retireOnReplica)
                    {
                        // move in time & start retire
                        replicaDb.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                        await replicaDb.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                        var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
                        await AssertAllRetiredAttachments(replica, cloudObjects, size, identifier2);

                        var stats = replica.Maintenance.Send(new GetDetailedStatisticsOperation());
                        Assert.Equal(attachmentsCount, stats.CountOfRetiredAttachments);

                    }


                    foreach (var attachment in Attachments)
                    {
                        await store.Operations.SendAsync(new DeleteAttachmentOperation(attachment.DocumentId, attachment.Name));
                    }

                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount); // we dont delete the retired attachments from cloud, so we still have them

                    etlDone = Etl.WaitForEtlToComplete(store);
                    etlDone.Wait(TimeSpan.FromSeconds(15));

                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var c = database.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).Count();
                        Assert.Equal(0, c); // we deleted the attachments from source, so we don't have them in storage
                    }

                    
                    var val3 = WaitForValue(() =>
                    {
                        using (replicaDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var c = replicaDb.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).Count();
                            return c;
                        }
                    }, 0, 30_000);
                    Assert.Equal(0, val3);
                }
            }
        }

        [AmazonS3RetryTheory(Skip = "TODO EGOR RavenDB-24604")]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanExternalReplicateDeletedRetiredAttachmentsToDestination(int attachmentsCount, int size)
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await using (var holder = CreateCloudSettings())
                {
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();
                    var identifier = await PutRetireAttachmentsConfiguration(store1, Settings);
                    await CreateDocs(store1, docsCount, ids);
                    await PopulateDocsWithRandomAttachments(store1, identifier, size, ids, attachmentsPerDoc);

                    await SetupReplicationAsync(store1, store2);
                    await EnsureReplicatingAsync(store1, store2);

                    var database2 = (await GetDocumentDatabaseInstanceForAsync(store2.Database));

                    // I create new settings for destination database, so each db upload to different folder
                    var settings = Etl.GetS3Settings(nameof(RetiredAttachments), $"{store2.Database}-{Guid.NewGuid()}");
                    GetStorageAttachmentsMetadataFromAllAttachments(database2, settings);

                    Assert.Equal(attachmentsCount, Attachments.Count);

                    // I don't have retire attachments config. but as in other background task features, I populate the retire attachment tree after replicating it
                    GetToRetireAttachmentsCount(database2, attachmentsCount);

                    var database = await Databases.GetDocumentDatabaseInstanceFor(store1);
                    GetStorageAttachmentsMetadataFromAllAttachments(database);
                    Assert.Equal(attachmentsCount, Attachments.Count);

                    // move in time & start retire
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                    var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);

                    await AssertAllRetiredAttachments(store1, cloudObjects, size, identifier);

                    using (database2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var attachments = database2.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();
                        Assert.Equal(attachmentsCount, attachments.Count);

                        await Assert.AllAsync(attachments, async attachment =>
                        {
                            Assert.True(attachment.RetireParameters.Flags == RetiredAttachmentFlags.None);
                            // we cannot receive it using source retired attachment configuration
                            var a = Attachments.FirstOrDefault(x => x.Key == attachment.Key);
                            Assert.NotNull(a);
                            attachment.Stream = a.Stream;

                            // this sends GetAttachmentOperation and compares the result
                            await GetAndCompareRetiredAttachment(store2, a.DocumentId, attachment.Name, attachment.Base64Hash.ToString(), attachment.ContentType,
                                (MemoryStream)attachment.Stream, size, identifier, RetiredAttachmentFlags.None);
                        });
                    }

                    var stats = store1.Maintenance.Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(attachmentsCount, stats.CountOfRetiredAttachments);

                    var stats2 = store2.Maintenance.Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(0, stats2.CountOfRetiredAttachments);
                    Assert.Equal(attachmentsCount, stats2.CountOfAttachments);



                    foreach (var attachment in Attachments)
                    {
                        await store1.Operations.SendAsync(new DeleteAttachmentOperation(attachment.DocumentId, attachment.Name));
                    }

                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);

                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                    // we don't delete the retired attachments from cloud, so we still have them
                    await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount);

                    await EnsureReplicatingAsync(store1, store2);

                    var stats11 = store1.Maintenance.Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(0, stats11.CountOfRetiredAttachments);
                    Assert.Equal(0, stats11.CountOfAttachments);
                    var stats22 = store2.Maintenance.Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(0, stats22.CountOfRetiredAttachments);
                    Assert.Equal(0, stats22.CountOfAttachments);
                }
            }
        }
    }
}
