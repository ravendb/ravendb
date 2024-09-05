using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Orders;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Attachments
{
    public class S3RetiredAttachmentsSlowTests : RetiredAttachmentsS3Base
    {
        //TODO: egor test CanUploadRetiredAttachmentToS3IfItAlreadyExists - will rewrite the retired attachment, even if it is the same - is it the behaviour we want?
        //TODO: egor do big attachments tests
        //TODO: egor test for "now we delete doc with retired attachemnt, it will delete the retire attachment from cloud!"
        public S3RetiredAttachmentsSlowTests(ITestOutputHelper output) : base(output)
        {
        }


        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanCrudAttachmentWhenHaveRetiredAttachment(bool purgeOnDelete)
        {
            var attachmentsCount = 1;
            var size = 3;
            await using (var holder = CreateCloudSettings())
            {
                using (var store = GetDocumentStore())
                {
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();

                    RetiredAttachments.ModifyRetiredAttachmentsConfig = config =>
                    {
                        config.PurgeOnDelete = purgeOnDelete;
                    };

                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc, null);

                    var data = Attachments.FirstOrDefault();
                    Assert.NotNull(data);

                    using (var profileStream = new MemoryStream(new byte[] { 3, 2, 2 }))
                    {
                        // retire of this attachment should happen in baseline + 40 mins
                        var result = store.Operations.Send(new PutAttachmentOperation(data.DocumentId, "profile.png", profileStream, "image/png"));
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
                    if (purgeOnDelete)
                    {
              database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                        var key = string.Empty;
                        S3RetiredAttachmentsSlowTests.GetToRetireAttachmentsCount(database, 1, infos =>
                        {
                            if (infos == null)
                                return;

                            key = infos.First().LowerId.ToString();
                        });

                        var expected = $"d\u001eOrders\u001eorders/0\u001ed\u001etest_0.png\u001e{data.Hash}\u001eimage/png";
                        Assert.Equal(expected, key);
                        await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);

                        database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(1);
                        await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                        await GetBlobsFromCloudAndAssertForCount(Settings, 0, 15_000);
                    }
                    else
                    {
 database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                        var key = string.Empty;
                        S3RetiredAttachmentsSlowTests.GetToRetireAttachmentsCount(database, 1, infos =>
                        {
                            if (infos == null)
                                return;

                            key = infos.First().LowerId.ToString();
                        });

                        await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);

                        Assert.Equal("p\u001eorders/0\u001ed\u001eprofile.png\u001ebucfDXJ3eWRJYpgggJrnskJtMuMyFohjO2GHATxTmUs=\u001eimage/png", key);
                    }

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

                    S3RetiredAttachmentsSlowTests.GetToRetireAttachmentsCount(await Databases.GetDocumentDatabaseInstanceFor(Server, store), 0);
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
                    await PutRetireAttachmentsConfiguration(store, Settings);
                    var docId = "Orders/3";
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order { Id = docId, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/2", Company = $"Companies/2" });

                        await session.SaveChangesAsync();
                    }

                    using var profileStream = new MemoryStream([1, 2, 3]);
                    await store.Operations.SendAsync(new PutAttachmentOperation(docId, "test.png", profileStream, "image/png"));

                    var res = await store.Operations.SendAsync(new GetAttachmentOperation(docId, "test.png", AttachmentType.Document, null));
                    Assert.Equal("test.png", res.Details.Name);

                    int count = 0;
                    DocumentDatabase database = null;
                    //Assert.Equal(1, await WaitForValueAsync(async () =>
                    //{
                    //  
                    //    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);

                    //    count += await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                    //    return count;
                    //}, 1, interval: 1000));

                    var key = string.Empty;
                    database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    S3RetiredAttachmentsSlowTests.GetToRetireAttachmentsCount(database, 1, infos =>
                    {
                        if (infos == null)
                            return;

                        key = infos.First().LowerId.ToString();
                    });

                    Assert.Equal("p\u001eorders/3\u001ed\u001etest.png\u001eEcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=\u001eimage/png", key);
                       
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

                    //var key = string.Empty;
                    //S3RetiredAttachmentsSlowTests.GetToRetireAttachmentsCount(database, 1, infos =>
                    //{
                    //    if(infos == null)
                    //        return;

                    //    key = infos.First().LowerId.ToString();
                    //});

                    //Console.WriteLine();


                    //count = 0;
                    //Assert.Equal(0, await WaitForValueAsync(async () =>
                    //{
                    //    database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    //    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);

                    //    count += await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                    //    return count;
                    //}, 0, interval: 1000));
                    GetToRetireAttachmentsCount(database, 0);
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanPutInRetiredAttachmentAndDeleteTheDocBeforeRetirementInCloud()
        {
            var attachmentsCount = 1;
            var size = 3;
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
                    await PutRetireAttachmentsConfiguration(store, Settings);
                    var docId = "Orders/3";
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order { Id = docId, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/2", Company = $"Companies/2" });

                        await session.SaveChangesAsync();
                    }

                    using var profileStream = new MemoryStream([1, 2, 3]);
                    await store.Operations.SendAsync(new PutAttachmentOperation(docId, "test.png", profileStream, "image/png"));

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
                        Assert.Equal("p\u001eorders/3\u001ed\u001etest.png\u001eEcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=\u001eimage/png", key);
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

        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task DeletingDocumentWithRetiredAttachmentShouldKeepRetiredAttachmentByDefault(bool purgeOnDelete)
        {
            var attachmentsCount = 1;
            var size = 3;
            await using (var holder = CreateCloudSettings())
            {
                using (var store = GetDocumentStore())
                {
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();

                    RetiredAttachments.ModifyRetiredAttachmentsConfig = config =>
                    {
                        config.PurgeOnDelete = purgeOnDelete;
                    };

                    await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc, null);

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

                    if (purgeOnDelete)
                    {
                        var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                        database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                        await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                        await GetBlobsFromCloudAndAssertForCount(Settings, 0, 15_000);
                    }
                    else
                    {
                        var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                        GetToRetireAttachmentsCount(database, 0);
                        await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task DeletingDocumentWithRetiredAttachmentShouldKeepRetiredAttachmentByDefaultInCluster(bool purgeOnDelete)
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

                    RetiredAttachments.ModifyRetiredAttachmentsConfig = config =>
                    {
                        config.PurgeOnDelete = purgeOnDelete;
                    };

                    //TODO: egor /////////////////////// DUPLICATE CODE WITH CanUploadRetiredAttachmentToCloudInClusterAndDeleteInternal
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

                        count += await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                        return count;
                    }, attachmentsCount, interval: 1000);


                    Assert.Equal(attachmentsCount, retired);

                    var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
                    await AssertAllRetiredAttachments(store, cloudObjects, size);

                    //TODO: egor ////////////////////// DUPLICATE CODE WITH CanUploadRetiredAttachmentToCloudInClusterAndDeleteInternal
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
                 //       Assert.Equal(true, await WaitForChangeVectorInClusterAsync(srcNodes.Servers, srcDb));
                        if (purgeOnDelete)
                        {

                            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)).ConfigureAwait(false);
                            var f = record.Topology.AllNodes.FirstOrDefault();

                            foreach (var node in srcRaft.Nodes)
                            {
                                database = await Databases.GetDocumentDatabaseInstanceFor(node, store);

                                GetToRetireAttachmentsCount(database, 1);
                                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                            }
                            await GetBlobsFromCloudAndAssertForCount(Settings, 0, 15_000);
                        }
                        else
                        {
                            foreach (var node in srcRaft.Nodes)
                            {
                                database = await Databases.GetDocumentDatabaseInstanceFor(node, store);
                                //var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                                GetToRetireAttachmentsCount(database, 0);

                                // nothing should happen
                                //database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                                //await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                            }

                            await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
                        }
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
                    await PutRetireAttachmentsConfiguration(store, Settings, collections);
                    await CreateDocs(store, docsCount, ids, collections);
                    await PopulateDocsWithRandomAttachments(store, 3, ids, attachmentsPerDoc);

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

                var options = new BackgroundWorkParameters(context, DateTime.MaxValue, dbRecord, nodeTag, int.MinValue);
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
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        //[InlineData(128, 3)]
        //TODO: egor in the future need optimizations so I can do a lot faster[InlineData(256)]
        //[InlineData(1024)]
        public async Task CanUploadRetiredAttachmentToS3AndGet(int attachmentsCount, int size)
        {
            await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(64, 3)]
        //[InlineData(128, 3)]
        //TODO: egor in the future need optimizations so I can do a lot faster[InlineData(256)]
        //[InlineData(1024, 3)]
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
        [InlineData(1, 3, true)]
        [InlineData(64, 3, true)]
        [InlineData(1, 3, false)]
        [InlineData(64, 3, false)]
        public async Task CanUploadRetiredAttachmentToS3AndDelete(int attachmentsCount, int size, bool storageOnly)
        {
            await CanUploadRetiredAttachmentToCloudAndDeleteInternal(attachmentsCount, size, storageOnly: storageOnly);
        }

        [AmazonS3RetryTheory]
        [InlineData(16, 3, 4)]
        //[InlineData(64, 3, 4)]
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
        public async Task ShouldThrowUsingRegularAttachmentsApiOnRetiredAttachmentToS3()
        {
            await ShouldThrowUsingRegularAttachmentsApiOnRetiredAttachmentInternal();
        }


        [AmazonS3RetryTheory]
        [InlineData(3, 3, 1)]
        [InlineData(16, 3, 4)]
        //[InlineData(128, 3)]
        public async Task CanUploadRetiredAttachmentsFromDifferentCollectionsToS3AndGetInBulk(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            var collections = new List<string> { "Orders", "Products" };
            await CanUploadRetiredAttachmentsToCloudAndGetInBulkInternal(attachmentsCount, size, attachmentsPerDoc, collections);
        }

        [AmazonS3RetryTheory]
        [InlineData(3, 3, 1)]
        [InlineData(16, 3, 4)]
        //[InlineData(128, 3)]
        public async Task CanUploadRetiredAttachmentsToS3AndGetInBulk(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            await CanUploadRetiredAttachmentsToCloudAndGetInBulkInternal(attachmentsCount, size, attachmentsPerDoc);
        }
        [AmazonS3RetryTheory]
        [InlineData(3, 3, 1)]
        [InlineData(16, 3, 4)]
        //[InlineData(128, 3)]
        public async Task CanUploadRetiredAttachmentsToS3AndDeleteInBulk(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            await CanUploadRetiredAttachmentsToCloudAndDeleteInBulkInternal(attachmentsCount, size, attachmentsPerDoc);
        }

        [AmazonS3RetryFact]
        public async Task CanUploadRetiredAttachmentToS3IfItAlreadyExists()
        {
            await CanUploadRetiredAttachmentToCloudIfItAlreadyExistsInternal();
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
        //[InlineData(128, 3)]

        //TODO: egor add test that backup & restore already retired attachment (so the stream is null) (maybe should throw if there is no config?)
        public async Task CanUploadRetiredAttachmentToS3FromBackupAndGet(int attachmentsCount, int size)
        {

            await CanUploadRetiredAttachmentToCloudFromBackupAndGet(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanExternalReplicateRetiredAttachmentAndThenUploadToS3AndGet(int attachmentsCount, int size)
        {
            await CanExternalReplicateRetiredAttachmentAndThenUploadToCloudAndGet(attachmentsCount, size);
        }


        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        //[InlineData(64, 3)]
        //[InlineData(128, 3)]

        //TODO: egor add test that backup & restore already retired attachment (so the stream is null) (maybe should throw if there is no config?)
        public async Task CanBackupRetiredAttachments(int attachmentsCount, int size)
        {
            await CanBackupRetiredAttachmentsInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        //[InlineData(128, 3)]

        //TODO: egor add test that backup & restore already retired attachment (so the stream is null) (maybe should throw if there is no config?)
        public async Task CanExportImportWithRetiredAttachment(int attachmentsCount, int size)
        {
            await CanExportImportWithRetiredAttachmentInternal(attachmentsCount, size);
        }


        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        //[InlineData(128, 3)]

        //TODO: egor add test that backup & restore already retired attachment (so the stream is null) (maybe should throw if there is no config?)
        public async Task CanIndexWithRetiredAttachment(int attachmentsCount, int size)
        {
            await CanIndexWithRetiredAttachmentInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        //[InlineData(128, 3)]

        public async Task CanEtlWithRetiredAttachmentAndRetireOnDestination(int attachmentsCount, int size)
        {
            await CanEtlWithRetiredAttachmentAndRetireOnDestinationInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        //[InlineData(128, 3)]

        public async Task CanEtlRetiredAttachmentsToDestination(int attachmentsCount, int size)
        {
            await CanEtlRetiredAttachmentsToDestinationInternal(attachmentsCount, size);
        }

    }
}
