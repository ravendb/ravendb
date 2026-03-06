using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Client;
using FastTests.Utils;
using Orders;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.BackgroundWork;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Client.Attachments;
using Sparrow.Server;
using Sparrow.Utils;
using Tests.Infrastructure;
using Tests.Infrastructure.Extensions;
using Voron;
using Xunit;

namespace SlowTests.Server.Documents.Attachments
{
    public class S3RemoteAttachmentsSlowTests : RemoteAttachmentsS3Base
    {
        public S3RemoteAttachmentsSlowTests(ITestOutputHelper output) : base(output)
        {
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanProcessItemWithDeleteStatusInSender(int attachmentsCount, int size)
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
                    await PopulateDocsWithRandomAttachments(store, identifier, size, ids, attachmentsPerDoc);
                    Assert.Equal(true, await WaitForChangeVectorInClusterAsync(srcNodes.Servers, srcDb));

                    int count = 0;
                    DocumentDatabase database = null;

                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)).ConfigureAwait(false);

                    // ReSharper disable once InconsistentNaming
                    string F = record.Topology.AllNodes.FirstOrDefault();

                    var remote = await WaitForValueAsync(async () =>
                    {
                        record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)).ConfigureAwait(false);
                        F = record.Topology.AllNodes.FirstOrDefault();
                        var srv = srcRaft.Nodes.FirstOrDefault(x => x.ServerStore.NodeTag == F);
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

                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)).ConfigureAwait(false);
                    var notF = record.Topology.AllNodes.FirstOrDefault(x => x != F);
                    var srv = srcRaft.Nodes.FirstOrDefault(x => x.ServerStore.NodeTag == notF);
                    Assert.NotNull(srv);
                    database = await Databases.GetDocumentDatabaseInstanceFor(srv, store);
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    var deleted = await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                    Assert.Equal(attachmentsCount, deleted);

                    var notNotF = record.Topology.AllNodes.FirstOrDefault(x => x != F && x != notF);
                    srv = srcRaft.Nodes.FirstOrDefault(x => x.ServerStore.NodeTag == notNotF);
                    Assert.NotNull(srv);
                    database = await Databases.GetDocumentDatabaseInstanceFor(srv, store);
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    var deleted2 = await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                    Assert.Equal(attachmentsCount, deleted2);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task ShouldThrowOnDocumentWithRemoteAttachmentsOnImportToShardedDatabase()
        {
            int attachmentsCount = 1;
            int size = 3;
            using (var store = GetDocumentStore())
            using (var sharded = Sharding.GetDocumentStore())
            {
                await using (var holder = CreateCloudSettings())
                {
                    int docsCount = RemoteAttachmentsHolderBase.GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();
                    var identifier = await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc, null);

                    using (var dest = new MemoryStream())
                    {
                        var export = await store.Smuggler.ExportToStreamAsync(new DatabaseSmugglerExportOptions(), s => s.CopyToAsync(dest));
                        await export.WaitForCompletionAsync();
                        dest.Position = 0;
                        var import = await sharded.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                        {
                        }, dest);

                        var e = await Assert.ThrowsAsync<RavenException>(() => import.WaitForCompletionAsync());

                        Assert.Contains("System.NotSupportedException: Document 'Orders/0' cannot be imported because it contains remote attachments, which are not supported in sharded databases. Consider downloading the attachments locally before importing to a sharded database.", e.Message);
                    }
                }
            }
        }

        [AmazonS3RetryTheory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanPatchRemoteAttachments(bool patchMultiple)
        {
            int attachmentsCount = 2;
            int size = 3;
            await using (var holder = CreateCloudSettings())
            {
                using (var store = GetDocumentStore())
                {
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();

                    var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);
                    await CreateDocs(store, docsCount, ids);
                    await PopulateDocsWithRandomAttachments(store, identifier, size, ids, attachmentsPerDoc, remote: false);

                    var id = ids.First().Id;

                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    GetStorageAttachmentsMetadataFromAllAttachments(database);
                    Assert.Equal(attachmentsCount, Attachments.Count);
                    var att1 = Attachments.First().Name;
                    var att2 = Attachments.Last().Name;
                    using (AttachmentResult att = await store.Operations.SendAsync(new GetAttachmentOperation(id, att1, AttachmentType.Document, null)))
                    {
                        Assert.Equal(att.Details.Name, att1);
                        Assert.Null(att.Details.RemoteParameters);
                    }

                    if (patchMultiple)
                    {
                        using (AttachmentResult att = await store.Operations.SendAsync(new GetAttachmentOperation(id, att2, AttachmentType.Document, null)))
                        {
                            Assert.Equal(att.Details.Name, att2);
                            Assert.Null(att.Details.RemoteParameters);
                        }
                    }

                    var dt = DateTime.UtcNow.AddMinutes(3);

                    var patch = new PatchRequest
                    {
                        Script = "attachments(this, args.name).remote(args.identifier, args.at);",
                        Values =
                        {
                            { "name", att1 },
                            { "identifier", identifier },
                            { "at", dt },
                        }
                    };

                    var result = await store.Operations.SendAsync(new PatchOperation(id, null, patch));
                    Assert.Equal(result, PatchStatus.Patched);
                    if (patchMultiple)
                    {
                        patch.Values["name"] = att2;
                        var result2 = await store.Operations.SendAsync(new PatchOperation(id, null, patch));
                        Assert.Equal(result2, PatchStatus.Patched);
                    }

                    using (AttachmentResult att = await store.Operations.SendAsync(new GetAttachmentOperation(id, att1, AttachmentType.Document, null)))
                    {
                        Assert.Equal(att.Details.Name, att1);
                        Assert.NotNull(att.Details.RemoteParameters);

                        Assert.Equal(identifier, att.Details.RemoteParameters.Identifier);
                        Assert.Equal(dt, att.Details.RemoteParameters.At);
                    }

                    if (patchMultiple)
                    {
                        using (AttachmentResult att = await store.Operations.SendAsync(new GetAttachmentOperation(id, att2, AttachmentType.Document, null)))
                        {
                            Assert.Equal(att.Details.Name, att2);
                            Assert.NotNull(att.Details.RemoteParameters);

                            Assert.Equal(identifier, att.Details.RemoteParameters.Identifier);
                            Assert.Equal(dt, att.Details.RemoteParameters.At);
                        }
                    }

                    // move in time & start remote
                        database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                    var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, patchMultiple ? 2 : 1, 15_000);
                    Assert.Equal(patchMultiple ? 2 : 1, cloudObjects.Count);

                    var stats = store.Maintenance.Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(patchMultiple ? 2 : 1, stats.CountOfRemoteAttachments);
                    Assert.Equal(2, stats.CountOfAttachments);
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanPatchRemoteAttachments_NonExists()
        {
            int attachmentsCount = 2;
            int size = 3;
            await using (var holder = CreateCloudSettings())
            {
                using (var store = GetDocumentStore())
                {
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();

                    var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);
                    await CreateDocs(store, docsCount, ids);
                    await PopulateDocsWithRandomAttachments(store, identifier, size, ids, attachmentsPerDoc, remote: false);

                    var id = ids.First().Id;

                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    GetStorageAttachmentsMetadataFromAllAttachments(database);
                    Assert.Equal(attachmentsCount, Attachments.Count);
                    var att1 = Attachments.First().Name;
                    using (AttachmentResult att = await store.Operations.SendAsync(new GetAttachmentOperation(id, att1, AttachmentType.Document, null)))
                    {
                        Assert.Equal(att.Details.Name, att1);
                        Assert.Null(att.Details.RemoteParameters);
                    }

                    var dt = DateTime.UtcNow.AddMinutes(3);

                    var patch = new PatchRequest
                    {
                        Script = "attachments(this, args.name).remote(args.identifier, args.at);",
                        Values =
                        {
                            { "name", "EGOR" },
                            { "identifier", identifier },
                            { "at", dt },
                        }
                    };

                    var result = await store.Operations.SendAsync(new PatchOperation(id, null, patch));
                    Assert.Equal(result, PatchStatus.NotModified);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task RemoteAttachmentWithDisabledIdentifierShouldBeSkipped()
        {
            int attachmentsCount = 1;
            int size = 3;
            await using (var holder = CreateCloudSettings())
            {
                using (var store = GetDocumentStore())
                {
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();

                    var badIdentifierDisabled = "new-identifier"; // should be skipped

                    ModifyRemoteAttachmentsConfig = config =>
                    {
                        config.Destinations.Add(badIdentifierDisabled, new RemoteAttachmentsDestinationConfiguration
                        {
                            S3Settings = new RemoteAttachmentsS3Settings()
                            {
                                BucketName = "TEST"
                            },
                            Disabled = true,
                        });

                    };

                    var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);

                    await CreateDocs(store, docsCount, ids);
                    await PopulateDocsWithRandomAttachments(store, identifier, size, ids, attachmentsPerDoc);

                    var id = ids.First().Id;
                    using (var session = store.OpenSession())
                    {
                        var profileStream = new MemoryStream([1, 2, 3]);
                        var remoteParams = new RemoteAttachmentParameters(badIdentifierDisabled, DateTime.UtcNow.AddMinutes(3));
                        session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("att-bad-identifier.png", profileStream)
                        {
                            RemoteParameters = remoteParams,
                            ContentType = "image/png"
                        });

                        session.SaveChanges();
                        using AttachmentResult a = await store.Operations.SendAsync(new GetAttachmentOperation(id, "att-bad-identifier.png", AttachmentType.Document, null));

                        Attachments.Add(new RemoteAttachment()
                        {
                            Name = a.Details.Name,
                            DocumentId = id,
                            Stream = profileStream,
                            ContentType = a.Details.ContentType,
                            Hash = a.Details.Hash,
                            RemoteParameters = remoteParams
                        });
                    }

                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    GetStorageAttachmentsMetadataFromAllAttachments(database);
                    Assert.Equal(attachmentsCount + 1, Attachments.Count);

                    List<string> myExceptions = new List<string>();
                    database.RemoteAttachmentsSender.ForTestingPurposesOnly().BeforeEndOfTheBatch = exceptions =>
                    {
                        myExceptions.Add(exceptions);
                    };

                    // move in time & start remote
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                    var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
                    Assert.Equal(1, cloudObjects.Count);

                    var stats = store.Maintenance.Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(1, stats.CountOfRemoteAttachments);
                    Assert.Equal(2, stats.CountOfAttachments);

                    Assert.NotNull(myExceptions);
                    Assert.Equal(0, myExceptions.Count);
                    using AttachmentResult bad = await store.Operations.SendAsync(new GetAttachmentOperation(id, "att-bad-identifier.png", AttachmentType.Document, null));

                    Assert.Equal("att-bad-identifier.png", bad.Details.Name);
                    Assert.Equal(RemoteAttachmentFlags.None, bad.Details.RemoteParameters.Flags);
                    Assert.NotNull(bad.Details.RemoteParameters.At);

                    var goodName = Attachments.First(x => x.Name != "att-bad-identifier.png").Name;
                    using AttachmentResult good = await store.Operations.SendAsync(new GetAttachmentOperation(id, goodName, AttachmentType.Document, null));

                    Assert.Equal(goodName, good.Details.Name);
                    Assert.Equal(RemoteAttachmentFlags.Remote, good.Details.RemoteParameters.Flags);
                    Assert.NotNull(good.Details.RemoteParameters.At);

                }
            }
        }

        [AmazonS3RetryTheory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task RemoteAttachmentWithBadIdentifierShouldBeSkippedWithError(bool uploadAdditional)
        {
            int attachmentsCount = 1;
            int size = 3;
            await using (var holder = CreateCloudSettings())
            {
                using (var store = GetDocumentStore())
                {
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();

                    var badIdentifierEnabled = "test-identifier"; // should be skipped

                    ModifyRemoteAttachmentsConfig = config =>
                    {
                        config.Destinations.Add(badIdentifierEnabled, new RemoteAttachmentsDestinationConfiguration
                        {
                            S3Settings = new RemoteAttachmentsS3Settings()
                            {
                                BucketName = "TEST2"
                            },
                            Disabled = false,
                        });
                    };

                    var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);

                    await CreateDocs(store, docsCount, ids);
                    await PopulateDocsWithRandomAttachments(store, identifier, size, ids, attachmentsPerDoc);

                    var id = ids.First().Id;
                    using (var session = store.OpenSession())
                    {
                        var profileStream = new MemoryStream([1, 2, 3]);
                        var remoteParams = new RemoteAttachmentParameters(badIdentifierEnabled, DateTime.UtcNow.AddMinutes(3));
                        session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("att-bad-identifier.png", profileStream)
                        {
                            RemoteParameters = remoteParams,
                            ContentType = "image/png"
                        });

                        session.SaveChanges();
                        using AttachmentResult a = await store.Operations.SendAsync(new GetAttachmentOperation(id, "att-bad-identifier.png", AttachmentType.Document, null));

                        Attachments.Add(new RemoteAttachment()
                        {
                            Name = a.Details.Name,
                            DocumentId = id,
                            Stream = profileStream,
                            ContentType = a.Details.ContentType,
                            Hash = a.Details.Hash,
                            RemoteParameters = remoteParams
                        });
                    }

                        var id2 = "Orders/322";
                    if (uploadAdditional)
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            await session.StoreAsync(new Query.Order { Id = id2, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/2", Company = $"Companies/2" });

                            await session.SaveChangesAsync();

                            var profileStream = new MemoryStream([3, 2, 2]);
                            var remoteParams = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3));
                            session.Advanced.Attachments.Store(id2, new StoreAttachmentParameters("egor.png", profileStream)
                            {
                                RemoteParameters = remoteParams,
                                ContentType = "image/png"
                            });

                            await session.SaveChangesAsync();
                            using AttachmentResult a = await store.Operations.SendAsync(new GetAttachmentOperation(id2, "egor.png", AttachmentType.Document, null));

                            Attachments.Add(new RemoteAttachment()
                            {
                                Name = a.Details.Name,
                                DocumentId = id2,
                                Stream = profileStream,
                                ContentType = a.Details.ContentType,
                                Hash = a.Details.Hash,
                                RemoteParameters = remoteParams
                            });
                        }
                    }

                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    GetStorageAttachmentsMetadataFromAllAttachments(database);
                    if (uploadAdditional)
                    {
                        Assert.Equal(attachmentsCount + 2, Attachments.Count);
                    }
                    else
                    {
                        Assert.Equal(attachmentsCount + 1, Attachments.Count);
                    }

                    List<string> myExceptions = new List<string>();
                    database.RemoteAttachmentsSender.ForTestingPurposesOnly().BeforeEndOfTheBatch = exceptions =>
                    {
                        myExceptions.Add(exceptions);
                    };


                    var oldDt = DateTime.UtcNow;

                        oldDt = oldDt.AddMinutes(15);

                        // move in time & start remote
                        database.Time.UtcDateTime = () => oldDt;
                        await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                    // nothing was uploaded because we had a faulty identifier
                    if (uploadAdditional)
                    {
                        var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, 2, 15_000);

                        Assert.Equal(2, cloudObjects.Count);

                    }
                    else
                    {
                        var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);

                        Assert.Equal(1, cloudObjects.Count);

                    }

                    var stats = store.Maintenance.Send(new GetDetailedStatisticsOperation());
                    if (uploadAdditional)
                    {

                        Assert.Equal(2, stats.CountOfRemoteAttachments);
                        Assert.Equal(3, stats.CountOfAttachments);


                    }
                    else
                    {
                        Assert.Equal(1, stats.CountOfRemoteAttachments);
                        Assert.Equal(2, stats.CountOfAttachments);

                    }


                    Assert.NotNull(myExceptions);
                    Assert.Equal(1, myExceptions.Count);

                    var exception = myExceptions.FirstOrDefault();
                    Assert.NotNull(exception);
                    Assert.Contains("Failed to upload remote attachment for identifier", exception);

                    using AttachmentResult bad = await store.Operations.SendAsync(new GetAttachmentOperation(id, "att-bad-identifier.png", AttachmentType.Document, null));

                    Assert.Equal("att-bad-identifier.png", bad.Details.Name);
                    Assert.Equal(RemoteAttachmentFlags.None, bad.Details.RemoteParameters.Flags);
                    Assert.NotNull(bad.Details.RemoteParameters.At);

                    var goodName = Attachments.First(x => x.Name != "att-bad-identifier.png" && x.DocumentId == id).Name;
                    using AttachmentResult good = await store.Operations.SendAsync(new GetAttachmentOperation(id, goodName, AttachmentType.Document, null));

                    Assert.Equal(goodName, good.Details.Name);
                    Assert.Equal(RemoteAttachmentFlags.Remote, good.Details.RemoteParameters.Flags);
                    Assert.NotNull(good.Details.RemoteParameters.At);


                    if (uploadAdditional)
                    {
                        var goodName2 = Attachments.First(x => x.DocumentId == id2).Name;
                        using AttachmentResult good2 = await store.Operations.SendAsync(new GetAttachmentOperation(id2, goodName2, AttachmentType.Document, null));

                        Assert.Equal(goodName2, good2.Details.Name);
                        Assert.Equal(RemoteAttachmentFlags.Remote, good2.Details.RemoteParameters.Flags);
                        Assert.NotNull(good2.Details.RemoteParameters.At);
                    }

                }
            }
        }

        [AmazonS3RetryFact]
        public async Task RemoteAttachmentWithoutDestinationOnDocumentWithRemoteAttachmentWithDestinationShouldBeSkipped()
        {
            int attachmentsCount = 1;
            int size = 3;
            await using (var holder = CreateCloudSettings())
            {
                using (var store = GetDocumentStore())
                {
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();

                    var removedIdentifier = "test-identifier"; // should be skipped

                    ModifyRemoteAttachmentsConfig = config =>
                    {

                        config.Destinations.Add(removedIdentifier, new RemoteAttachmentsDestinationConfiguration
                        {
                            S3Settings = new RemoteAttachmentsS3Settings()
                            {
                                BucketName = "TEST2"
                            },
                            Disabled = false,
                        });
                    };

                    var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);

                    await CreateDocs(store, docsCount, ids);
                    await PopulateDocsWithRandomAttachments(store, identifier, size, ids, attachmentsPerDoc);

                    var id = ids.First().Id;
                    using (var session = store.OpenSession())
                    {
                        var profileStream = new MemoryStream([1, 2, 3]);
                        var remoteParams = new RemoteAttachmentParameters(removedIdentifier, DateTime.UtcNow.AddMinutes(3));
                        session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("att-bad-identifier.png", profileStream)
                        {
                            RemoteParameters = remoteParams,
                            ContentType = "image/png"
                        });

                        session.SaveChanges();
                        using AttachmentResult a = await store.Operations.SendAsync(new GetAttachmentOperation(id, "att-bad-identifier.png", AttachmentType.Document, null));

                        Attachments.Add(new RemoteAttachment()
                        {
                            Name = a.Details.Name,
                            DocumentId = id,
                            Stream = profileStream,
                            ContentType = a.Details.ContentType,
                            Hash = a.Details.Hash,
                            RemoteParameters = remoteParams
                        });
                    }

                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    GetStorageAttachmentsMetadataFromAllAttachments(database);
                    Assert.Equal(attachmentsCount + 1, Attachments.Count);

                    ModifyRemoteAttachmentsConfig = null;
                    identifier = await PutRemoteAttachmentsConfiguration(store, Settings); // rewrite the config without 2nd identifier

                    List<string> myExceptions = new List<string>();
                    database.RemoteAttachmentsSender.ForTestingPurposesOnly().BeforeEndOfTheBatch = exceptions =>
                    {
                        myExceptions.Add(exceptions);
                    };

                    // move in time & start remote
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                    var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
                    Assert.Equal(1, cloudObjects.Count);

                    var stats = store.Maintenance.Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(1, stats.CountOfRemoteAttachments);
                    Assert.Equal(2, stats.CountOfAttachments);

                    Assert.NotNull(myExceptions);
                    Assert.Equal(0, myExceptions.Count);

                    using AttachmentResult bad = await store.Operations.SendAsync(new GetAttachmentOperation(id, "att-bad-identifier.png", AttachmentType.Document, null));

                    Assert.Equal("att-bad-identifier.png", bad.Details.Name);
                    Assert.Equal(RemoteAttachmentFlags.None, bad.Details.RemoteParameters.Flags);
                    Assert.NotNull(bad.Details.RemoteParameters.At);

                    var goodName = Attachments.First(x => x.Name != "att-bad-identifier.png").Name;
                    using AttachmentResult good = await store.Operations.SendAsync(new GetAttachmentOperation(id, goodName, AttachmentType.Document, null));

                    Assert.Equal(goodName, good.Details.Name);
                    Assert.Equal(RemoteAttachmentFlags.Remote, good.Details.RemoteParameters.Flags);
                    Assert.NotNull(good.Details.RemoteParameters.At);

                }
            }
        }

        [AmazonS3RetryFact]
        public async Task RemoteAttachmentWithoutDestinationShouldBeSkipped()
        {
            int attachmentsCount=1;
            int size = 3;
            await using (var holder = CreateCloudSettings())
            {
                using (var store = GetDocumentStore())
                {
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();

                    var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);
                    await CreateDocs(store, docsCount, ids);
                    await PopulateDocsWithRandomAttachments(store, identifier, size, ids, attachmentsPerDoc);

                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    GetStorageAttachmentsMetadataFromAllAttachments(database);
                    Assert.Equal(attachmentsCount, Attachments.Count);

                    var identifier2 = await PutRemoteAttachmentsConfiguration(store, Settings, id: "new-identifier");

                    // move in time & start remote
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                    var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, 0, 15_000);
                    Assert.Equal(0, cloudObjects.Count);

                    var stats = store.Maintenance.Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(0, stats.CountOfRemoteAttachments);
                    Assert.Equal(1, stats.CountOfAttachments);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanPutAttachmentThenAddRemoteConfigAndNewAttachment(bool remoteExistingAttachments)
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
                await store.Operations.SendAsync(new PutAttachmentOperation(id, new StoreAttachmentParameters("test.png", profileStream) { /*RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)),*/ ContentType = "image/png" }));

                var res = await store.Operations.SendAsync(new GetAttachmentOperation(id, "test.png", AttachmentType.Document, null));
                Assert.Equal("test.png", res.Details.Name);
                Assert.Null(res.Details.RemoteParameters);
                await using (var holder = CreateCloudSettings())
                {
                    var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);

                    using var profileStream2 = new MemoryStream([3, 2, 1]);

                    await store.Operations.SendAsync(new PutAttachmentOperation(id, new StoreAttachmentParameters("test2.png", profileStream2) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" }));
                    var res2 = await store.Operations.SendAsync(new GetAttachmentOperation(id, "test2.png", AttachmentType.Document, null));
                    Assert.Equal("test2.png", res2.Details.Name);

                    Assert.Equal(RemoteAttachmentFlags.None, res2.Details.RemoteParameters.Flags);
                    Assert.NotNull(res2.Details.RemoteParameters.At);

                    var res3 = await store.Operations.SendAsync(new GetAttachmentOperation(id, "test.png", AttachmentType.Document, null));
                    Assert.Equal("test.png", res3.Details.Name);
                    Assert.Null(res3.Details.RemoteParameters);

                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);

                    if (remoteExistingAttachments)
                    {
                        profileStream.Position = 0;
                        await store.Operations.SendAsync(new PutAttachmentOperation(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" }));
                        database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                        await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                        await GetBlobsFromCloudAndAssertForCount(Settings, 2, 15_000);

                        var res4 = await store.Operations.SendAsync(new GetAttachmentOperation(res3.Details.DocumentId, res3.Details.Name, AttachmentType.Document, null));
                        Assert.Equal("test.png", res4.Details.Name);
                        Assert.Equal(RemoteAttachmentFlags.Remote, res4.Details.RemoteParameters.Flags);
                        Assert.NotNull(res4.Details.RemoteParameters.At);
                    }
                    else
                    {
                        database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                        await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                        await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);

                        var res4 = await store.Operations.SendAsync(new GetAttachmentOperation(res3.Details.DocumentId, res3.Details.Name, AttachmentType.Document, null));
                        Assert.Equal("test.png", res4.Details.Name);
                        Assert.Null(res4.Details.RemoteParameters);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanCrudAttachmentWhenHaveRemoteAttachment()
        {
            var attachmentsCount = 1;
            var size = 3;
            await using (var holder = CreateCloudSettings())
            {
                using (var store = GetDocumentStore())
                {
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();

                    var identifier = await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc, null);

                    var data = Attachments.FirstOrDefault();
                    Assert.NotNull(data);

                    using (var profileStream = new MemoryStream(new byte[] { 3, 2, 2 }))
                    {
                        // remote of this attachment should happen in baseline + 40 mins
                        var remoteParams = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3));
                        var result = store.Operations.Send(new PutAttachmentOperation(data.DocumentId, new StoreAttachmentParameters("profile.png", profileStream) { RemoteParameters = remoteParams, ContentType = "image/png" }));
                        Assert.Equal("profile.png", result.Name);
                        Assert.Equal(data.DocumentId, result.DocumentId);
                        Assert.Equal("image/png", result.ContentType);
                        Assert.Equal("bucfDXJ3eWRJYpgggJrnskJtMuMyFohjO2GHATxTmUs=", result.Hash);
                        Assert.Equal(3, result.Size);
                        Assert.NotNull(result.RemoteParameters);
                        Assert.Equal(remoteParams.Identifier, result.RemoteParameters.Identifier);
                        Assert.Equal(remoteParams.At, result.RemoteParameters.At);
                        Assert.Equal(RemoteAttachmentFlags.None, result.RemoteParameters.Flags);
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

                        // this would put a Delete remote attachment task in the queue, that should happen immediately
                        session.Advanced.Attachments.Delete(doc, data.Name);
                        session.SaveChanges();
                    }

                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    var key = string.Empty;
                    GetToRemoteAttachmentsCount(database, 1, infos =>
                    {
                        if (infos == null)
                            return;

                        key = infos.First().LowerId.ToString();
                    });

                    await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);

                    Assert.Equal("orders/0", key);

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

                    GetToRemoteAttachmentsCount(await Databases.GetDocumentDatabaseInstanceFor(Server, store), 0);
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanPutInRemoteAttachmentAndDeleteTheDocBeforeUploadingToRemote()
        {
            await using (var holder = CreateCloudSettings())
            {
                using (var store = GetDocumentStore())
                {

                    //TODO: egor test with this config will make exceptions (in _threads.exception need to add test for that !
                    //await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(new RemoteAttachmentsConfiguration()
                    //{
                    //    S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                    //    Disabled = false,
                    //    RemotePeriods = new Dictionary<string, TimeSpan>() { { "Orders", TimeSpan.FromMinutes(3) }, { "Products", TimeSpan.FromMilliseconds(322228) } },
                    //    CheckFrequencyInSec = 1000
                    //}));
                    var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);
                    var docId = "Orders/3";
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order { Id = docId, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/2", Company = $"Companies/2" });

                        await session.SaveChangesAsync();
                    }

                    using var profileStream = new MemoryStream([1, 2, 3]);
                    await store.Operations.SendAsync(
                        new PutAttachmentOperation(docId,
                            new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" }));
                    var res = await store.Operations.SendAsync(new GetAttachmentOperation(docId, "test.png", AttachmentType.Document, null));
                    Assert.Equal("test.png", res.Details.Name);

                    DocumentDatabase database = null;
         
                    var key = string.Empty;
                    database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    GetToRemoteAttachmentsCount(database, 1, infos =>
                    {
                        if (infos == null)
                            return;

                        key = infos.First().LowerId.ToString();
                    });

                    Assert.Equal("orders/3", key);

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

                    GetToRemoteAttachmentsCount(database, 0);
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanPutInRemoteAttachmentAndDeleteTheDocBeforeUploadingToRemoteInCluster()
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
                    //await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(new RemoteAttachmentsConfiguration()
                    //{
                    //    S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                    //    Disabled = false,
                    //    RemotePeriods = new Dictionary<string, TimeSpan>() { { "Orders", TimeSpan.FromMinutes(3) }, { "Products", TimeSpan.FromMilliseconds(322228) } },
                    //    CheckFrequencyInSec = 1000
                    //}));
                    var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);
                    var docId = "Orders/3";
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order { Id = docId, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/2", Company = $"Companies/2" });

                        await session.SaveChangesAsync();
                    }

                    using var profileStream = new MemoryStream([1, 2, 3]);
                    await store.Operations.SendAsync(

                    new PutAttachmentOperation(docId,
                        new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" }));
                    var res = await store.Operations.SendAsync(new GetAttachmentOperation(docId, "test.png", AttachmentType.Document, null));
                    Assert.Equal("test.png", res.Details.Name);
                    Assert.Equal(true, await WaitForChangeVectorInClusterAsync(srcNodes.Servers, srcDb));

                    DocumentDatabase database = null;
                    foreach (var node in srcRaft.Nodes)
                    {
                        database = await Databases.GetDocumentDatabaseInstanceFor(node, store);

                        var key = string.Empty;
                        GetToRemoteAttachmentsCount(database, 1, infos =>
                        {
                            var arr = infos?.ToArray();
                            if (arr == null || arr.Length == 0)
                                return;

                            key = arr.First().LowerId.ToString();
                        });
                        Assert.Equal("orders/3", key);
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
                        GetToRemoteAttachmentsCount(database, 0);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task DeletingDocumentWithRemoteAttachmentShouldKeepRemoteAttachmentByDefault()
        {
            var attachmentsCount = 1;
            var size = 3;
            await using (var holder = CreateCloudSettings())
            {
                using (var store = GetDocumentStore())
                {
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();

                    var identifier = await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc, null);

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
                    GetToRemoteAttachmentsCount(database, 0);
                    await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task DeletingDocumentWithRemoteAttachmentShouldKeepRemoteAttachmentByDefaultInCluster()
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
                            GetToRemoteAttachmentsCount(database, 0);
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
        public async Task DeletingAttachmentShouldRemoveFromRemoteTree()
        {
            await using (var holder = CreateCloudSettings())
            {
                int docsCount = GetDocsAndAttachmentCount(1, out int attachmentsPerDoc);
                var ids = new List<(string Id, string Collection)>();
                List<string> collections = null;

                using (var store = GetDocumentStore())
                {
                    var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections);
                    await CreateDocs(store, docsCount, ids, collections);
                    await PopulateDocsWithRandomAttachments(store, identifier,3, ids, attachmentsPerDoc);

                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    GetStorageAttachmentsMetadataFromAllAttachments(database);
                    Assert.Equal(1, Attachments.Count);

                    GetToRemoteAttachmentsCount(database, 1);
                    var attachment = Attachments[0];
                    await store.Operations.SendAsync(new DeleteAttachmentOperation(attachment.DocumentId, attachment.Name));
                    GetToRemoteAttachmentsCount(database, 0);
                }
            }
        }

        public static void GetToRemoteAttachmentsCount(DocumentDatabase database, int expected, Action<Queue<DocumentExpirationInfo>> action = null)
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

                var options = new BackgroundWorkParameters(context, DateTime.MaxValue, dbRecord.Topology, nodeTag, int.MaxValue);
                // need to sort the list so current checked node is first in topology, since only the "first topology node is checked in GetDocuments() method
                options.DatabaseTopology.Members = options.DatabaseTopology.Members.OrderByDescending(x => x == nodeTag).ToList();

                var totalCount = 0;

                var expired = database.DocumentsStorage.AttachmentsStorage.RemoteAttachmentsStorage.GetDocuments(options, ref totalCount, out _,
                    CancellationToken.None);

                Assert.Equal(expected, totalCount);

                action?.Invoke(expired);
            }
        }

        [AmazonS3RetryTheory]
        [InlineData(1, new byte[] { 1, 2, 3, 4, 5 })]
    //    [InlineData(5, new byte[] { 1, 2, 3, 4, 5 })]
        public async Task CanRemoteIdenticalAttachmentOnTwoDocuments_OnlyOneInCloud_AndGetFromBoth(int count, byte[] arr)
        {
            // Pseudocode:
            // 1. Create cloud settings and document store.
            // 2. Put remote attachments configuration.
            // 3. Create two documents.
            // 4. Add the same attachment (same content, name, content-type) to both documents.
            // 5. Remote both attachments (move time forward and trigger upload remote cloud storage).
            // 6. Assert only one blob in cloud.
            // 7. Assert we can get the remote attachment from both documents.

            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);

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
                            RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)),
                            ContentType = contentType
                        }));
                        stream1.Position = 0;
                    }

                }

                // Move time forward and remote attachments
                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                // Assert only one blob in cloud
                var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);


                using (var ms1 = new MemoryStream())
                {
                    var remote1 = await store.Operations.SendAsync(new GetAttachmentOperation($"Orders/0", $"shared_0.png", AttachmentType.Document, null));
                    await remote1.Stream.CopyToAsync(ms1);
                    Assert.Equal(attachmentBytes, ms1.ToArray());
                    Assert.Equal($"shared_0.png", remote1.Details.Name);
                    Assert.Equal(contentType, remote1.Details.ContentType);
                    Assert.Equal(RemoteAttachmentFlags.Remote, remote1.Details.RemoteParameters.Flags);
                    for (int i = 1; i < count; i++)
                    {
                        var remote2 = await store.Operations.SendAsync(new GetAttachmentOperation($"Orders/{i}", $"shared_{i}.png", AttachmentType.Document, null));

                        Assert.Equal($"shared_{i}.png", remote2.Details.Name);
                        Assert.Equal(contentType, remote2.Details.ContentType);
                        Assert.Equal(RemoteAttachmentFlags.Remote, remote2.Details.RemoteParameters.Flags);

                        ms1.Position = 0;

                        // Compare content
                        using (var ms2 = new MemoryStream())
                        {

                            await remote2.Stream.CopyToAsync(ms2);
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
        public async Task CanUploadRemoteAttachmentToS3AndGet(int attachmentsCount, int size)
        {
            await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(64, 3)]
        //[InlineData(128, 3)]
        //TODO: egor in the future need optimizations so I can do a lot faster[InlineData(256)]
        public async Task CanUploadRemoteAttachmentFromDifferentCollectionsToS3AndGet(int attachmentsCount, int size)
        {
            var collections = new List<string> { "Orders", "Products" };
            Assert.True(attachmentsCount > 32, "this test meant to have more than 32 attachments so we will have more than one document");
            await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, collections: collections);
        }

        [AmazonS3RetryTheory]
        [InlineData(64, 3)]
        public async Task CanUploadRemoteAttachmentFromDifferentCollectionsToS3AndDelete(int attachmentsCount, int size)
        {
            Assert.True(attachmentsCount > 32, "this test meant to have more than 32 attachments so we will have more than one document");
            var collections = new List<string> { "Orders", "Products" };
            await CanUploadRemoteAttachmentToCloudAndDeleteInternal(attachmentsCount, size, collections);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRemoteAttachmentToS3AndDelete(int attachmentsCount, int size)
        {
            await CanUploadRemoteAttachmentToCloudAndDeleteInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(16, 3, 4)]
        public async Task CanUploadRemoteAttachmentToS3AndDeleteInTheSameTime(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            await CanUploadRemoteAttachmentToCloudAndDeleteInTheSameTimeInternal(attachmentsCount, size, attachmentsPerDoc);
        }

        [AmazonS3RetryFact]
        public async Task ShouldAddRemoteAtToAttachmentMetadataUsingS3Configuration()
        {
            await ShouldAddRemoteAtToAttachmentMetadataInternal();
        }

        [AmazonS3RetryFact]
        public async Task ShouldNotThrowUsingRegularAttachmentsApiOnRemoteAttachmentToS3()
        {
            await ShouldNotThrowUsingRegularAttachmentsApiOnRemoteAttachmentInternal();
        }

        [AmazonS3RetryTheory]
        [InlineData(3, 3, 1)]
        [InlineData(16, 3, 4)]
        public async Task CanUploadRemoteAttachmentsFromDifferentCollectionsToS3AndGetInBulk(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            var collections = new List<string> { "Orders", "Products" };
            await CanUploadRemoteAttachmentsToCloudAndGetInBulkInternal(attachmentsCount, size, attachmentsPerDoc, collections);
        }

        [AmazonS3RetryTheory]
        [InlineData(3, 3, 1)]
        [InlineData(16, 3, 4)]
        public async Task CanUploadRemoteAttachmentsToS3AndGetInBulk(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            await CanUploadRemoteAttachmentsToCloudAndGetInBulkInternal(attachmentsCount, size, attachmentsPerDoc);
        }
        [AmazonS3RetryTheory]
        [InlineData(3, 3, 1)]
        [InlineData(16, 3, 4)]
        public async Task CanUploadRemoteAttachmentsToS3AndDeleteInBulk(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            await CanUploadRemoteAttachmentsToCloudAndDeleteInBulkInternal(attachmentsCount, size, attachmentsPerDoc);
        }

        [AmazonS3RetryFact]
        public async Task CanUploadRemoteAttachmentToS3IfItAlreadyExists_ShouldNotOverwrite()
        {
            await CanUploadRemoteAttachmentToCloudIfItAlreadyExists_ShouldNotOverwriteInternal(overwriteWithDummy: false);
        }

        [AmazonS3RetryFact]
        public async Task CanUploadRemoteAttachmentToS3IfItAlreadyExists_ShouldOverwriteIfBroken()
        {
            await CanUploadRemoteAttachmentToCloudIfItAlreadyExists_ShouldNotOverwriteInternal(overwriteWithDummy: true);
        }

        [AmazonS3RetryFact]
        public async Task UploadRemoteAttachmentToS3ThenManuallyDeleteAndGetShouldThrow()
        {
            await UploadRemoteAttachmentToCloudThenManuallyDeleteAndGetShouldThrowInternal();
        }

        [AmazonS3RetryFact]
        public async Task CanDeleteRemoteAttachmentFromS3WhenItsNotExistsInS3()
        {
            await CanDeleteRemoteAttachmentFromCloudWhenItsNotExistsInCloudInternal();
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRemoteAttachmentToS3InClusterAndGet(int attachmentsCount, int size)
        {
            await CanUploadRemoteAttachmentToCloudInClusterAndGetInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRemoteAttachmentToS3InClusterAndGet2(int attachmentsCount, int size)
        {
            await CanUploadRemoteAttachmentToCloudInClusterAndGet2Internal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRemoteAttachmentToS3InClusterAndDelete(int attachmentsCount, int size)
        {
            await CanUploadRemoteAttachmentToCloudInClusterAndDeleteInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRemoteAttachmentToS3FromBackupAndGet(int attachmentsCount, int size)
        {

            await CanUploadRemoteAttachmentToCloudFromBackupAndGet(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanExternalReplicateRemoteAttachmentAndThenUploadToS3AndGet(int attachmentsCount, int size)
        {
            await CanExternalReplicateRemoteAttachmentAndThenUploadToCloudAndGet(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanExternalReplicateRemoteAttachmentAndThenHaveNoRelevantDestination(int attachmentsCount, int size)
        {
            await CanExternalReplicateRemoteAttachmentAndThenUploadToCloudAndGet2(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task AddRemoteAttachmentThenExternalReplicateToDatabaseWithoutRemoteConfig(int attachmentsCount, int size)
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await using (var holder = CreateCloudSettings())
                {
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();
                    var identifier = await PutRemoteAttachmentsConfiguration(store1, Settings);
                    await CreateDocs(store1, docsCount, ids);
                    await PopulateDocsWithRandomAttachments(store1, identifier, size, ids, attachmentsPerDoc);

                    await SetupReplicationAsync(store1, store2);
                    await EnsureReplicatingAsync(store1, store2);

                    var database2 = (await GetDocumentDatabaseInstanceForAsync(store2.Database));

                    // I create new settings for destination database, so each db upload to different folder
                    var settings = Etl.GetS3Settings(nameof(RemoteAttachments), $"{store2.Database}-{Guid.NewGuid()}").ToRemoteAttachmentsS3Settings();
                    GetStorageAttachmentsMetadataFromAllAttachments(database2, settings);

                    Assert.Equal(attachmentsCount, Attachments.Count);

                    GetToRemoteAttachmentsCount(database2, attachmentsCount);

                    try
                    {
                        await PutRemoteAttachmentsConfiguration(store2, settings);
                        // move in time & start remote
                        database2.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                        await database2.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                        var cloudObjects = await GetBlobsFromCloudAndAssertForCount(settings, attachmentsCount, 15_000);
                        await GetBlobsFromCloudAndAssertForCount(Settings, 0, 15_000);
                        await AssertAllRemoteAttachments(store2, cloudObjects, size, identifier);

                        var database1 = (await GetDocumentDatabaseInstanceForAsync(store1.Database));

                        using (database1.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var attachments = database1.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();
                            Assert.Equal(attachmentsCount, attachments.Count);
                            Assert.All(attachments, attachment => Assert.True(attachment.RemoteParameters.Flags == RemoteAttachmentFlags.None));
                        }

                        // replicate remote attachments to source
                        await SetupReplicationAsync(store2, store1);
                        await EnsureReplicatingAsync(store2, store1);


                        using (database1.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var attachments = database1.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();
                            Assert.Equal(attachmentsCount, attachments.Count);

                            // update the remote attachments configuration to be same as destination
                            await PutRemoteAttachmentsConfiguration(store1, settings);
                          
                            await Assert.AllAsync(attachments, async attachment =>
                            {
                                Assert.True(attachment.RemoteParameters.Flags == RemoteAttachmentFlags.Remote);
                                // we cannot receive it using source remote attachment configuration
                                var a = Attachments.FirstOrDefault(x => x.Key == attachment.Key);
                                Assert.NotNull(a);
                                attachment.Stream = a.Stream;

                                // this sends GetAttachmentOperation and compares the result
                                await GetAndCompareRemoteAttachment(store2, a.DocumentId, attachment.Name, attachment.Base64Hash.ToString(), attachment.ContentType,
                                    (MemoryStream)attachment.Stream, size, identifier, RemoteAttachmentFlags.Remote);
                            });

                            // we replicated the remote attachment and overwrite the regular one, the replication should have deleted the stream
                            var streams = database1.DocumentsStorage.AttachmentsStorage.GetAllAttachmentsStreamHashes(context).ToList();
                            Assert.Equal(0, streams.Count);

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
        [InlineData(64, 3)]
        public async Task ExternalReplicationOfRemoteAttachmentToExternalDatabaseShouldNotUnwrap(int attachmentsCount, int size)
        {
            using (var store1 = GetDocumentStore())
            {
                await using (var holder = CreateCloudSettings())
                {
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();
                    var identifier = await PutRemoteAttachmentsConfiguration(store1, Settings);
                    await CreateDocs(store1, docsCount, ids);
                    await PopulateDocsWithRandomAttachments(store1, identifier, size, ids, attachmentsPerDoc);

                    var database = (await GetDocumentDatabaseInstanceForAsync(store1.Database));

                    // move in time & start remote
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                    var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
                    await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);

                    GetStorageAttachmentsMetadataFromAllAttachments(database, Settings);
                    await AssertAllRemoteAttachments(store1, cloudObjects, size, identifier);

                    using var store2 = GetDocumentStore();
                    await SetupReplicationAsync(store1, store2);
                    await EnsureReplicatingAsync(store1, store2);

                    foreach (var remote in Attachments)
                    {
                        var e = await Assert.ThrowsAsync<RavenException>(async () =>
                            await store2.Operations.SendAsync(new GetAttachmentOperation(remote.DocumentId, remote.Name, AttachmentType.Document, null)));
                        Assert.Contains($"Cannot perform 'GetAttachmentOperation' for remote attachment '{remote.Name}' on document '{remote.DocumentId}' because the database does not have a RemoteAttachmentsConfiguration configured.", e.Message);

                    }

                    await PutRemoteAttachmentsConfiguration(store2, Settings);
                    foreach (var remote in Attachments)
                    {
                        Assert.Contains(cloudObjects, x => x.FullPath.Contains(remote.RemoteKey));
                        remote.Stream.Position = 0;
                        var attachment = await store2.Operations.SendAsync(new GetAttachmentOperation(remote.DocumentId, remote.Name, AttachmentType.Document, null));
                        Assert.NotNull(attachment);
                        Assert.Equal(remote.Hash, attachment.Details.Hash);
                        Assert.Equal(remote.ContentType, attachment.Details.ContentType);
                        Assert.Equal(remote.Name, attachment.Details.Name);
                        Assert.Equal(size, attachment.Details.Size);
                        Assert.Equal(RemoteAttachmentFlags.Remote, attachment.Details.RemoteParameters.Flags);
                        Assert.NotNull(attachment.Details.RemoteParameters.At);
                        using var remoteStream = new MemoryStream();
                        await attachment.Stream.CopyToAsync(remoteStream);
                        remote.Stream.Position = 0;
                        remoteStream.Position = 0;
                        await AttachmentsStreamTests.CompareStreamsAsync(remote.Stream, remoteStream);
                    }
                }
            }
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task AddRemoteAttachmentThenExternalReplicateToDatabaseWithoutRemoteConfig2(int attachmentsCount, int size)
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


                    GetStorageAttachmentsMetadataFromAllAttachments(database2, Settings);

                    Assert.Equal(attachmentsCount, Attachments.Count);

                    // I don't have remote attachments config. but as in other background task features, I populate the remote attachment tree after replicating it
                    GetToRemoteAttachmentsCount(database2, attachmentsCount);

                    try
                    {
                        var identifier2 = await PutRemoteAttachmentsConfiguration(store2, Settings);
                        // move in time & start remote
                        database2.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                        await database2.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                        var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
                        await AssertAllRemoteAttachments(store2, cloudObjects, size, identifier2);

                        // on store 1 the attachments are still not remote, so we cannot get them
                        await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);

                        var database1 = (await GetDocumentDatabaseInstanceForAsync(store1.Database));

                        using (database1.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var attachments = database1.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();
                            Assert.Equal(attachmentsCount, attachments.Count);
                            Assert.All(attachments, attachment => Assert.True(attachment.RemoteParameters.Flags == RemoteAttachmentFlags.None));
                        }

                        // replicate remote attachments to source
                        await SetupReplicationAsync(store2, store1);
                        await EnsureReplicatingAsync(store2, store1);

                        using (database1.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var attachments = database1.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();
                            Assert.Equal(attachmentsCount, attachments.Count);

                            await Assert.AllAsync(attachments, async attachment =>
                            {
                                Assert.True(attachment.RemoteParameters.Flags == RemoteAttachmentFlags.Remote);
                                // we cannot receive it using source remote attachment configuration
                                var a = Attachments.FirstOrDefault(x => x.Key == attachment.Key);
                                Assert.NotNull(a);
                                attachment.Stream = a.Stream;

                                // this sends GetAttachmentOperation and compares the result
                                await GetAndCompareRemoteAttachment(store1, a.DocumentId, attachment.Name, attachment.Base64Hash.ToString(), attachment.ContentType, (MemoryStream)attachment.Stream, size, identifier1, RemoteAttachmentFlags.Remote);
                            });

                            // update the remote attachments configuration to be same as destination
                            // we still didn't remote the attachments on source, so we cannot get them
                            await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);

                            // move in time & start remote
                            database1.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                            await database1.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                            await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
                            await Assert.AllAsync(attachments, async attachment =>
                            {
                                // we loaded remote attachment from storage it doesn't have stream, so we populate it from the one we saved in test, so we can compare
                                var a = Attachments.FirstOrDefault(x => x.Key == attachment.Key);
                                Assert.NotNull(a);
                                attachment.Stream = a.Stream;

                                // this sends GetAttachmentOperation and compares the result
                                await GetAndCompareRemoteAttachment(store1, a.DocumentId, attachment.Name, attachment.Base64Hash.ToString(), attachment.ContentType, (MemoryStream)attachment.Stream, size, identifier1);
                            });
                        }
                    }
                    finally
                    {
                        await DeleteObjects(Settings);
                    }
                }
            }
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        public async Task CanBackupRemoteAttachments(int attachmentsCount, int size)
        {
            await CanBackupRemoteAttachmentsInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanExportImportWithRemoteAttachment(int attachmentsCount, int size)
        {
            await CanExportImportWithRemoteAttachmentInternal(attachmentsCount, size);
        }


        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanIndexWithRemoteAttachment(int attachmentsCount, int size)
        {
            await CanIndexWithRemoteAttachmentInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanEtlWithRemoteAttachmentAndRemoteOnDestination(int attachmentsCount, int size)
        {
            await CanEtlWithRemoteAttachmentAndRemoteOnDestinationInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanEtlRemoteAttachmentsToDestination(int attachmentsCount, int size)
        {
            await CanEtlRemoteAttachmentsToDestinationInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(32, 3)]
        public async Task CanEtlRemoteAttachmentsToDestination2(int attachmentsCount, int size)
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
                        Transforms = { new Transformation { Name = "S1", Collections = { "Orders" }, Script = @"

var meta = this['@metadata'];
if (!meta || !meta['@attachments'])
    return;
var doc = loadToOrders(this);
for (var i = 0; i < meta['@attachments'].length; i++) {
    var att = meta['@attachments'][i];
    
    doc.addAttachment(att.Name, loadAttachment(att.Name));
}

" } }
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

        [AmazonS3RetryTheory]
        [InlineData(1, 3, true)]
        [InlineData(1, 3, false)]
        [InlineData(64, 3, true)]
        [InlineData(64, 3, false)]
        public async Task CanEtlDeletedRemoteAttachmentsToDestination(int attachmentsCount, int size, bool remoteOnReplica)
        {
            await using (var holder = CreateCloudSettings())
            {
                int docsCount = RemoteAttachmentsHolderBase.GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
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
                    if (remoteOnReplica)
                    {
                        var identifier11 = await PutRemoteAttachmentsConfiguration(store, Settings);
                        await CreateDocs(store, docsCount, ids);
                        await PopulateDocsWithRandomAttachments(store, identifier11, size, ids, attachmentsPerDoc);
                    }
                    else
                    {
                        await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, store, docsCount, ids, attachmentsPerDoc);
                    }
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

                    await etlDone.WaitAsync(TimeSpan.FromSeconds(15));
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
                    var identifier2 = await PutRemoteAttachmentsConfiguration(replica, Settings);

                    if (remoteOnReplica)
                    {
                        await AssertGetRemoteAttachmentsInBulk(replica, size, identifier2, RemoteAttachmentFlags.None);
                        // move in time & start remote
                        replicaDb.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                        await replicaDb.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                        var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
                        var database2 = await Databases.GetDocumentDatabaseInstanceFor( replica);
                        GetStorageAttachmentsMetadataFromAllAttachments(database2);
                        await AssertAllRemoteAttachments(replica, cloudObjects, size, identifier2);

                        var stats = replica.Maintenance.Send(new GetDetailedStatisticsOperation());
                        Assert.Equal(attachmentsCount, stats.CountOfRemoteAttachments);
                    }

                    await AssertGetRemoteAttachmentsInBulk(replica, size, identifier2, RemoteAttachmentFlags.Remote);

                    foreach (var attachment in Attachments)
                    {
                        await store.Operations.SendAsync(new DeleteAttachmentOperation(attachment.DocumentId, attachment.Name));
                    }

                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount); // we dont delete the remote attachments from cloud, so we still have them

                    etlDone = Etl.WaitForEtlToComplete(store);
                    await etlDone.WaitAsync(TimeSpan.FromSeconds(15));

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

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanExternalReplicateDeletedRemoteAttachmentsToDestination(int attachmentsCount, int size)
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await using (var holder = CreateCloudSettings())
                {
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();
                    var identifier = await PutRemoteAttachmentsConfiguration(store1, Settings);
                    await CreateDocs(store1, docsCount, ids);
                    await PopulateDocsWithRandomAttachments(store1, identifier, size, ids, attachmentsPerDoc);

                    await SetupReplicationAsync(store1, store2);
                    await EnsureReplicatingAsync(store1, store2);

                    var database2 = (await GetDocumentDatabaseInstanceForAsync(store2.Database));

                    // I create new settings for destination database, so each db upload to different folder
                    var settings = Etl.GetS3Settings(nameof(RemoteAttachments), $"{store2.Database}-{Guid.NewGuid()}").ToRemoteAttachmentsS3Settings();
                    GetStorageAttachmentsMetadataFromAllAttachments(database2, settings);

                    Assert.Equal(attachmentsCount, Attachments.Count);

                    // I don't have remote attachments config. but as in other background task features, I populate the remote attachment tree after replicating it
                    GetToRemoteAttachmentsCount(database2, attachmentsCount);

                    var database = await Databases.GetDocumentDatabaseInstanceFor(store1);
                    GetStorageAttachmentsMetadataFromAllAttachments(database);
                    Assert.Equal(attachmentsCount, Attachments.Count);

                    // move in time & start remote
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                    var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);

                    await AssertAllRemoteAttachments(store1, cloudObjects, size, identifier);

                    using (database2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var attachments = database2.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context).ToList();
                        Assert.Equal(attachmentsCount, attachments.Count);

                        await Assert.AllAsync(attachments, async attachment =>
                        {
                            Assert.True(attachment.RemoteParameters.Flags == RemoteAttachmentFlags.Remote);
                            var a = Attachments.FirstOrDefault(x => x.Key == attachment.Key);
                            Assert.NotNull(a);

                            var e = await Assert.ThrowsAsync<RavenException>(async () => await store2.Operations.SendAsync(new GetAttachmentOperation(a.DocumentId, attachment.Name, AttachmentType.Document, null)));
                            Assert.Contains($"Cannot perform 'GetAttachmentOperation' for remote attachment '{attachment.Name}' on document '{a.DocumentId}' because the database does not have a RemoteAttachmentsConfiguration configured.", e.Message);
                        });

                        await PutRemoteAttachmentsConfiguration(store2, Settings);
                        await Assert.AllAsync(attachments, async attachment =>
                        {
                            Assert.True(attachment.RemoteParameters.Flags == RemoteAttachmentFlags.Remote);
                            // we cannot receive it using source remote attachment configuration
                            var a = Attachments.FirstOrDefault(x => x.Key == attachment.Key);
                            Assert.NotNull(a);
                            attachment.Stream = a.Stream;

                            // this sends GetAttachmentOperation and compares the result
                            await GetAndCompareRemoteAttachment(store2, a.DocumentId, attachment.Name, attachment.Base64Hash.ToString(), attachment.ContentType, (MemoryStream)attachment.Stream, size, identifier, RemoteAttachmentFlags.Remote);
                        });
                    }

                    var stats = store1.Maintenance.Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(attachmentsCount, stats.CountOfRemoteAttachments);

                    var stats2 = store2.Maintenance.Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(attachmentsCount, stats2.CountOfRemoteAttachments);
                    Assert.Equal(attachmentsCount, stats2.CountOfAttachments);

                    foreach (var attachment in Attachments)
                    {
                        await store1.Operations.SendAsync(new DeleteAttachmentOperation(attachment.DocumentId, attachment.Name));
                    }

                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);

                    await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                    // we don't delete the remote attachments from cloud, so we still have them
                    await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount);

                    await EnsureReplicatingAsync(store1, store2);

                    var stats11 = store1.Maintenance.Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(0, stats11.CountOfRemoteAttachments);
                    Assert.Equal(0, stats11.CountOfAttachments);
                    var stats22 = store2.Maintenance.Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(0, stats22.CountOfRemoteAttachments);
                    Assert.Equal(0, stats22.CountOfAttachments);
                }
            }
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        public async Task ExternalReplicateRemoteAttachmentFromRegularToShrdDatabase_ShouldThrow(int attachmentsCount, int size)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Egor, DevelopmentHelper.Severity.Minor, "Change/Remove this test when RavenDB-24148 is implemented");
            using (var regularStore = GetDocumentStore())
            using (var shardedStore = Sharding.GetDocumentStore())
            {
                await using (var holder = CreateCloudSettings())
                {
                    // Setup remote attachments configuration for regular store
                    var identifier = await PutRemoteAttachmentsConfiguration(regularStore, Settings);

                    // Create documents and attachments in regular store
                    int docsCount = GetDocsAndAttachmentCount(attachmentsCount, out int attachmentsPerDoc);
                    var ids = new List<(string Id, string Collection)>();
                    await CreateDocs(regularStore, docsCount, ids);
                    await PopulateDocsWithRandomAttachments(regularStore, identifier, size, ids, attachmentsPerDoc);

                    // Get database instance and remote attachments
                    var regularDatabase = await GetDocumentDatabaseInstanceFor(regularStore);

                    // Move time forward and remote attachments
                    regularDatabase.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await regularDatabase.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                    // Verify attachments are remote in cloud
                    var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, attachmentsCount, 15_000);
                    GetStorageAttachmentsMetadataFromAllAttachments(regularDatabase, Settings);
                    await AssertAllRemoteAttachments(regularStore, cloudObjects, size, identifier);

                    var mre = new AsyncManualResetEvent();
                    Exception ex = null;
                    await foreach (var db in Sharding.GetShardsDocumentDatabaseInstancesFor(shardedStore))
                    {
                        db.ReplicationLoader.ForTestingPurposesOnly().OnIncomingReplicationHandlerFailure += (exception) =>
                        {
                            if (exception is AggregateException ae)
                            {
                                exception = ae.InnerException;
                            }

                            if (exception == null)
                                return;

                            if (exception is NotSupportedException nse)
                            {
                                if (nse.Message.Contains("Replicating remote attachments to sharded database is not supported"))
                                {
                                    // yes
                                    ex = nse;
                                    mre.Set();
                                }
                            }
                        };
                    }

                    // Setup external replication from regular to sharded store
                    await SetupReplicationAsync(regularStore, shardedStore);

                    Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(15)));
                    Assert.NotNull(ex);
                    Assert.Equal(typeof(NotSupportedException), ex.GetType());
                    Assert.Equal("Replicating remote attachments to sharded database is not supported.", ex.Message);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanRemoteAttachmentThenRename()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);
                var docId = "Orders/1";
                var attachmentName = "test.png";
                var newAttachmentName = "renamed_test.png";

                // Create document and attachment
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = docId });
                    await session.SaveChangesAsync();
                }

                using var attachmentStream = new MemoryStream([1, 2, 3]);
                await store.Operations.SendAsync(new PutAttachmentOperation(docId, new StoreAttachmentParameters(attachmentName, attachmentStream)
                {
                    RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)),
                    ContentType = "image/png"
                }));

                // Verify attachment exists and has remote parameters
                var attachment = await store.Operations.SendAsync(new GetAttachmentOperation(docId, attachmentName, AttachmentType.Document, null));
                Assert.Equal(attachmentName, attachment.Details.Name);
                Assert.NotNull(attachment.Details.RemoteParameters);
                Assert.Equal(RemoteAttachmentFlags.None, attachment.Details.RemoteParameters.Flags);

                // Remote the attachment
                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                // Verify attachment is Remote
                await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
                var RemoteAttachment = await store.Operations.SendAsync(new GetAttachmentOperation(docId, attachmentName, AttachmentType.Document, null));
                Assert.Equal(RemoteAttachmentFlags.Remote, RemoteAttachment.Details.RemoteParameters.Flags);

                // Now try to rename the Remote attachment
                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Rename(docId, attachmentName, newAttachmentName);
                    session.SaveChanges();
                }

                // Verify the attachment was renamed
                using (var session = store.OpenSession())
                {
                    Assert.False(session.Advanced.Attachments.Exists(docId, attachmentName));
                    Assert.True(session.Advanced.Attachments.Exists(docId, newAttachmentName));
                }

                // Verify renamed attachment is still Remote and accessible
                var renamedAttachment = await store.Operations.SendAsync(new GetAttachmentOperation(docId, newAttachmentName, AttachmentType.Document, null));
                Assert.Equal(newAttachmentName, renamedAttachment.Details.Name);
                Assert.Equal(RemoteAttachmentFlags.Remote, renamedAttachment.Details.RemoteParameters.Flags);

                // Verify we can still read the content
                using var ms = new MemoryStream();
                await renamedAttachment.Stream.CopyToAsync(ms);
                Assert.Equal([1, 2, 3], ms.ToArray());
            }
        }

        [AmazonS3RetryFact]
        public async Task CanRemoteAttachmentThenCopy()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);
                var sourceDocId = "Orders/1";
                var destinationDocId = "Orders/2";
                var attachmentName = "test.png";
                var copiedAttachmentName = "copied_test.png";

                // Create documents and attachment
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = sourceDocId });
                    await session.StoreAsync(new Order { Id = destinationDocId });
                    await session.SaveChangesAsync();
                }

                using var attachmentStream = new MemoryStream([1, 2, 3]);
                await store.Operations.SendAsync(new PutAttachmentOperation(sourceDocId, new StoreAttachmentParameters(attachmentName, attachmentStream)
                {
                    RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)),
                    ContentType = "image/png"
                }));

                // Verify attachment exists and has remote parameters
                var attachment = await store.Operations.SendAsync(new GetAttachmentOperation(sourceDocId, attachmentName, AttachmentType.Document, null));
                Assert.Equal(attachmentName, attachment.Details.Name);
                Assert.NotNull(attachment.Details.RemoteParameters);
                Assert.Equal(RemoteAttachmentFlags.None, attachment.Details.RemoteParameters.Flags);

                // Remote the attachment
                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                // Verify attachment is Remote
                await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
                var RemoteAttachment = await store.Operations.SendAsync(new GetAttachmentOperation(sourceDocId, attachmentName, AttachmentType.Document, null));
                Assert.Equal(RemoteAttachmentFlags.Remote, RemoteAttachment.Details.RemoteParameters.Flags);

                // Now try to copy the Remote attachment
                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Copy(sourceDocId, attachmentName, destinationDocId, copiedAttachmentName);
                    session.SaveChanges();
                }

                // Verify both attachments exist
                using (var session = store.OpenSession())
                {
                    Assert.True(session.Advanced.Attachments.Exists(sourceDocId, attachmentName));
                    Assert.True(session.Advanced.Attachments.Exists(destinationDocId, copiedAttachmentName));
                }

                // Verify source attachment is still Remote
                var sourceAttachmentAfterCopy = await store.Operations.SendAsync(new GetAttachmentOperation(sourceDocId, attachmentName, AttachmentType.Document, null));
                Assert.Equal(RemoteAttachmentFlags.Remote, sourceAttachmentAfterCopy.Details.RemoteParameters.Flags);

                // Verify copied attachment has the same remote parameters
                var copiedAttachment = await store.Operations.SendAsync(new GetAttachmentOperation(destinationDocId, copiedAttachmentName, AttachmentType.Document, null));
                Assert.Equal(copiedAttachmentName, copiedAttachment.Details.Name);
                Assert.Equal(RemoteAttachmentFlags.Remote, copiedAttachment.Details.RemoteParameters.Flags);
                Assert.Equal(RemoteAttachment.Details.RemoteParameters.Identifier, copiedAttachment.Details.RemoteParameters.Identifier);

                // Verify we can still read the content from both
                using var sourceMs = new MemoryStream();
                await sourceAttachmentAfterCopy.Stream.CopyToAsync(sourceMs);
                Assert.Equal([1, 2, 3], sourceMs.ToArray());

                using var copiedMs = new MemoryStream();
                await copiedAttachment.Stream.CopyToAsync(copiedMs);
                Assert.Equal([1, 2, 3], copiedMs.ToArray());
            }
        }

        [AmazonS3RetryFact]
        public async Task CanRemoteAttachmentThenMove()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);
                var sourceDocId = "Orders/1";
                var destinationDocId = "Orders/2";
                var attachmentName = "test.png";
                var movedAttachmentName = "moved_test.png";

                // Create documents and attachment
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = sourceDocId });
                    await session.StoreAsync(new Order { Id = destinationDocId });
                    await session.SaveChangesAsync();
                }

                using var attachmentStream = new MemoryStream([1, 2, 3]);
                await store.Operations.SendAsync(new PutAttachmentOperation(sourceDocId, new StoreAttachmentParameters(attachmentName, attachmentStream)
                {
                    RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)),
                    ContentType = "image/png"
                }));

                // Verify attachment exists and has remote parameters
                var attachment = await store.Operations.SendAsync(new GetAttachmentOperation(sourceDocId, attachmentName, AttachmentType.Document, null));
                Assert.Equal(attachmentName, attachment.Details.Name);
                Assert.NotNull(attachment.Details.RemoteParameters);
                Assert.Equal(RemoteAttachmentFlags.None, attachment.Details.RemoteParameters.Flags);

                // Remote the attachment
                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                // Verify attachment is Remote
                await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
                var RemoteAttachment = await store.Operations.SendAsync(new GetAttachmentOperation(sourceDocId, attachmentName, AttachmentType.Document, null));
                Assert.Equal(RemoteAttachmentFlags.Remote, RemoteAttachment.Details.RemoteParameters.Flags);

                // Now try to move the Remote attachment
                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Move(sourceDocId, attachmentName, destinationDocId, movedAttachmentName);
                    session.SaveChanges();
                }

                // Verify attachment was moved
                using (var session = store.OpenSession())
                {
                    Assert.False(session.Advanced.Attachments.Exists(sourceDocId, attachmentName));
                    Assert.True(session.Advanced.Attachments.Exists(destinationDocId, movedAttachmentName));
                }

                // Verify moved attachment retains remote properties
                var movedAttachment = await store.Operations.SendAsync(new GetAttachmentOperation(destinationDocId, movedAttachmentName, AttachmentType.Document, null));
                Assert.Equal(movedAttachmentName, movedAttachment.Details.Name);
                Assert.Equal(RemoteAttachmentFlags.Remote, movedAttachment.Details.RemoteParameters.Flags);
                Assert.Equal(RemoteAttachment.Details.RemoteParameters.Identifier, movedAttachment.Details.RemoteParameters.Identifier);

                // Verify we can still read the content
                using var ms = new MemoryStream();
                await movedAttachment.Stream.CopyToAsync(ms);
                Assert.Equal([1, 2, 3], ms.ToArray());

                // Verify source attachment no longer exists
                Assert.Null(await store.Operations.SendAsync(new GetAttachmentOperation(sourceDocId, attachmentName, AttachmentType.Document, null))); 
            }
        }

        [AmazonS3RetryFact]
        public async Task CreateRemoteAttachmentThenMakeRevisionsEnabledThenAddAnotherAttachment()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    var result = store.Operations.Send(new PutAttachmentOperation("users/1", new StoreAttachmentParameters("foo/bar", profileStream)
                    {
                        RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)),
                        ContentType = "image/png",
                    }));
                    Assert.Equal("foo/bar", result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("image/png", result.ContentType);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", result.Hash);
                }
                // Remote the attachment
                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                // Verify attachment is Remote
                await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
                var remoteAttachment = await store.Operations.SendAsync(new GetAttachmentOperation("users/1", "foo/bar", AttachmentType.Document, null));
                Assert.Equal(RemoteAttachmentFlags.Remote, remoteAttachment.Details.RemoteParameters.Flags);

                await RevisionsHelper.SetupRevisionsAsync(store, modifyConfiguration: configuration =>
                {
                    configuration.Collections["Users"].PurgeOnDelete = true;
                    configuration.Collections["Users"].MinimumRevisionsToKeep = 4;
                });

                using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                {
                    var result = store.Operations.Send(new PutAttachmentOperation("users/1", "foo/bar", backgroundStream, "ImGgE/jPeG"));
                    Assert.Equal("foo/bar", result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("ImGgE/jPeG", result.ContentType);
                    Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", result.Hash);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var rev = await session.Advanced.Revisions.GetMetadataForAsync("users/1");
                    Assert.Equal(2, rev.Count);

                    var att1 = rev[0].GetObjects("@attachments");
                    var att2 = rev[1].GetObjects("@attachments");
                    Assert.Equal(1, att1.Length);
                    Assert.Equal(1, att2.Length);

                }


                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                using (var hash1 = ctx.GetLazyString("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo="))
                using (var hash2 = ctx.GetLazyString("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U="))
                using (Slice.From(ctx.Allocator, hash1, out var hash1Slice))
                using (Slice.From(ctx.Allocator, hash2, out var hash2Slice))
                {
                    Assert.False(database.DocumentsStorage.AttachmentsStorage.AttachmentExists(ctx, hash1));
                    Assert.True(database.DocumentsStorage.AttachmentsStorage.AttachmentExists(ctx, hash2));
                    Assert.Equal(0, database.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(ctx, hash1Slice).LocalAttachmentsCount);
                    Assert.Equal(1, database.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(ctx, hash1Slice).RemoteAttachmentsCount);
                    Assert.Equal(1, database.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(ctx, hash1Slice).Count);
                    Assert.Equal(2, database.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(ctx, hash2Slice).LocalAttachmentsCount);
                    Assert.Equal(0, database.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(ctx, hash2Slice).RemoteAttachmentsCount);
                    Assert.Equal(2, database.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(ctx, hash2Slice).Count);
                }
            }
        }


        [AmazonS3RetryFact]
        public async Task CreateRemoteAttachmentThenMakeRevisionsEnabledThenOverwriteTheAttachmentWithLocalStream()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    var result = store.Operations.Send(new PutAttachmentOperation("users/1", new StoreAttachmentParameters("foo/bar", profileStream)
                    {
                        RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)),
                        ContentType = "image/png",
                    }));
                    Assert.Equal("foo/bar", result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("image/png", result.ContentType);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", result.Hash);
                }
                // Remote the attachment
                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                // Verify attachment is Remote
                await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
                var remoteAttachment = await store.Operations.SendAsync(new GetAttachmentOperation("users/1", "foo/bar", AttachmentType.Document, null));
                Assert.Equal(RemoteAttachmentFlags.Remote, remoteAttachment.Details.RemoteParameters.Flags);

                await RevisionsHelper.SetupRevisionsAsync(store, modifyConfiguration: configuration =>
                {
                    configuration.Collections["Users"].PurgeOnDelete = true;
                    configuration.Collections["Users"].MinimumRevisionsToKeep = 4;
                });

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    var result = store.Operations.Send(new PutAttachmentOperation("users/1", "foo/bar", profileStream, "ImGgE/jPeG"));
                    Assert.Equal("foo/bar", result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("ImGgE/jPeG", result.ContentType);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", result.Hash);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var rev = await session.Advanced.Revisions.GetMetadataForAsync("users/1");
                    Assert.Equal(2, rev.Count);

                    var att1 = rev[0].GetObjects("@attachments");
                    var att2 = rev[1].GetObjects("@attachments");
                    Assert.Equal(1, att1.Length);
                    Assert.Equal(1, att2.Length);

                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                using (var hash1 = ctx.GetLazyString("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo="))
                using (Slice.From(ctx.Allocator, hash1, out var hash1Slice))
                {
                    Assert.True(database.DocumentsStorage.AttachmentsStorage.AttachmentExists(ctx, hash1));
                    // 1 doc + 1 rev
                    Assert.Equal(2, database.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(ctx, hash1Slice).LocalAttachmentsCount);
                    // 1 rev
                    Assert.Equal(1, database.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(ctx, hash1Slice).RemoteAttachmentsCount);
                    // overall
                    Assert.Equal(3, database.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(ctx, hash1Slice).Count);
                }
            }
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 32 * 1024, true)]
        [InlineData(32, 32 * 1024, true)]
        [InlineData(32, 32 * 1024, false)]
        public async Task StoreManyRemoteAttachmentsUsingBulkInsert(int count, int size, bool allRandom)
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);
                const string userId = "user/1";
                var streams = new Dictionary<string, MemoryStream>();
                try
                {
                    var hashset = new HashSet<string>();
                    byte[] defArr = null;
                    using (var bulkInsert = store.BulkInsert())
                    {
                        var user1 = new User { Name = "EGR" };
                        bulkInsert.Store(user1, userId);
                        var attachmentsBulkInsert = bulkInsert.AttachmentsFor(userId);
                        for (int i = 0; i < count; i++)
                        {
                            byte[] bArr;
                            if (allRandom)
                            {
                                bArr = GetActuallyRandomBArr(size, hashset);
                            }
                            else
                            {
                                if (i == 0)
                                {
                                    bArr = GetActuallyRandomBArr(size, hashset);
                                    defArr = bArr;
                                }
                                else
                                {
                                    bArr = defArr;
                                }
                            }

                            var name = i.ToString();
                            var stream = new MemoryStream(bArr);
                            await attachmentsBulkInsert.StoreAsync(new StoreAttachmentParameters(name, stream)
                            {
                                RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)),
                            });

                            stream.Position = 0;
                            streams[name] = stream;
                        }
                    }

                    // Remote the attachment
                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);


                    if (allRandom)
                    {
                        // Verify attachment is Remote
                        await GetBlobsFromCloudAndAssertForCount(Settings, count, 15_000);
                    }
                    else
                    {
                        await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
                    }



                    var attachmentsNames = streams.Select(x => new AttachmentRequest(userId, x.Key));

                    var tester = store.ForSessionTesting();

                    await tester.AssertAllAsync((_, session) =>
                    {
                        var attachmentsEnumerator = session.Advanced.Attachments.Get(attachmentsNames);

                        while (attachmentsEnumerator.MoveNext())
                        {
                            Assert.NotNull(attachmentsEnumerator.Current != null);
                            Assert.True(AttachmentsStreamTests.CompareStreams(attachmentsEnumerator.Current.Stream, streams[attachmentsEnumerator.Current.Details.Name]));
                        }
                    });
                }
                finally
                {
                    foreach (var stream in streams.Values)
                    {
                        await stream.DisposeAsync();
                    }
                }
            }
        }

        private static byte[] GetActuallyRandomBArr(int size, HashSet<string> hashset)
        {
            var rnd = new Random(DateTime.Now.Millisecond);
            var bArr = new byte[size];
            rnd.NextBytes(bArr);

            var hash = Convert.ToBase64String(bArr);
            while (hashset.Add(hash) == false)
            {
                rnd = new Random(DateTime.Now.Millisecond);
                rnd.NextBytes(bArr);
                hash = Convert.ToBase64String(bArr);
            }

            return bArr;
        }
    }
}
