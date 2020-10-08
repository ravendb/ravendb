using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Graph;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15538 : ReplicationTestBase
    {
        public RavenDB_15538(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task GetAttachmentsForDocumentShouldReturnAttachmentsOfRevisions()
        {
            using (var store = GetDocumentStore())
            {
                await SetupRevisionsForTest(Server, store, store.Database);
                var id = "user/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "EGR_1" }, id);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    using var stream = new MemoryStream(new byte[] { 3, 2, 2 });
                    session.Advanced.Attachments.Store(id, 1.ToString(), stream, "image/png");
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(id);
                    u.Name = "RGE" + u.Name.Substring(3);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(id);
                    u.Age = 30;
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var m = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(4, m.Count);
                    var cv = (string)m.First()[Constants.Documents.Metadata.ChangeVector];
                    var db = await GetDocumentDatabaseInstanceFor(store);
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
                    using (documentsContext.OpenReadTransaction())
                    {
                        var lzv = documentsContext.GetLazyString(id.ToLowerInvariant());
                        var attachments = db.DocumentsStorage.AttachmentsStorage.GetAttachmentsForDocument(documentsContext, AttachmentType.Revision, lzv, cv).ToList();
                        Assert.Equal(1, attachments.Count);
                        var revisionsAttachment = attachments.First();
                        Assert.Equal("bucfDXJ3eWRJYpgggJrnskJtMuMyFohjO2GHATxTmUs=", revisionsAttachment.Base64Hash.ToString());
                        Assert.StartsWith("A:10", revisionsAttachment.ChangeVector);
                        Assert.StartsWith($"user/1\u001er\u001e{revisionsAttachment.ChangeVector}\u001e1\u001e{revisionsAttachment.Base64Hash}\u001eimage/png", revisionsAttachment.Key);
                    }
                }
            }
        }

        [Fact]
        public async Task ItemsShouldPreserveTheOrderInStorageAfterReplicatingToDestinationsWithRevisionsConfig()
        {
            var reasonableWaitTime = Debugger.IsAttached ? (int)TimeSpan.FromMinutes(15).TotalMilliseconds : (int)TimeSpan.FromSeconds(15).TotalMilliseconds;

            using (var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string> { [RavenConfiguration.GetKey(x => x.Replication.MaxItemsCount)] = 1.ToString() },
                RegisterForDisposal = false
            }))
            {
                using (var source = GetDocumentStore(new Options { Server = server }))
                {
                    await SetupRevisionsForTest(server, source, source.Database);

                    var id = "user/0";
                    using (var session = source.OpenSession())
                    {
                        session.Store(new User { Name = "EGR_0" }, id);
                        session.SaveChanges();
                    }

                    using (var session = source.OpenSession())
                    {
                        using var stream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 });
                        session.Advanced.Attachments.Store(id, 0.ToString(), stream, "image/png");
                        session.SaveChanges();

                    }

                    using (var session = source.OpenAsyncSession())
                    {
                        var u = await session.LoadAsync<User>(id);
                        u.Name = "RGE" + u.Name.Substring(3);
                        await session.SaveChangesAsync();
                    }

                    List<ReplicationBatchItem> firstReplicationOrder = new List<ReplicationBatchItem>();
                    List<ReplicationBatchItem> secondReplicationOrder = new List<ReplicationBatchItem>();
                    var etag = 0L;

                    using var disposable1 = server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx);
                    using (var firstDestination = GetDocumentStore(new Options { ModifyDatabaseName = s => GetDatabaseName() + "_destination", Server = server }))
                    {
                        await SetupRevisionsForTest(server, firstDestination, firstDestination.Database);

                        var stats = source.Maintenance.Send(new GetStatisticsOperation());

                        var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(source.Database);
                        using var disposable2 = database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context1);
                        using (context1.OpenReadTransaction())
                        {
                            Assert.NotNull(database);

                            firstReplicationOrder.AddRange(ReplicationDocumentSender.GetReplicationItems(database, context1, etag, new ReplicationDocumentSender.ReplicationStats
                            {
                                DocumentRead = new OutgoingReplicationStatsScope(new OutgoingReplicationRunStats()),
                                AttachmentRead = new OutgoingReplicationStatsScope(new OutgoingReplicationRunStats())
                            }, false).Select(item => item.Clone(ctx)));
                            etag = firstReplicationOrder.Select(x => x.Etag).Max();
                        }
                        // Replication from source to firstDestination
                        await SetupReplicationAsync(source, firstDestination);
                        Assert.True(WaitForValue(() => AssertReplication(firstDestination, firstDestination.Database, stats), true, reasonableWaitTime, 333));

                        using (var session = source.OpenAsyncSession())
                        {
                            var u = await session.LoadAsync<User>(id);
                            u.Age = 30;
                            await session.SaveChangesAsync();
                        }

                        stats = source.Maintenance.Send(new GetStatisticsOperation());
                        Assert.True(WaitForValue(() => AssertReplication(firstDestination, firstDestination.Database, stats), true, reasonableWaitTime, 333));
                        using (context1.OpenReadTransaction())
                        {
                            firstReplicationOrder.AddRange(ReplicationDocumentSender.GetReplicationItems(database, context1, etag, new ReplicationDocumentSender.ReplicationStats
                            {
                                DocumentRead = new OutgoingReplicationStatsScope(new OutgoingReplicationRunStats()),
                                AttachmentRead = new OutgoingReplicationStatsScope(new OutgoingReplicationRunStats())
                            }, false).Select(item => item.Clone(ctx)));
                        }

                        var database2 = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(firstDestination.Database);
                        Assert.NotNull(database2);

                        // record the replicated items order
                        using var disposable3 = database2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
                        using (context.OpenReadTransaction())
                        {
                            secondReplicationOrder.AddRange(ReplicationDocumentSender.GetReplicationItems(database2, context, 0, new ReplicationDocumentSender.ReplicationStats
                            {
                                DocumentRead = new OutgoingReplicationStatsScope(new OutgoingReplicationRunStats()),
                                AttachmentRead = new OutgoingReplicationStatsScope(new OutgoingReplicationRunStats())
                            }, false).Select(item => item.Clone(ctx)));
                        }

                        // remove the 1st doc sent, because of its change in the middle of the 1st replication, so it was sent twice
                        Assert.True(firstReplicationOrder.Remove(firstReplicationOrder.First(y => y is DocumentReplicationItem doc && doc.Flags.Contain(DocumentFlags.Revision) == false)));
                        Assert.Equal(firstReplicationOrder.Count, secondReplicationOrder.Count);

                        for (int i = 0; i < secondReplicationOrder.Count; i++)
                        {
                            var item = firstReplicationOrder[i];
                            var item2 = secondReplicationOrder[i];

                            if (item is AttachmentReplicationItem a1 && item2 is AttachmentReplicationItem a2)
                            {
                                Assert.Equal(a1.Key.ToString(), a2.Key.ToString());
                                Assert.Equal(a1.ChangeVector, a2.ChangeVector);
                                Assert.Equal(a1.Type, a2.Type);
                            }
                            else if (item is DocumentReplicationItem d1 && item2 is DocumentReplicationItem d2)
                            {
                                Assert.Equal(d1.Id, d2.Id);
                                Assert.Equal(d1.ChangeVector, d2.ChangeVector);
                                Assert.Equal(d1.Flags | DocumentFlags.FromReplication, d2.Flags);
                                var type1 = d1.Flags.Contain(DocumentFlags.Revision) ? " (Revision)" : "";
                                var type2 = d2.Flags.Contain(DocumentFlags.Revision) ? " (Revision)" : "";
                                Assert.Equal(type1, type2);
                            }
                            else
                            {
                                string msg;
                                if (item is AttachmentReplicationItem)
                                {
                                    Assert.True(item2 is DocumentReplicationItem);
                                    msg = "item is AttachmentReplicationItem AND item2 is DocumentReplicationItem";
                                }
                                else if (item is DocumentReplicationItem)
                                {
                                    Assert.True(item2 is AttachmentReplicationItem);
                                    msg = "item is DocumentReplicationItem AND item2 is AttachmentReplicationItem";
                                }
                                else
                                {
                                    msg = "item and item2 got invalid type.";
                                }
                                Assert.True(false, $"Expected to get exact order of replication on destination as on source but got: {msg}");
                            }
                        }
                    }
                }
            }
        }
        private static bool AssertReplication(DocumentStore destinationStore, string database, DatabaseStatistics sourceStats)
        {
            var destinationStats = destinationStore.Maintenance.ForDatabase(database).Send(new GetStatisticsOperation());
            if (destinationStats.CountOfAttachments == sourceStats.CountOfAttachments
                && destinationStats.CountOfDocuments == sourceStats.CountOfDocuments
                && destinationStats.CountOfRevisionDocuments == sourceStats.CountOfRevisionDocuments
                && destinationStats.CountOfUniqueAttachments == sourceStats.CountOfUniqueAttachments)
                return true;

            return false;
        }

        private static async Task SetupRevisionsForTest(RavenServer server, DocumentStore store, string database)
        {
            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = int.MaxValue },
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                {
                    ["Users"] = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = int.MaxValue }
                }
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var configurationJson = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(configuration, context);
                var (index, _) = await server.ServerStore.ModifyDatabaseRevisions(context, store.Database, configurationJson, Guid.NewGuid().ToString());
                var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                await documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(index, server.ServerStore.Engine.OperationTimeout);
            }
        }
    }
}
