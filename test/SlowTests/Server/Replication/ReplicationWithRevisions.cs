using System;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class ReplicationWithRevisions : ReplicationTestBase
    {
        public ReplicationWithRevisions(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanReplicateRevisions(Options options)
        {
            options = UpdateConflictSolverAndGetMergedOptions(options);

            var company = new Company { Name = "Company Name" };
            var company2 = new Company { Name = "Company Name2" };

            using (var master = GetDocumentStore(options: options))
            using (var slave = GetDocumentStore(options: options))
            {
                await RevisionsHelper.SetupRevisionsAsync(master);
                await RevisionsHelper.SetupRevisionsAsync(slave);
                await SetupReplicationAsync(master, slave);

                using (var session = master.OpenAsyncSession())
                {
                    await session.StoreAsync(company, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = master.OpenAsyncSession())
                {
                    await session.StoreAsync(company2, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = slave.OpenSession())
                {
                    Assert.True(WaitForDocument(slave, "foo/bar"));
                    Assert.Equal(2, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 2, interval: 128));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CreateRevisionsAndReplicateThemAll(Options options)
        {
            options = UpdateConflictSolverAndGetMergedOptions(options);

            var company = new Company { Name = "Company Name" };
            var company2 = new Company { Name = "Company Name2" };
            var company3 = new Company { Name = "Company Name3" };
            var company4 = new Company { Name = "Company Name4" };

            using (var master = GetDocumentStore(options: options))
            using (var slave = GetDocumentStore(options: options))
            {
                await RevisionsHelper.SetupRevisionsAsync(master);
                await RevisionsHelper.SetupRevisionsAsync(slave);

                using (var session = master.OpenAsyncSession())
                {
                    await session.StoreAsync(company, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = master.OpenAsyncSession())
                {
                    await session.StoreAsync(company2, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = master.OpenAsyncSession())
                {
                    await session.StoreAsync(company3, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = master.OpenAsyncSession())
                {
                    await session.StoreAsync(company4, "foo/bar");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(master, slave);

                using (var session = slave.OpenSession())
                {
                    Assert.True(WaitForDocument(slave, "foo/bar"));
                    Assert.Equal(4, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 4, interval: 128));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ReplicateRevisionsIgnoringConflicts(Options options)
        {
            options = UpdateConflictSolverAndGetMergedOptions(options);
            using (var storeA = GetDocumentStore(options: options))
            using (var storeB = GetDocumentStore(options: options))
            {
                await GenerateConflictAndSetupMasterMasterReplication(storeA, storeB);

                using (var session = storeA.OpenSession())
                {
                    Assert.Equal(2, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 2, interval: 128));
                }
                using (var session = storeB.OpenSession())
                {
                    Assert.Equal(2, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 2, interval: 128));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ChangeDefaultRevisionsConflictConfiguration(Options options)
        {
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            {
                var result = await storeB.Maintenance.Server.SendAsync(new ConfigureRevisionsForConflictsOperation(storeB.Database, new RevisionsCollectionConfiguration
                {
                    MinimumRevisionsToKeep = 3,
                }));

                if (options.DatabaseMode == RavenDatabaseMode.Single)
                {
                    var rachisLogIndexNotifications = (await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(storeB.Database)).RachisLogIndexNotifications;
                    await rachisLogIndexNotifications.WaitForIndexNotification(result.RaftCommandIndex.Value, TimeSpan.FromSeconds(10));
                }
                else
                {
                    var rachisLogIndexNotifications = Sharding.GetOrchestrator(storeB.Database).RachisLogIndexNotifications;
                    await rachisLogIndexNotifications.WaitForIndexNotification(result.RaftCommandIndex.Value, TimeSpan.FromSeconds(10));
                }
                
                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company(), "keep-conflicted-revision-insert-order");
                    await session.SaveChangesAsync();

                    await session.StoreAsync(new User
                    {
                        Name = "Karmel-A-1"
                    }, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Karmel-B-1"
                    }, "foo/bar");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeA, storeB);

                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Karmel-B-2"
                    }, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Karmel-A-2"
                    }, "foo/bar");
                    await session.SaveChangesAsync();
                }

                await EnsureReplicatingAsync(storeA, storeB);

                using (var session = storeB.OpenSession())
                {
                    Assert.Equal(3, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 3, interval: 128));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanDisableRevisionsConflict(Options options)
        {
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            {
                var result = await storeB.Maintenance.Server.SendAsync(new ConfigureRevisionsForConflictsOperation(storeB.Database, new RevisionsCollectionConfiguration
                {
                    Disabled = true
                }));

                if (options.DatabaseMode == RavenDatabaseMode.Single)
                {
                    var rachisLogIndexNotifications = (await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(storeB.Database)).RachisLogIndexNotifications;
                    await rachisLogIndexNotifications.WaitForIndexNotification(result.RaftCommandIndex.Value, TimeSpan.FromSeconds(10));
                }
                else
                {
                    var rachisLogIndexNotifications = Sharding.GetOrchestrator(storeB.Database).RachisLogIndexNotifications;
                    await rachisLogIndexNotifications.WaitForIndexNotification(result.RaftCommandIndex.Value, TimeSpan.FromSeconds(10));
                }

                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company(), "keep-conflicted-revision-insert-order");
                    await session.SaveChangesAsync();

                    await session.StoreAsync(new User
                    {
                        Name = "Karmel-A-1"
                    }, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Karmel-B-1"
                    }, "foo/bar");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeA, storeB);

                using (var session = storeB.OpenSession())
                {
                    Assert.Equal(0, session.Advanced.Revisions.GetMetadataFor("foo/bar").Count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        public async Task CreateConflictAndResolveItIncreaseTheRevisions(Options options, bool configureVersioning)
        {
            options = UpdateConflictSolverAndGetMergedOptions(options);
            using (var storeA = GetDocumentStore(options: options))
            using (var storeB = GetDocumentStore(options: options))
            {
                await GenerateConflictAndSetupMasterMasterReplication(storeA, storeB, configureVersioning);

                if (configureVersioning)
                {
                    using (var session = storeA.OpenSession())
                    {
                        Assert.Equal(2, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 2, interval: 128));
                    }
                    using (var session = storeB.OpenSession())
                    {
                        Assert.Equal(2, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 2, interval: 128));
                    }
                }

                var config = new ConflictSolver
                {
                    ResolveToLatest = true
                };

                await UpdateConflictResolver(storeA, config.ResolveByCollection, config.ResolveToLatest);

                using (var session = storeA.OpenSession())
                {
                    Assert.Equal(3, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 3, interval: 128));

                    var metadata = session.Advanced.Revisions.GetMetadataFor("foo/bar");
                    var flags = metadata[0]["@flags"];
                    Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.Resolved).ToString(), flags);
                    flags = metadata[1]["@flags"];
                    Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication | DocumentFlags.Conflicted).ToString(), flags);
                    flags = metadata[2]["@flags"];
                    Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.Conflicted).ToString(), flags);
                }
                using (var session = storeB.OpenSession())
                {
                    Assert.Equal(3, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 3, interval: 128));

                    var db2 = await GetDocumentDatabaseInstanceForAsync(storeB, options.DatabaseMode, "foo/bar");

                    var metadata = session.Advanced.Revisions.GetMetadataFor("foo/bar");
                    var flags = metadata[0]["@flags"];
                    Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication | DocumentFlags.Resolved).ToString(), flags);

                    var expectedFlags1 = (DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.Conflicted).ToString();
                    var expectedFlags2 = (DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication | DocumentFlags.Conflicted).ToString();

                    AssertRevisionFlags(metadata[1]);
                    AssertRevisionFlags(metadata[2]);

                    void AssertRevisionFlags(IMetadataDictionary revisionMetadata)
                    {
                        flags = revisionMetadata["@flags"];
                        var cv = revisionMetadata["@change-vector"].ToString();
                        var etag = ChangeVectorUtils.GetEtagById(cv, db2.DbBase64Id);
                        var expected = etag != 0 ? expectedFlags1 : expectedFlags2;
                        Assert.Equal(expected, flags);
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions | RavenTestCategory.BackupExportImport)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ResolvedDocumentShouldNotGenerateRevision(Options options)
        {
            const int revisionsAmountFromConflict = 3;
            const string docId = "foo/bar";

            var file = GetTempFileName();
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            {
                var user = new User { Name = "Name" };
                var user2 = new User { Name = "Name2" };

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(user, docId);
                    await session.SaveChangesAsync();
                }

                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(user2, docId);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                using (var sessionB = storeB.OpenSession())
                {
                    Assert.Equal(revisionsAmountFromConflict, WaitForValue(() => sessionB.Advanced.Revisions.GetMetadataFor(docId).Count, revisionsAmountFromConflict, interval: 128));
                }

                await SetupReplicationAsync(storeB, storeA);
                using (var sessionA = storeA.OpenSession())
                {
                    Assert.Equal(revisionsAmountFromConflict, WaitForValue(() => sessionA.Advanced.Revisions.GetMetadataFor(docId).Count, revisionsAmountFromConflict, interval: 128));
                }

                var exportOp = await storeA.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions
                {
                    OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments
                }, file);
                await exportOp.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }

            using (var src = GetDocumentStore(options))
            using (var dst = GetDocumentStore(options))
            {
                var importOp = await src.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions
                {
                    OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments
                }, file);
                await importOp.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                await SetupReplicationAsync(src, dst);
                Assert.NotNull(WaitForDocument(dst, docId));

                using (var session1 = src.OpenSession())
                using (var session2 = dst.OpenSession())
                {
                    Assert.Equal(revisionsAmountFromConflict, session1.Advanced.Revisions.GetMetadataFor(docId).Count);
                    Assert.Equal(revisionsAmountFromConflict, session2.Advanced.Revisions.GetMetadataFor(docId).Count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public async Task RevisionsAreReplicatedBack(Options options, bool configureVersioning)
        {
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            {
                if (configureVersioning)
                {
                    await RevisionsHelper.SetupRevisionsAsync(storeA);
                    await RevisionsHelper.SetupRevisionsAsync(storeB);
                }

                var company = new Company { Name = "Name" };
                var company2 = new Company { Name = "Name2" };

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "keep-conflicted-revision-insert-order$foo/bar");
                    await session.SaveChangesAsync();

                    await session.StoreAsync(company, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(company2, "foo/bar");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);

                using (var session = storeB.OpenSession())
                {
                    Assert.Equal(3, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 3, interval: 128));

                    var metadata = session.Advanced.Revisions.GetMetadataFor("foo/bar");
                    var flags = metadata[0]["@flags"];
                    Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.Resolved).ToString(), flags);
                    flags = metadata[1]["@flags"];
                    Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.Conflicted).ToString(), flags);
                    flags = metadata[2]["@flags"];
                    Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication | DocumentFlags.Conflicted).ToString(), flags);
                }

                await SetupReplicationAsync(storeB, storeA);

                using (var session = storeA.OpenSession())
                {
                    Assert.Equal(3, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 3, interval: 128));

                    var metadata = session.Advanced.Revisions.GetMetadataFor("foo/bar");
                    var flags = metadata[0]["@flags"];
                    Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication | DocumentFlags.Resolved).ToString(), flags);
                    flags = metadata[1]["@flags"];
                    Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication | DocumentFlags.Conflicted).ToString(), flags);
                    flags = metadata[2]["@flags"];
                    Assert.Equal(
                        configureVersioning
                            ? (DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.Conflicted).ToString()
                            : (DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication | DocumentFlags.Conflicted).ToString(), flags);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        public async Task RevisionsAreReplicatedBackWithTombstoneAsResolved(Options options, bool configureVersioning)
        {
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            {
                var expectedRevisionsCount = 3;
                if (configureVersioning)
                {
                    await RevisionsHelper.SetupRevisionsAsync(storeA);
                    await RevisionsHelper.SetupRevisionsAsync(storeB);
                    expectedRevisionsCount = 4;
                }

                var company = new Company { Name = "Name" };
                var company2 = new Company { Name = "Name2" };

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(company, "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company(), "keep-conflicted-revision-insert-order$foo/bar");
                    await session.SaveChangesAsync();

                    await session.StoreAsync(company2, "foo/bar");
                    await session.SaveChangesAsync();
                    session.Delete("foo/bar");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);

                using (var session = storeB.OpenSession())
                {
                    Assert.Equal(expectedRevisionsCount, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, expectedRevisionsCount, interval: 128));

                    var metadata = session.Advanced.Revisions.GetMetadataFor("foo/bar");
                    var flags = metadata[0]["@flags"];
                    Assert.Equal((DocumentFlags.DeleteRevision | DocumentFlags.HasRevisions | DocumentFlags.Resolved).ToString(), flags);
                    flags = metadata[1]["@flags"];
                    Assert.Equal((DocumentFlags.DeleteRevision | DocumentFlags.HasRevisions | DocumentFlags.Conflicted).ToString(), flags);
                    flags = metadata[2]["@flags"];
                    Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication | DocumentFlags.Conflicted).ToString(), flags);

                    if (configureVersioning)
                    {
                        flags = metadata[3]["@flags"];
                        Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions).ToString(), flags);
                    }
                }

                await SetupReplicationAsync(storeB, storeA);

                using (var session = storeA.OpenSession())
                {
                    Assert.Equal(expectedRevisionsCount, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, expectedRevisionsCount, interval: 128));

                    var metadata = session.Advanced.Revisions.GetMetadataFor("foo/bar");
                    var flags = metadata[0]["@flags"];
                    Assert.Equal((DocumentFlags.DeleteRevision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication | DocumentFlags.Resolved).ToString(), flags);
                    flags = metadata[1]["@flags"];
                    Assert.Equal((DocumentFlags.DeleteRevision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication | DocumentFlags.Conflicted).ToString(), flags);
                    flags = metadata[2]["@flags"];

                    if (configureVersioning)
                    {
                        Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.Conflicted).ToString(), flags);
                        flags = metadata[3]["@flags"];
                        Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication).ToString(), flags);
                    }
                    else
                    {
                        Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication | DocumentFlags.Conflicted).ToString(), flags);
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        public async Task RevisionsAreReplicatedBackWithTombstone(Options options, bool configureVersioning)
        {
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            {
                var expectedRevisionsCount = 3;
                if (configureVersioning)
                {
                    await RevisionsHelper.SetupRevisionsAsync(storeA);
                    await RevisionsHelper.SetupRevisionsAsync(storeB);
                    expectedRevisionsCount = 4;
                }

                var company = new Company { Name = "Name" };
                var company2 = new Company { Name = "Name2" };

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company(), "keep-conflicted-revision-insert-order$foo/bar");
                    await session.SaveChangesAsync();

                    await session.StoreAsync(company, "foo/bar");
                    await session.SaveChangesAsync();
                    session.Delete("foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(company2, "foo/bar");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);

                using (var session = storeB.OpenSession())
                {
                    Assert.Equal(expectedRevisionsCount, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, expectedRevisionsCount, interval: 128));

                    var metadata = session.Advanced.Revisions.GetMetadataFor("foo/bar");
                    var flags = metadata[0]["@flags"];
                    Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.Resolved).ToString(), flags);
                    flags = metadata[1]["@flags"];
                    Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.Conflicted).ToString(), flags);
                    flags = metadata[2]["@flags"];
                    Assert.Equal((DocumentFlags.DeleteRevision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication | DocumentFlags.Conflicted).ToString(), flags);

                    if (configureVersioning)
                    {
                        flags = metadata[3]["@flags"];
                        Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication).ToString(), flags);
                    }
                }

                await SetupReplicationAsync(storeB, storeA);

                using (var session = storeA.OpenSession())
                {
                    Assert.Equal(expectedRevisionsCount, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, expectedRevisionsCount, interval: 128));

                    var metadata = session.Advanced.Revisions.GetMetadataFor("foo/bar");
                    var flags = metadata[0]["@flags"];
                    Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication | DocumentFlags.Resolved).ToString(), flags);
                    flags = metadata[1]["@flags"];
                    Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication | DocumentFlags.Conflicted).ToString(), flags);
                    flags = metadata[2]["@flags"];

                    if (configureVersioning)
                    {
                        Assert.Equal((DocumentFlags.DeleteRevision | DocumentFlags.HasRevisions | DocumentFlags.Conflicted).ToString(), flags);
                        flags = metadata[3]["@flags"];
                        Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions).ToString(), flags);
                    }
                    else
                    {
                        Assert.Equal((DocumentFlags.DeleteRevision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication | DocumentFlags.Conflicted).ToString(), flags);
                    }
                }
            }
        }

        private async Task GenerateConflictAndSetupMasterMasterReplication(DocumentStore storeA, DocumentStore storeB, bool configureVersioning = true)
        {
            var user = new User { Name = "Name" };
            var user2 = new User { Name = "Name2" };

            if (configureVersioning)
            {
                await RevisionsHelper.SetupRevisionsAsync(storeA);
                await RevisionsHelper.SetupRevisionsAsync(storeB);
            }

            using (var session = storeB.OpenAsyncSession())
            {
                await session.StoreAsync(new Company(), "keep-conflicted-revision-insert-order$foo/bar");
                await session.SaveChangesAsync();

                await session.StoreAsync(user2, "foo/bar");
                await session.SaveChangesAsync();
            }

            using (var session = storeA.OpenAsyncSession())
            {
                await session.StoreAsync(user, "foo/bar");
                await session.SaveChangesAsync();
            }

            await SetupReplicationAsync(storeA, storeB);
            Assert.Equal(2, WaitUntilHasConflict(storeB, "foo/bar").Length);
            await SetupReplicationAsync(storeB, storeA);
            Assert.Equal(2, WaitUntilHasConflict(storeA, "foo/bar").Length);
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task UpdateTheSameRevisionWhenGettingExistingRevision(Options options)
        {
            options = UpdateConflictSolverAndGetMergedOptions(options);
            using (var storeA = GetDocumentStore(options: options))
            using (var storeB = GetDocumentStore(options: options))
            using (var storeC = GetDocumentStore(options: options))
            {
                await RevisionsHelper.SetupRevisionsAsync(storeA);
                await RevisionsHelper.SetupRevisionsAsync(storeB);
                await RevisionsHelper.SetupRevisionsAsync(storeC);

                await SetupReplicationAsync(storeA, storeB);

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }, "users/1");
                    await session.SaveChangesAsync();
                }
                using (var session = storeA.OpenSession())
                {
                    Assert.Equal(1, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("users/1").Count, 1, interval: 128));
                }

                Assert.True(WaitForDocument(storeB, "users/1"));
                using (var session = storeB.OpenSession())
                {
                    Assert.Equal(1, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("users/1").Count, 1, interval: 128));
                }

                await SetupReplicationAsync(storeA, storeC);
                await SetupReplicationAsync(storeB, storeC);

                await EnsureReplicatingAsync(storeA, storeC);
                await EnsureReplicatingAsync(storeB, storeC);

                using (var session = storeC.OpenSession())
                {
                    Assert.Equal(1, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("users/1").Count, 1, interval: 128));
                }
            }
        }
    }
}
