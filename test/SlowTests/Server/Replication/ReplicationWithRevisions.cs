using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Replication
{
    public class ReplicationWithRevisions : ReplicationTestBase
    {
        [Fact]
        public async Task CanReplicateRevisions()
        {
            var company = new Company {Name = "Company Name"};
            var company2 = new Company {Name = "Company Name2"};

            using (var master = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var slave = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, master.Database);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, slave.Database);
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
                    Assert.Equal(2, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 2));
                }
            }
        }

        [Fact]
        public async Task CreateRevisionsAndReplicateThemAll()
        {
            var company = new Company {Name = "Company Name"};
            var company2 = new Company {Name = "Company Name2"};
            var company3 = new Company {Name = "Company Name3"};
            var company4 = new Company {Name = "Company Name4"};

            using (var master = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var slave = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, master.Database);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, slave.Database);

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
                    Assert.Equal(4, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 4));
                }
            }
        }

        [Fact]
        public async Task ReplicateRevisionsIgnoringConflicts()
        {
            using (var storeA = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var storeB = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            {
                await GenerateConflictAndSetupMasterMasterReplication(storeA, storeB);

               

                using (var session = storeA.OpenSession())
                {
                    Assert.Equal(2, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 2));
                }
                using (var session = storeB.OpenSession())
                {
                    Assert.Equal(2, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 2));
                }
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task CreateConflictAndResolveItIncreaseTheRevisions(bool configureVersioning)
        {
            using (var storeA = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var storeB = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            {
                await GenerateConflictAndSetupMasterMasterReplication(storeA, storeB, configureVersioning);

                if (configureVersioning)
                {
                    using (var session = storeA.OpenSession())
                    {
                        Assert.Equal(2, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 2));
                    }
                    using (var session = storeB.OpenSession())
                    {
                        Assert.Equal(2, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 2));
                    }
                }

                var config = new ConflictSolver
                {
                    ResolveToLatest = true
                };

                await UpdateConflictResolver(storeA, config.ResolveByCollection, config.ResolveToLatest);

                Assert.True(WaitForDocument(storeA, "foo/bar"));
                Assert.True(WaitForDocument(storeB, "foo/bar"));

                using (var session = storeA.OpenSession())
                {
                    Assert.Equal(3, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 3));

                    var metadata = session.Advanced.Revisions.GetMetadataFor("foo/bar");
                    var flags = metadata[0]["@flags"];
                    Assert.Equal((DocumentFlags.Revision| DocumentFlags.HasRevisions | DocumentFlags.Resolved).ToString(), flags);
                    flags = metadata[1]["@flags"];
                    Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication | DocumentFlags.Conflicted).ToString(), flags);
                    flags = metadata[2]["@flags"];
                    Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.Conflicted).ToString(), flags);
                }
                using (var session = storeB.OpenSession())
                {
                    Assert.Equal(3, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 3));

                    var metadata = session.Advanced.Revisions.GetMetadataFor("foo/bar");
                    var flags = metadata[0]["@flags"];
                    Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication | DocumentFlags.Resolved).ToString(), flags);
                    flags = metadata[1]["@flags"];
                    if (configureVersioning)
                    {
                        Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.Conflicted).ToString(), flags);
                    }
                    else
                    {
                        Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication | DocumentFlags.Conflicted).ToString(), flags);
                    }
                    flags = metadata[2]["@flags"];
                    Assert.Equal((DocumentFlags.Revision | DocumentFlags.HasRevisions | DocumentFlags.FromReplication | DocumentFlags.Conflicted).ToString(), flags);
                }
            }
        }

        [Fact]
        public async Task ResolvedDocumentShouldNotGenerateRevision()
        {
            var file = GetTempFileName();
            try
            {
                using (var storeA = GetDocumentStore())
                using (var storeB = GetDocumentStore())
                {
                    var user = new User { Name = "Name" };
                    var user2 = new User { Name = "Name2" };

                    using (var session = storeA.OpenAsyncSession())
                    {
                        await session.StoreAsync(user, "foo/bar");
                        await session.SaveChangesAsync();
                    }

                    using (var session = storeB.OpenAsyncSession())
                    {
                        await session.StoreAsync(user2, "foo/bar");
                        await session.SaveChangesAsync();
                    }

                    await SetupReplicationAsync(storeA, storeB);
                    using (var sessionB = storeB.OpenSession())
                    {
                        Assert.Equal(3, WaitForValue(() => sessionB.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 3));
                    }

                    await SetupReplicationAsync(storeB, storeA);
                    using (var sessionA = storeA.OpenSession())
                    {
                        Assert.Equal(3, WaitForValue(() => sessionA.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 3));
                    }

                    var exportOp = await storeA.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions
                    {
                        OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments
                    }, file);
                    await exportOp.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var src = GetDocumentStore())
                using (var dst = GetDocumentStore())
                {
                    var importOp = await src.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions
                    {
                        OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments
                    }, file);
                    await importOp.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    await SetupReplicationAsync(src, dst);
                    WaitForDocument(dst, "foo/bar");

                    WaitForUserToContinueTheTest(src);

                    using (var session1 = src.OpenSession())
                    using (var session2 = dst.OpenSession())
                    {
                        Assert.Equal(0, session1.Advanced.Revisions.GetMetadataFor("foo/bar").Count);
                        Assert.Equal(0, session2.Advanced.Revisions.GetMetadataFor("foo/bar").Count);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task RevisionsAreReplicatedBack(bool configureVersioning)
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                if (configureVersioning)
                {
                    await RevisionsHelper.SetupRevisions(Server.ServerStore, storeA.Database);
                    await RevisionsHelper.SetupRevisions(Server.ServerStore, storeB.Database);
                }

                var company = new Company { Name = "Name" };
                var company2 = new Company { Name = "Name2" };

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "keep-conflicted-revision-insert-order");
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
                    Assert.Equal(3, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 3));

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
                    Assert.Equal(3, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 3));

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

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task RevisionsAreReplicatedBackWithTombstoneAsResolved(bool configureVersioning)
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var expectedRevisionsCount = 3;
                if (configureVersioning)
                {
                    await RevisionsHelper.SetupRevisions(Server.ServerStore, storeA.Database);
                    await RevisionsHelper.SetupRevisions(Server.ServerStore, storeB.Database);
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
                    await session.StoreAsync(new Company(), "keep-conflicted-revision-insert-order");
                    await session.SaveChangesAsync();

                    await session.StoreAsync(company2, "foo/bar");
                    await session.SaveChangesAsync();
                    session.Delete("foo/bar");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);

                using (var session = storeB.OpenSession())
                {
                    Assert.Equal(expectedRevisionsCount, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, expectedRevisionsCount));

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
                    Assert.Equal(expectedRevisionsCount, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, expectedRevisionsCount));

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

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task RevisionsAreReplicatedBackWithTombstone(bool configureVersioning)
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var expectedRevisionsCount = 3;
                if (configureVersioning)
                {
                    await RevisionsHelper.SetupRevisions(Server.ServerStore, storeA.Database);
                    await RevisionsHelper.SetupRevisions(Server.ServerStore, storeB.Database);
                    expectedRevisionsCount = 4;
                }

                var company = new Company { Name = "Name" };
                var company2 = new Company { Name = "Name2" };

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company(), "keep-conflicted-revision-insert-order");
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
                    Assert.Equal(expectedRevisionsCount, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, expectedRevisionsCount));

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
                    Assert.Equal(expectedRevisionsCount, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, expectedRevisionsCount));

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
                await RevisionsHelper.SetupRevisions(Server.ServerStore, storeA.Database);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, storeB.Database);
            }

            using (var session = storeB.OpenAsyncSession())
            {
                await session.StoreAsync(new Company(), "keep-conflicted-revision-insert-order");
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

        [Fact]
        public async Task UpdateTheSameRevisionWhenGettingExistingRevision()
        {
            using (var storeA = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var storeB = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var storeC = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, storeA.Database);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, storeB.Database);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, storeC.Database);

                await SetupReplicationAsync(storeA, storeB);

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Fitzchak"}, "users/1");
                    await session.SaveChangesAsync();
                }
                using (var session = storeA.OpenSession())
                {
                    Assert.Equal(1, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("users/1").Count, 1));
                }
                using (var session = storeB.OpenSession())
                {
                    Assert.Equal(1, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("users/1").Count, 1));
                }
                Assert.True(WaitForDocument(storeB, "users/1"));

                await SetupReplicationAsync(storeA, storeC);
                await SetupReplicationAsync(storeB, storeC);

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Marker" }, "marker");
                    await session.SaveChangesAsync();
                }
                Assert.True(WaitForDocument(storeC, "marker"));
                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Marker" }, "marker");
                    await session.SaveChangesAsync();
                }
                Assert.True(WaitForDocument(storeB, "marker"));

                using (var session = storeC.OpenSession())
                {
                    Assert.Equal(1, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("users/1").Count, 1));
                }
            }
        }
    }
}
