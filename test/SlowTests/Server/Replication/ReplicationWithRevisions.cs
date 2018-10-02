using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests.Server.Documents.Revisions;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
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

                Assert.Equal(2, WaitUntilHasConflict(storeA, "foo/bar").Length);
                Assert.Equal(2, WaitUntilHasConflict(storeB, "foo/bar").Length);

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

                Assert.Equal(2, WaitUntilHasConflict(storeA, "foo/bar").Length);
                Assert.Equal(2, WaitUntilHasConflict(storeB, "foo/bar").Length);

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
                }
                using (var session = storeB.OpenSession())
                {
                    Assert.Equal(3, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 3));
                }
            }
        }

        [Fact]
        public async Task RevisionsAreReplicatedBack()
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

                using (var session = storeB.OpenSession())
                {
                    Assert.Equal(3, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 3));
                }

                await SetupReplicationAsync(storeB, storeA);

                using (var session = storeA.OpenSession())
                {
                    Assert.Equal(3, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 3));
                }
            }
        }

        [Fact]
        public async Task RevisionsAreReplicatedBackWithTombstoneAsResolved()
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
                    session.Delete("foo/bar");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);

                using (var session = storeB.OpenSession())
                {
                    Assert.Equal(3, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 3));
                }

                await SetupReplicationAsync(storeB, storeA);

                using (var session = storeA.OpenSession())
                {
                    Assert.Equal(3, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 3));
                }
            }
        }

        [Fact]
        public async Task RevisionsAreReplicatedBackWithTombstone()
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
                    session.Delete("foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(user2, "foo/bar");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);

                using (var session = storeB.OpenSession())
                {
                    Assert.Equal(3, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 3));
                }

                await SetupReplicationAsync(storeB, storeA);

                using (var session = storeA.OpenSession())
                {
                    Assert.Equal(3, WaitForValue(() => session.Advanced.Revisions.GetMetadataFor("foo/bar").Count, 3));
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
            await SetupReplicationAsync(storeB, storeA);
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
