using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Replication;
using Raven.Client.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Replication
{
    public class ReplicationSpecialCases : ReplicationTestBase
    {

        [Fact]
        public async Task NonIdenticalContentConflict()
        {
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

                await SetupReplicationAsync(master, slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }


                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmeli"
                    }, "users/1");
                    session.SaveChanges();
                }

                Assert.Equal(2, WaitUntilHasConflict(slave, "users/1", 1).Length);
            }
        }

        [Fact]
        public async Task NonIdenticalMetadataConflict()
        {
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

                await SetupReplicationAsync(master, slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    var user = session.Load<User>("users/1");
                    var meta = session.Advanced.GetMetadataFor(user);
                    meta.Add("bla", "asd");
                    session.Store(user);
                    session.SaveChanges();
                }


                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    var user = session.Load<User>("users/1");
                    var meta = session.Advanced.GetMetadataFor(user);
                    meta.Add("bla", "asd");
                    meta.Add("bla2", "asd");
                    session.SaveChanges();
                }

                Assert.Equal(2, WaitUntilHasConflict(slave, "users/1", 1).Length);
            }
        }


        [Fact]
        public async Task UpdateConflictOnParentDocumentArrival()
        {
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
                await SetupReplicationAsync(master, slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmeli"
                    }, "users/1");
                    session.SaveChanges();
                }
                Assert.Equal(2, WaitUntilHasConflict(slave, "users/1", 1).Length);

                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel123"
                    }, "users/1");
                    session.SaveChanges();
                }

                Assert.Equal(2, WaitUntilHasConflict(slave, "users/1", 1).Length);
            }
        }

        [Fact]
        public async Task IdenticalContentConflictResolution()
        {
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

                await SetReplicationConflictResolutionAsync(slave, StraightforwardConflictResolution.None);
                await SetupReplicationAsync(master, slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel",
                        Age = 12
                    }, "users/1");
                    session.SaveChanges();
                }


                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Age = 12,
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                bool failed = false;
                try
                {
                    WaitUntilHasConflict(slave, "users/1", 1);
                    failed = true;
                }
                catch
                {
                    // all good! no conflict here
                }
                Assert.False(failed);
            }
        }


        [Fact]
        public async Task TomstoneToTombstoneConflict()
        {
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

                await SetReplicationConflictResolutionAsync(slave, StraightforwardConflictResolution.None);
                await SetupReplicationAsync(master, slave);

                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmeli"
                    }, "users/1");
                    session.SaveChanges();
                }

                var doc = WaitForDocument(slave, "users/1");
                Assert.True(doc);

                using (var session = slave.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                var deletedDoc = WaitForDocumentDeletion(slave, "users/1");
                Assert.True(deletedDoc);

                using (var session = master.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }
                bool failed = false;
                try
                {
                    WaitUntilHasConflict(slave, "users/1", 1);
                    failed = true;
                }
                catch
                {
                    // all good! no conflict here
                }
                Assert.False(failed);
            }
        }

        [Fact]
        public async Task ReplicationShouldSendMissingAttachments()
        {
            var docId = "users/1";
            using (var source = GetDocumentStore(options: new Options
            {
                ModifyDatabaseName = s => "Source" 

            }))
            using (var destination = GetDocumentStore(options: new Options
            {
                ModifyDatabaseName = s => "Destination"
            }))
            {
                await SetupReplicationAsync(source, destination);
                using (var session = source.OpenAsyncSession())
                using (var fooStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    await session.StoreAsync(new User() { Name = "Foo" }, docId);
                    session.Advanced.Attachments.Store(docId, "foo.png", fooStream, "image/png");
                    await session.SaveChangesAsync();
                }

                WaitForDocumentToReplicate<User>(destination, docId, 15 * 1000);
                using (var session = destination.OpenAsyncSession())
                {
                    session.Delete(docId);
                    await session.SaveChangesAsync();
                }

                using (var session = source.OpenAsyncSession())
                using (var fooStream2 = new MemoryStream(new byte[] { 4, 5, 6 }))
                {
                    session.Advanced.Attachments.Store(docId, "foo2.png", fooStream2, "image/png");
                    await session.SaveChangesAsync();
                }

                Assert.NotNull(WaitForDocumentWithAttachmentToReplicate<User>(destination, docId, "foo2.png", 15 * 1000));

                var buffer = new byte[3];
                using (var session = destination.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(docId);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(attachments.Length, 2);
                    foreach (var name in attachments)
                    {
                        using (var attachment = await session.Advanced.Attachments.GetAsync(user, name.Name))
                        {
                            Assert.NotNull(attachment);
                            Assert.Equal(3, await attachment.Stream.ReadAsync(buffer, 0, 3));
                            if (attachment.Details.Name == "foo.png")
                            {
                                Assert.Equal(1, buffer[0]);
                                Assert.Equal(2, buffer[1]);
                                Assert.Equal(3, buffer[2]);
                            }                            
                        }
                    }
                    
                }
            }
        }
    }
}
