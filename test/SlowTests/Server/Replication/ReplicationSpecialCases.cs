using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Replication;
using Raven.Client.ServerWide;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Client.Attachments;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class ReplicationSpecialCases : ReplicationTestBase
    {
        public ReplicationSpecialCases(ITestOutputHelper output) : base(output)
        {
        }

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

        [Theory]
        [InlineData("users/1")]
        [InlineData("users/1-A")]
        [InlineData("FoObAr")]
        public async Task ReplicationShouldSendMissingAttachments(string documentId)
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                await SetupReplicationAsync(source, destination);

                using (var session = source.OpenAsyncSession())
                using (var fooStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    await session.StoreAsync(new User { Name = "Foo" }, documentId);
                    session.Advanced.Attachments.Store(documentId, "foo.png", fooStream, "image/png");
                    await session.SaveChangesAsync();
                }

                Assert.NotNull(WaitForDocumentToReplicate<User>(destination, documentId, 15 * 1000));

                using (var session = destination.OpenAsyncSession())
                {
                    session.Delete(documentId);
                    await session.SaveChangesAsync();
                }

                using (var session = source.OpenAsyncSession())
                using (var fooStream2 = new MemoryStream(new byte[] { 4, 5, 6 }))
                {
                    session.Advanced.Attachments.Store(documentId, "foo2.png", fooStream2, "image/png");
                    await session.SaveChangesAsync();
                }

                Assert.NotNull(WaitForDocumentWithAttachmentToReplicate<User>(destination, documentId, "foo2.png", 15 * 1000));

                var buffer = new byte[3];
                using (var session = destination.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(documentId);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(2, attachments.Length);
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

        [Theory]
        [InlineData("users/1", "users/2")]
        [InlineData("users/1-A", "users/2-A")]
        [InlineData("foo", "foo-2")]
        [InlineData("FOO", "FOO-2")]
        [InlineData("FoObAr", "FoObAr-2")]
        public async Task ReplicationShouldSendMissingAttachmentsAlongWithNewOnes(string documentId1, string documentId2)
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                await SetupReplicationAsync(source, destination);

                using (var session = source.OpenAsyncSession())
                using (var fooStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    await session.StoreAsync(new User { Name = "Foo" }, documentId1);
                    session.Advanced.Attachments.Store(documentId1, "foo.png", fooStream, "image/png");
                    await session.SaveChangesAsync();
                }

                Assert.NotNull(WaitForDocumentToReplicate<User>(destination, documentId1, 15 * 1000));

                using (var session = destination.OpenAsyncSession())
                {
                    session.Delete(documentId1);
                    await session.SaveChangesAsync();
                }

                using (var session = source.OpenAsyncSession())
                {
                    var toDispose = new List<IDisposable>();

                    // force replication of the original document (without the attachment)
                    var doc1 = await session.LoadAsync<User>(documentId1);
                    doc1.LastName = "Bar";

                    await session.StoreAsync(new User { Name = "Foo" }, documentId2);

                    for (var i = 0; i < 30; i++)
                    {
                        var fooStream = new MemoryStream(new byte[] { 4, 5, 6, (byte)i });
                        toDispose.Add(fooStream);
                        session.Advanced.Attachments.Store(documentId2, $"foo{i}.png", fooStream, "image/png");
                    }

                    await session.SaveChangesAsync();

                    session.Advanced.OnAfterSaveChanges += (_, __) => toDispose.ForEach(x => x.Dispose());
                }

                Assert.NotNull(WaitForDocumentWithAttachmentToReplicate<User>(destination, documentId1, "foo.png", 15 * 1000));
                Assert.NotNull(WaitForDocumentWithAttachmentToReplicate<User>(destination, documentId2, "foo29.png", 15 * 1000));

                var buffer = new byte[3];
                using (var session = destination.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(documentId1);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(1, attachments.Length);
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

        [Fact]
        public async Task ShouldNotInterruptReplicationBatchWhenThereAreMissingAttachments()
        {
            var co = new ServerCreationOptions
            {
                RunInMemory = false,
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Replication.MaxSizeToSend)] = 8.ToString()
                },
                RegisterForDisposal = false
            };
            using (var server = GetNewServer(co))
            using (var source = GetDocumentStore(new Options { Server = server, RunInMemory = false }))
            using (var destination = GetDocumentStore(new Options { Server = server, RunInMemory = false }))
            {
                const string documentId1 = "users/1-A";
                const string attachmentName1 = "foo.png";
                const string attachmentName2 = "foo.big";
                const string contentType = "image/png";
                using (var session = source.OpenAsyncSession())
                using (var stream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    await session.StoreAsync(new User { Name = "Foo" }, documentId1);
                    session.Advanced.Attachments.Store(documentId1, attachmentName1, stream, contentType);
                    await session.SaveChangesAsync();
                }

                using (var session = source.OpenAsyncSession())
                using (var stream = new BigDummyStream(8 * 1024 * 1024)) // 8mb
                {
                    await session.StoreAsync(new User { Name = "Foo2" }, documentId1);
                    session.Advanced.Attachments.Store(documentId1, attachmentName2, stream, contentType);
                    await session.SaveChangesAsync();
                }

                var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(source.Database);
                var documentsStorage = database.DocumentsStorage;
                using (documentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var attachmentStorage = documentsStorage.AttachmentsStorage;
                    var attachment = attachmentStorage.GetAttachment(context, documentId1, attachmentName1, AttachmentType.Document, null);
                    using (var stream = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        attachmentStorage.PutAttachment(context, documentId1, attachmentName1, contentType, attachment.Base64Hash.ToString(), null, stream, updateDocument: false);
                    }
                    tx.Commit();
                }
                await SetupReplicationAsync(source, destination);
                Assert.NotNull(WaitForDocumentWithAttachmentToReplicate<User>(destination, documentId1, attachmentName1, Debugger.IsAttached ? 60_000 : 15_000));

                using (var session = destination.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(documentId1);
                    Assert.Equal(user.Name, "Foo2");
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(2, attachments.Length);
                    Assert.NotNull(attachments[0]);
                    Assert.Equal("oQblnWsNxEG8d+ktN1Kz444Kuc2+Yd3O94mJSHtDD5o=", attachments[0].Hash);
                    Assert.Equal(attachmentName2, attachments[0].Name);
                    Assert.Equal(contentType, attachments[0].ContentType);
                    Assert.NotNull(attachments[1]);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[1].Hash);
                    Assert.Equal(attachmentName1, attachments[1].Name);
                    Assert.Equal(contentType, attachments[1].ContentType);
                }
            }
        }

        [Fact]
        public async Task ShouldNotThrowNREWhenCheckingForMissingAttachments()
        {
            var documentId1 = "users/1";
            var documentId2 = "users/2";
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                await SetupReplicationAsync(source, destination);

                using (var session = source.OpenAsyncSession())
                using (var ms = new MemoryStream(new byte[] { 1, 2, 3 }))
                using (var ms2 = new MemoryStream(new byte[] { 1, 2, 3, 4 }))
                {
                    await session.StoreAsync(new User { Name = "EGR" }, documentId1);
                    session.Advanced.Attachments.Store(documentId1, "pic.gif", ms, "image/gif");

                    await session.StoreAsync(new User { Name = "RGE" }, documentId2);
                    session.Advanced.Attachments.Store(documentId2, "pic2.gif", ms2, "image/gif");
                    await session.SaveChangesAsync();
                }

                Assert.NotNull(WaitForDocumentWithAttachmentToReplicate<User>(destination, documentId1, "pic.gif", Debugger.IsAttached ? 60_000 : 15_000));
                Assert.NotNull(WaitForDocumentWithAttachmentToReplicate<User>(destination, documentId2, "pic2.gif", Debugger.IsAttached ? 60_000 : 15_000));

                using (var session = destination.OpenAsyncSession())
                {
                    session.Delete(documentId1);
                    await session.SaveChangesAsync();
                }

                using (var session = source.OpenAsyncSession())
                {
                    var doc1 = await session.LoadAsync<User>(documentId1);
                    doc1.LastName = "Bar";
                    var doc2 = await session.LoadAsync<User>(documentId2);
                    doc2.LastName = "Bar";
                    await session.SaveChangesAsync();
                }
                EnsureReplicating(source, destination);
                using (var session = destination.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(documentId1);
                    Assert.Equal(user.Name, "EGR");
                    Assert.Equal(user.LastName, "Bar");
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(1, attachments.Length);
                    var buffer = new byte[3];
                    foreach (var name in attachments)
                    {
                        using (var attachment = await session.Advanced.Attachments.GetAsync(user, name.Name))
                        {
                            Assert.NotNull(attachments[0]);
                            Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachment.Details.Hash);
                            Assert.Equal("pic.gif", attachment.Details.Name);
                            Assert.Equal("image/gif", attachment.Details.ContentType);
                            Assert.Equal(3, await attachment.Stream.ReadAsync(buffer, 0, 3));
                            Assert.Equal(1, buffer[0]);
                            Assert.Equal(2, buffer[1]);
                            Assert.Equal(3, buffer[2]);
                        }
                    }

                    user = await session.LoadAsync<User>(documentId2);
                    Assert.Equal(user.Name, "RGE");
                    Assert.Equal(user.LastName, "Bar");
                    attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(1, attachments.Length);
                    buffer = new byte[4];
                    foreach (var name in attachments)
                    {
                        using (var attachment = await session.Advanced.Attachments.GetAsync(user, name.Name))
                        {
                            Assert.NotNull(attachment);
                            Assert.Equal("KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=", attachment.Details.Hash);
                            Assert.Equal("pic2.gif", attachment.Details.Name);
                            Assert.Equal("image/gif", attachment.Details.ContentType);
                            Assert.Equal(4, await attachment.Stream.ReadAsync(buffer, 0, 4));
                            Assert.Equal(1, buffer[0]);
                            Assert.Equal(2, buffer[1]);
                            Assert.Equal(3, buffer[2]);
                            Assert.Equal(4, buffer[3]);
                        }
                    }
                }
            }
        }

        // RavenDB-15820
        [Fact]
        public async Task ShouldResolveAttachmentConflictToLatestAndNotThrowNRE()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                using (var session = store1.OpenAsyncSession())
                using (var a1 = new MemoryStream(new byte[] { 1, 2, 3, 4 }))
                {
                    var user = new User();
                    await session.StoreAsync(user, "foo");
                    session.Advanced.Attachments.Store(user, "dummy", a1);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store1, store2);
                EnsureReplicating(store1, store2);

                using (var session = store2.OpenAsyncSession())
                using (var a1 = new MemoryStream(new byte[] { 6, 6, 6 }))
                {
                    session.Advanced.Attachments.Store("foo", "dummy", a1);
                    await session.SaveChangesAsync();
                }

                using (var session = store1.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("foo");
                    user.Name = "Karmel";
                    await session.SaveChangesAsync();
                }

                EnsureReplicating(store1, store2);

                using (var session = store2.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("foo");
                    Assert.Equal(user.Name, "Karmel");
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(1, attachments.Length);
                    var buffer = new byte[4];
                    foreach (var name in attachments)
                    {
                        using (var attachment = await session.Advanced.Attachments.GetAsync(user, name.Name))
                        {
                            // this is the 1st (old) attachment, resolved from the latest document
                            Assert.NotNull(attachments[0]);
                            Assert.Equal("KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=", attachment.Details.Hash);
                            Assert.Equal("dummy", attachment.Details.Name);
                            Assert.Equal(string.Empty, attachment.Details.ContentType);
                            Assert.Equal(4, await attachment.Stream.ReadAsync(buffer, 0, 4));
                            Assert.Equal(1, buffer[0]);
                            Assert.Equal(2, buffer[1]);
                            Assert.Equal(3, buffer[2]);
                            Assert.Equal(4, buffer[3]);
                        }
                    }
                }
            }
        }
    }
}
