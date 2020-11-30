using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using FastTests.Utils;
using Orders;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Attachments
{
    public class AttachmentsReplication : ReplicationTestBase
    {
        public AttachmentsReplication(ITestOutputHelper output) : base(output)
        {
        }

        public static Guid dbId1 = new Guid("00000000-48c4-421e-9466-000000000000");
        public static Guid dbId2 = new Guid("99999999-48c4-421e-9466-000000000000");

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task PutAttachments(bool replicateDocumentFirst)
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await SetDatabaseId(store1, dbId1);
                await SetDatabaseId(store2, dbId2);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }
                if (replicateDocumentFirst)
                {
                    await SetupAttachmentReplicationAsync(store1, store2, false);
                    Assert.True(WaitForDocument(store2, "users/1"));
                }

                var names = new[]
                {
                    "profile.png",
                    "background-photo.jpg",
                    "fileNAME_#$1^%_בעברית.txt"
                };
                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    var result = store1.Operations.Send(new PutAttachmentOperation("users/1", names[0], profileStream, "image/png"));
                    Assert.Equal(names[0], result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("image/png", result.ContentType);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", result.Hash);
                }
                using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                {
                    var result = store1.Operations.Send(new PutAttachmentOperation("users/1", names[1], backgroundStream, "ImGgE/jPeG"));
                    Assert.Equal(names[1], result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("ImGgE/jPeG", result.ContentType);
                    Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", result.Hash);
                }
                using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    var result = store1.Operations.Send(new PutAttachmentOperation("users/1", names[2], fileStream, null));
                    Assert.Equal(names[2], result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("", result.ContentType);
                    Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", result.Hash);
                }

                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Marker" }, "marker");
                    session.SaveChanges();
                }
                if (replicateDocumentFirst == false)
                {
                    await SetupAttachmentReplicationAsync(store1, store2, false);
                }
                Assert.True(WaitForDocument(store2, "marker"));

                using (var session = store2.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal((DocumentFlags.HasAttachments | DocumentFlags.FromReplication).ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Assert.Equal(3, attachments.Length);
                    var orderedNames = names.OrderBy(x => x).ToArray();
                    for (var i = 0; i < names.Length; i++)
                    {
                        var name = orderedNames[i];
                        var attachment = attachments[i];
                        Assert.Equal(name, attachment.GetString(nameof(AttachmentName.Name)));
                        var hash = attachment.GetString(nameof(AttachmentName.Hash));
                        if (i == 0)
                        {
                            Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", hash);
                        }
                        else if (i == 1)
                        {
                            Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", hash);
                        }
                        else if (i == 2)
                        {
                            Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                        }
                    }
                }

                AssertAttachmentCount(store2, 3, 3, 2);

                using (var session = store2.OpenSession())
                {
                    var readBuffer = new byte[8];
                    for (var i = 0; i < names.Length; i++)
                    {
                        var name = names[i];
                        using (var attachmentStream = new MemoryStream(readBuffer))
                        using (var attachment = session.Advanced.Attachments.Get("users/1", name))
                        {
                            attachment.Stream.CopyTo(attachmentStream);

                            Assert.Equal(name, attachment.Details.Name);
                            Assert.Equal(i == 0 ? 3 : 5, attachmentStream.Position);
                            if (i == 0)
                            {
                                Assert.Equal(new byte[] { 1, 2, 3 }, readBuffer.Take(3));
                                Assert.Equal("image/png", attachment.Details.ContentType);
                                Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachment.Details.Hash);
                            }
                            else if (i == 1)
                            {
                                Assert.Equal(new byte[] { 10, 20, 30, 40, 50 }, readBuffer.Take(5));
                                Assert.Equal("ImGgE/jPeG", attachment.Details.ContentType);
                                Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", attachment.Details.Hash);
                            }
                            else if (i == 2)
                            {
                                Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, readBuffer.Take(5));
                                Assert.Equal("", attachment.Details.ContentType);
                                Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", attachment.Details.Hash);
                            }
                        }
                    }

                    using (var notExistsAttachment = session.Advanced.Attachments.Get("users/1", "not-there"))
                    {
                        Assert.Null(notExistsAttachment);
                    }
                }
            }
        }

        [Theory]
        [InlineData("\t", null)]
        [InlineData("\\", "\\")]
        [InlineData("/", "/")]
        [InlineData("5", "5")]
        public async Task PutAndGetSpecialChar(string nameAndContentType, string expectedContentType)
        {
            var name = "aA" + nameAndContentType;
            if (expectedContentType != null)
                expectedContentType = "aA" + expectedContentType;

            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var dbId1 = new Guid("00000000-48c4-421e-9466-000000000000");
                await SetDatabaseId(store1, dbId1);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }
                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    var result = store1.Operations.Send(new PutAttachmentOperation("users/1", name, profileStream, name));
                    Assert.Contains("A:2", result.ChangeVector);
                    Assert.Equal(name, result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal(name, result.ContentType);
                }

                await SetupAttachmentReplicationAsync(store1, store2, false);
                Assert.True(WaitForDocument(store2, "users/1"));

                using (var session = store2.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal((DocumentFlags.HasAttachments | DocumentFlags.FromReplication).ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    var attachment = attachments.Single();
                    Assert.Equal(name, attachment.GetString(nameof(AttachmentName.Name)));
                }

                using (var session = store2.OpenSession())
                {
                    var readBuffer = new byte[8];
                    using (var attachmentStream = new MemoryStream(readBuffer))
                    using (var attachment = session.Advanced.Attachments.Get("users/1", name))
                    {
                        attachment.Stream.CopyTo(attachmentStream);
                        Assert.Equal(name, attachment.Details.Name);
                        Assert.Equal(new byte[] { 1, 2, 3 }, readBuffer.Take(3));
                        Assert.Equal(expectedContentType, attachment.Details.ContentType);
                    }
                }
            }
        }

        [Fact]
        public async Task DeleteAttachments()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var dbId1 = new Guid("00000000-48c4-421e-9466-000000000000");
                var dbId2 = new Guid("99999999-48c4-421e-9466-999999999999");
                await SetDatabaseId(store1, dbId1);
                await SetDatabaseId(store2, dbId2);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }

                for (int i = 1; i <= 3; i++)
                {
                    using (var profileStream = new MemoryStream(Enumerable.Range(1, 3 * i).Select(x => (byte)x).ToArray()))
                        store1.Operations.Send(new PutAttachmentOperation("users/1", "file" + i, profileStream, "image/png"));
                }
                AssertAttachmentCount(store1, 3);

                store1.Operations.Send(new DeleteAttachmentOperation("users/1", "file2"));

                await SetupAttachmentReplicationAsync(store1, store2);
                AssertAttachmentCount(store2, 2);

                using (var session = store2.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal((DocumentFlags.HasAttachments | DocumentFlags.FromReplication).ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Assert.Equal(2, attachments.Length);
                    Assert.Equal("file1", attachments[0].GetString(nameof(AttachmentName.Name)));
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[0].GetString(nameof(AttachmentName.Hash)));
                    Assert.Equal("file3", attachments[1].GetString(nameof(AttachmentName.Name)));
                    Assert.Equal("NRQuixiqj+xvEokF6MdQq1u+uH1dk/gk2PLChJQ58Vo=", attachments[1].GetString(nameof(AttachmentName.Hash)));
                }

                using (var session = store2.OpenSession())
                {
                    var readBuffer = new byte[16];
                    using (var attachmentStream = new MemoryStream(readBuffer))
                    using (var attachment = session.Advanced.Attachments.Get("users/1", "file1"))
                    {
                        attachment.Stream.CopyTo(attachmentStream);
                        Assert.Contains("A:2", attachment.Details.ChangeVector);
                        Assert.Equal("file1", attachment.Details.Name);
                        Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachment.Details.Hash);
                        Assert.Equal(3, attachmentStream.Position);
                        Assert.Equal(new byte[] { 1, 2, 3 }, readBuffer.Take(3));
                    }
                    using (var attachmentStream = new MemoryStream(readBuffer))
                    using (var attachment = session.Advanced.Attachments.Get("users/1", "file3"))
                    {
                        attachment.Stream.CopyTo(attachmentStream);
                        Assert.Contains("A:6", attachment.Details.ChangeVector);
                        Assert.Equal("file3", attachment.Details.Name);
                        Assert.Equal("NRQuixiqj+xvEokF6MdQq1u+uH1dk/gk2PLChJQ58Vo=", attachment.Details.Hash);
                        Assert.Equal(9, attachmentStream.Position);
                        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 }, readBuffer.Take(9));
                    }
                }

                // Delete document should delete all the attachments
                store1.Commands().Delete("users/1", null);
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Marker 2" }, "marker2");
                    session.SaveChanges();
                }
                Assert.True(WaitForDocument(store2, "marker2"));
                AssertAttachmentCount(store2, 0);
            }
        }

        public static void AssertAttachmentCount(DocumentStore store, long uniqueAttachmentCount, long? attachmentCount = null, long? documentsCount = null)
        {
            var statistics = store.Maintenance.Send(new GetStatisticsOperation());
            Assert.Equal(attachmentCount ?? uniqueAttachmentCount, statistics.CountOfAttachments);
            Assert.Equal(uniqueAttachmentCount, statistics.CountOfUniqueAttachments);

            if (documentsCount != null)
                Assert.Equal(documentsCount.Value, statistics.CountOfDocuments);
        }

        [Fact]
        public async Task PutAndDeleteAttachmentsWithTheSameStream_AlsoTestBigStreams()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var dbId1 = new Guid("00000000-48c4-421e-9466-000000000000");
                await SetDatabaseId(store1, dbId1);

                for (int i = 1; i <= 3; i++)
                {
                    using (var session = store1.OpenSession())
                    {
                        session.Store(new User { Name = "Fitzchak " + i }, "users/" + i);
                        session.SaveChanges();
                    }

                    // Use 128 KB file to test hashing a big file (> 32 KB)
                    using (var stream1 = new MemoryStream(Enumerable.Range(1, 128 * 1024).Select(x => (byte)x).ToArray()))
                        store1.Operations.Send(new PutAttachmentOperation("users/" + i, "file" + i, stream1, "image/png"));
                }
                using (var stream2 = new MemoryStream(Enumerable.Range(1, 999 * 1024).Select(x => (byte)x).ToArray()))
                    store1.Operations.Send(new PutAttachmentOperation("users/1", "big-file", stream2, "image/png"));

                await SetupAttachmentReplicationAsync(store1, store2);
                AssertAttachmentCount(store2, 2, 4);

                using (var session = store2.OpenSession())
                {
                    var readBuffer = new byte[1024 * 1024];
                    using (var attachmentStream = new MemoryStream(readBuffer))
                    using (var attachment = session.Advanced.Attachments.Get("users/3", "file3"))
                    {
                        attachment.Stream.CopyTo(attachmentStream);
                        Assert.Contains("A:8", attachment.Details.ChangeVector);
                        Assert.Equal("file3", attachment.Details.Name);
                        Assert.Equal("uuBtr5rVX6NAXzdW2DhuG04MGGyUzFzpS7TelHw3fJQ=", attachment.Details.Hash);
                        Assert.Equal(128 * 1024, attachmentStream.Position);
                        var expected = Enumerable.Range(1, 128 * 1024).Select(x => (byte)x);
                        var actual = readBuffer.Take((int)attachmentStream.Position);
                        Assert.Equal(expected, actual);
                    }
                    using (var attachmentStream = new MemoryStream(readBuffer))
                    using (var attachment = session.Advanced.Attachments.Get("users/1", "big-file"))
                    {
                        attachment.Stream.CopyTo(attachmentStream);
                        Assert.Contains("A:10", attachment.Details.ChangeVector);
                        Assert.Equal("big-file", attachment.Details.Name);
                        Assert.Equal("zKHiLyLNRBZti9DYbzuqZ/EDWAFMgOXB+SwKvjPAINk=", attachment.Details.Hash);
                        Assert.Equal(999 * 1024, attachmentStream.Position);
                        Assert.Equal(Enumerable.Range(1, 999 * 1024).Select(x => (byte)x), readBuffer.Take((int)attachmentStream.Position));
                    }
                }

                store1.Operations.Send(new DeleteAttachmentOperation("users/1", "file1"));
                AssertDelete(store1, store2, "file1", 2, 3);

                store1.Operations.Send(new DeleteAttachmentOperation("users/2", "file2"));
                AssertDelete(store1, store2, "file2", 2);

                store1.Operations.Send(new DeleteAttachmentOperation("users/3", "file3"));
                AssertDelete(store1, store2, "file3", 1);

                store1.Operations.Send(new DeleteAttachmentOperation("users/1", "big-file"));
                AssertDelete(store1, store2, "big-file", 0);

                for (int i = 1; i <= 3; i++)
                {
                    using (var session = store2.OpenSession())
                    {
                        var user = session.Load<User>("users/" + i);
                        var metadata = session.Advanced.GetMetadataFor(user);
                        Assert.DoesNotContain(DocumentFlags.HasAttachments.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                        Assert.Equal(DocumentFlags.FromReplication.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                        Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.Attachments));
                    }
                }
            }
        }

        private async Task SetupAttachmentReplicationAsync(DocumentStore store1, DocumentStore store2, bool waitOnMarker = true)
        {
            //var database1 = GetDocumentDatabaseInstanceFor(store1).Result;
            var database1 = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database).Result;
            database1.Configuration.Replication.MaxItemsCount = null;
            database1.Configuration.Replication.MaxSizeToSend = null;
            await SetupReplicationAsync(store1, store2);

            if (waitOnMarker)
            {
                WaitForMarker(store1, store2);
            }
        }

        private void WaitForMarker(DocumentStore store1, DocumentStore store2)
        {
            var id = "marker - " + Guid.NewGuid();
            using (var session = store1.OpenSession())
            {
                session.Store(new Product { Name = "Marker" }, id);
                session.SaveChanges();
            }
            Assert.True(WaitForDocument(store2, id));
        }

        private void AssertDelete(DocumentStore store1, DocumentStore store2, string name, long expectedUniqueAttachments, long? expectedAttachments = null)
        {
            using (var session = store1.OpenSession())
            {
                session.Store(new User { Name = "Marker " + name }, "marker-" + name);
                session.SaveChanges();
            }
            Assert.True(WaitForDocument(store2, "marker-" + name));
            AssertAttachmentCount(store2, expectedUniqueAttachments, expectedAttachments);
        }

        [Fact]
        public async Task DeleteDocumentWithAttachmentsThatHaveTheSameStream()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                for (int i = 1; i <= 3; i++)
                {
                    using (var session = store1.OpenSession())
                    {
                        session.Store(new User { Name = "Fitzchak " + i }, "users/" + i);
                        session.SaveChanges();
                    }

                    using (var profileStream = new MemoryStream(Enumerable.Range(1, 3).Select(x => (byte)x).ToArray()))
                        store1.Operations.Send(new PutAttachmentOperation("users/" + i, "file" + i, profileStream, "image/png"));
                }
                using (var profileStream = new MemoryStream(Enumerable.Range(1, 17).Select(x => (byte)x).ToArray()))
                    store1.Operations.Send(new PutAttachmentOperation("users/1", "second-file", profileStream, "image/png"));

                await SetupAttachmentReplicationAsync(store1, store2);
                AssertAttachmentCount(store2, 2, 4);

                store1.Commands().Delete("users/2", null);
                AssertDelete(store1, store2, "#1", 2, 3);

                store1.Commands().Delete("users/1", null);
                AssertDelete(store1, store2, "#2", 1);

                store1.Commands().Delete("users/3", null);
                AssertDelete(store1, store2, "#3", 0);
            }
        }

        [Fact]
        public async Task AttachmentsRevisionsReplication()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var dbId1 = new Guid("00000000-48c4-421e-9466-000000000000");
                await SetDatabaseId(store1, dbId1);

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database, modifyConfiguration: configuration =>
                {
                    configuration.Collections["Users"].PurgeOnDelete = false;
                    configuration.Collections["Users"].MinimumRevisionsToKeep = 4;
                });
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store2.Database, configuration =>
                {
                    configuration.Collections["Users"].PurgeOnDelete = false;
                    configuration.Collections["Users"].MinimumRevisionsToKeep = 4;
                });

                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }

                var names = new[]
                {
                    "profile.png",
                    "background-photo.jpg",
                    "fileNAME_#$1^%_בעברית.txt"
                };

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    var result = store1.Operations.Send(new PutAttachmentOperation("users/1", names[0], profileStream, "image/png"));
                    Assert.Contains("A:3", result.ChangeVector);
                    Assert.Equal(names[0], result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("image/png", result.ContentType);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", result.Hash);
                }
                using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                {
                    var result = store1.Operations.Send(new PutAttachmentOperation("users/1", names[1], backgroundStream, "ImGgE/jPeG"));
                    Assert.Contains("A:7", result.ChangeVector);
                    Assert.Equal(names[1], result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("ImGgE/jPeG", result.ContentType);
                    Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", result.Hash);
                }
                using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    var result = store1.Operations.Send(new PutAttachmentOperation("users/1", names[2], fileStream, null));
                    Assert.Contains("A:12", result.ChangeVector);
                    Assert.Equal(names[2], result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("", result.ContentType);
                    Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", result.Hash);
                }
                await SetupAttachmentReplicationAsync(store1, store2);

                AssertRevisions(store2, names, (session, revisions) =>
                {
                    AssertRevisionAttachments(names, 3, revisions[0], session);
                    AssertRevisionAttachments(names, 2, revisions[1], session);
                    AssertRevisionAttachments(names, 1, revisions[2], session);
                    AssertNoRevisionAttachment(revisions[3], session);
                }, 9);

                // Delete document should delete all the attachments
                store1.Commands().Delete("users/1", null);
                WaitForMarker(store1, store2);
                AssertRevisions(store2, names, (session, revisions) =>
                {
                    AssertNoRevisionAttachment(revisions[0], session, true);
                    AssertRevisionAttachments(names, 3, revisions[1], session);
                    AssertRevisionAttachments(names, 2, revisions[2], session);
                    AssertRevisionAttachments(names, 1, revisions[3], session);
                }, 6);

                // Create another revision which should delete old revision
                using (var session = store1.OpenSession()) // This will delete the revision #1 which is without attachment
                {
                    session.Store(new User { Name = "Fitzchak 2" }, "users/1");
                    session.SaveChanges();
                }
                WaitForMarker(store1, store2);
                AssertRevisions(store2, names, (session, revisions) =>
                {
                    AssertNoRevisionAttachment(revisions[0], session);
                    AssertNoRevisionAttachment(revisions[1], session, true);
                    AssertRevisionAttachments(names, 3, revisions[2], session);
                    AssertRevisionAttachments(names, 2, revisions[3], session);
                }, 5, expectedCountOfDocuments: 4);

                using (var session = store1.OpenSession()) // This will delete the revision #2 which is with attachment
                {
                    session.Store(new User { Name = "Fitzchak 3" }, "users/1");
                    session.SaveChanges();
                }
                WaitForMarker(store1, store2);
                AssertRevisions(store2, names, (session, revisions) =>
                {
                    AssertNoRevisionAttachment(revisions[0], session);
                    AssertNoRevisionAttachment(revisions[1], session);
                    AssertNoRevisionAttachment(revisions[2], session, true);
                    AssertRevisionAttachments(names, 3, revisions[3], session);
                }, 3, expectedCountOfDocuments: 5);

                using (var session = store1.OpenSession()) // This will delete the revision #3 which is with attachment
                {
                    session.Store(new User { Name = "Fitzchak 4" }, "users/1");
                    session.SaveChanges();
                }
                WaitForMarker(store1, store2);
                AssertRevisions(store2, names, (session, revisions) =>
                {
                    AssertNoRevisionAttachment(revisions[0], session);
                    AssertNoRevisionAttachment(revisions[1], session);
                    AssertNoRevisionAttachment(revisions[2], session);
                    AssertNoRevisionAttachment(revisions[3], session, true);
                }, 0, expectedCountOfUniqueAttachments: 0, expectedCountOfDocuments: 6);

                using (var session = store1.OpenSession()) // This will delete the revision #4 which is with attachment
                {
                    session.Store(new User { Name = "Fitzchak 5" }, "users/1");
                    session.SaveChanges();
                }
                WaitForMarker(store1, store2);
                AssertRevisions(store2, names, (session, revisions) =>
                {
                    AssertNoRevisionAttachment(revisions[0], session);
                    AssertNoRevisionAttachment(revisions[1], session);
                    AssertNoRevisionAttachment(revisions[2], session);
                    AssertNoRevisionAttachment(revisions[3], session);
                }, 0, expectedCountOfUniqueAttachments: 0, expectedCountOfDocuments: 7);
            }
        }

        private static void AssertRevisions(DocumentStore store, string[] names, Action<IDocumentSession, List<User>> assertAction,
            long expectedCountOfAttachments, long expectedCountOfDocuments = 2, long expectedCountOfUniqueAttachments = 3)
        {
            var statistics = store.Maintenance.Send(new GetStatisticsOperation());
            Assert.Equal(expectedCountOfAttachments, statistics.CountOfAttachments);
            Assert.Equal(expectedCountOfUniqueAttachments, statistics.CountOfUniqueAttachments);
            Assert.Equal(4, statistics.CountOfRevisionDocuments);
            Assert.Equal(expectedCountOfDocuments, statistics.CountOfDocuments);
            Assert.Equal(0, statistics.CountOfIndexes);

            using (var session = store.OpenSession())
            {
                var revisions = session.Advanced.Revisions.GetFor<User>("users/1");
                Assert.Equal(4, revisions.Count);
                assertAction(session, revisions);
            }
        }

        private static void AssertNoRevisionAttachment(User revision, IDocumentSession session, bool isDeleteRevision = false)
        {
            var metadata = session.Advanced.GetMetadataFor(revision);
            var flags = DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.FromReplication;
            if (isDeleteRevision)
                flags = DocumentFlags.HasRevisions | DocumentFlags.DeleteRevision | DocumentFlags.FromReplication;
            Assert.Equal(flags.ToString(), metadata[Constants.Documents.Metadata.Flags]);
            Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.Attachments));
        }

        private static void AssertRevisionAttachments(string[] names, int expectedCount, User revision, IDocumentSession session)
        {
            var metadata = session.Advanced.GetMetadataFor(revision);
            Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.HasAttachments | DocumentFlags.FromReplication).ToString(), metadata[Constants.Documents.Metadata.Flags]);
            var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
            Assert.Equal(expectedCount, attachments.Length);

            var orderedNames = names.Take(expectedCount).OrderBy(x => x).ToArray();
            for (var i = 0; i < expectedCount; i++)
            {
                var name = orderedNames[i];
                var attachment = attachments[i];
                Assert.Equal(name, attachment.GetString(nameof(AttachmentName.Name)));
                var hash = attachment.GetString(nameof(AttachmentName.Hash));
                if (name == names[1])
                {
                    Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", hash);
                }
                else if (name == names[2])
                {
                    Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", hash);
                }
                else if (name == names[0])
                {
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                }
            }

            var changeVector = session.Advanced.GetChangeVectorFor(revision);
            var readBuffer = new byte[8];
            for (var i = 0; i < names.Length; i++)
            {
                var name = names[i];
                if (orderedNames.Contains(name) == false)
                    continue;
                using (var attachmentStream = new MemoryStream(readBuffer))
                using (var attachment = session.Advanced.Attachments.GetRevision("users/1", name, changeVector))
                {
                    attachment.Stream.CopyTo(attachmentStream);
                    if (i >= expectedCount)
                    {
                        Assert.Null(attachment);
                        continue;
                    }

                    Assert.Equal(name, attachment.Details.Name);
                    if (name == names[0])
                    {
                        Assert.Equal(new byte[] { 1, 2, 3 }, readBuffer.Take(3));
                        Assert.Equal("image/png", attachment.Details.ContentType);
                        Assert.Equal(3, attachmentStream.Position);
                        Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachment.Details.Hash);
                    }
                    else if (name == names[1])
                    {
                        Assert.Equal(new byte[] { 10, 20, 30, 40, 50 }, readBuffer.Take(5));
                        Assert.Equal("ImGgE/jPeG", attachment.Details.ContentType);
                        Assert.Equal(5, attachmentStream.Position);
                        Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", attachment.Details.Hash);
                    }
                    else if (name == names[2])
                    {
                        Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, readBuffer.Take(5));
                        Assert.Equal("", attachment.Details.ContentType);
                        Assert.Equal(5, attachmentStream.Position);
                        Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", attachment.Details.Hash);
                    }
                }
            }
        }

        [Fact]
        public async Task PutDifferentAttachmentsShouldNotConflict()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a1/png"));
                    }

                    await session.StoreAsync(new User { Name = "Marker 1" }, "marker 1");
                    await session.SaveChangesAsync();
                }
                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a2 = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                    {
                        store2.Operations.Send(new PutAttachmentOperation("users/1", "a2", a2, "a2/jpeg"));
                    }

                    await session.StoreAsync(new User { Name = "Marker 2" }, "marker 2");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                Assert.True(WaitForDocument(store2, "marker 1"));
                Assert.True(WaitForDocument(store1, "marker 2"));

                await AssertAttachments(store1, new[] { "a1", "a2" });
                await AssertAttachments(store2, new[] { "a1", "a2" });
            }
        }

        private async Task AssertAttachments(DocumentStore store, string[] names)
        {
            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/1");
                var metadata = session.Advanced.GetMetadataFor(user);
                Assert.Contains(DocumentFlags.HasAttachments.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                Assert.Equal(names.Length, attachments.Length);
                for (int i = 0; i < names.Length; i++)
                {
                    Assert.Equal(names[i], attachments[i].GetString(nameof(AttachmentName.Name)));
                }
            }
        }

        [Fact]
        public async Task PutAndDeleteDifferentAttachmentsShouldNotConflict()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await SetDatabaseId(store1, new Guid("00000000-48c4-421e-9466-000000000000"));
                await SetDatabaseId(store2, new Guid("99999999-48c4-421e-9466-999999999999"));

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a1/png"));
                    }

                    await session.StoreAsync(new User { Name = "Marker 1" }, "marker 1");
                    await session.SaveChangesAsync();
                }
                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a2 = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                    {
                        store2.Operations.Send(new PutAttachmentOperation("users/1", "a2", a2, "a1/png"));
                    }
                    store2.Operations.Send(new DeleteAttachmentOperation("users/1", "a2"));

                    await session.StoreAsync(new User { Name = "Marker 2" }, "marker 2");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                Assert.True(WaitForDocument(store2, "marker 1"));
                Assert.True(WaitForDocument(store1, "marker 2"));

                await AssertAttachments(store1, new[] { "a1" });
                await AssertAttachments(store2, new[] { "a1" });
            }
        }

        [Fact]
        public async Task PutSameAttachmentsShouldNotConflict()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a1/png"));
                    }

                    await session.StoreAsync(new User { Name = "Marker 1" }, "marker 1");
                    await session.SaveChangesAsync();
                }
                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        store2.Operations.Send(new PutAttachmentOperation("users/1", "a1", a1, "a1/png"));
                    }

                    await session.StoreAsync(new User { Name = "Marker 2" }, "marker 2");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                Assert.True(WaitForDocument(store2, "marker 1"));
                Assert.True(WaitForDocument(store1, "marker 2"));

                await AssertAttachments(store1, new[] { "a1" });
                await AssertAttachments(store2, new[] { "a1" });
            }
        }

        [Fact]
        public async Task PutSameAttachmentsDifferentContentTypeShouldConflict()
        {
            using (var store1 = GetDocumentStore(options: new Options
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
            using (var store2 = GetDocumentStore(options: new Options
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
                await SetDatabaseId(store1, new Guid("00000000-48c4-421e-9466-000000000000"));
                await SetDatabaseId(store2, new Guid("99999999-48c4-421e-9466-999999999999"));

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a1/png"));
                    }
                }
                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        store2.Operations.Send(new PutAttachmentOperation("users/1", "a1", a1, "a2/jpeg"));
                    }
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                var conflicts = WaitUntilHasConflict(store1, "users/1");
                Assert.Equal(2, conflicts.Length);
                var hash = "EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=";
                AssertConflict(conflicts[0], "a1", hash, "a1/png", 3);
                AssertConflict(conflicts[1], "a1", hash, "a2/jpeg", 3);

                conflicts = WaitUntilHasConflict(store2, "users/1");
                Assert.Equal(2, conflicts.Length);
                AssertConflict(conflicts[0], "a1", hash, "a1/png", 3);
                AssertConflict(conflicts[1], "a1", hash, "a2/jpeg", 3);

                await ResolveConflict(store1, store2, conflicts[0].Doc, "a1", hash, "a1/png", 3);
            }
        }

        private async Task ResolveConflict(DocumentStore store1, DocumentStore store2, BlittableJsonReaderObject document,
            string name, string hash, string contentType, long size)
        {
            await store1.Commands().PutAsync("users/1", null, document);
            await AssertConflictResolved(store1, name, hash, contentType, size);

            WaitForMarker(store1, store2);
            await AssertConflictResolved(store2, name, hash, contentType, size);
        }

        private async Task AssertConflictResolved(DocumentStore store, string name, string hash, string contentType, long size)
        {
            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/1");
                var attachments = session.Advanced.Attachments.GetNames(user);
                var attachment = attachments.Single();
                Assert.Equal(name, attachment.Name);
                Assert.Equal(hash, attachment.Hash);
                Assert.Equal(contentType, attachment.ContentType);
                Assert.Equal(size, attachment.Size);
            }
        }

        [Fact]
        public async Task PutDifferentAttachmentsShouldConflict()
        {
            using (var store1 = GetDocumentStore(options: new Options
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
            using (var store2 = GetDocumentStore(options: new Options
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
                await SetDatabaseId(store1, new Guid("00000000-48c4-421e-9466-000000000000"));
                await SetDatabaseId(store2, new Guid("99999999-48c4-421e-9466-999999999999"));

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a1/png"));
                    }
                }
                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a2 = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                    {
                        store2.Operations.Send(new PutAttachmentOperation("users/1", "a1", a2, "a1/png"));
                    }
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                var hash1 = "EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=";
                var hash2 = "Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=";

                var conflicts = WaitUntilHasConflict(store1, "users/1");
                Assert.Equal(2, conflicts.Length);
                AssertConflict(conflicts[0], "a1", hash1, "a1/png", 3);
                AssertConflict(conflicts[1], "a1", hash2, "a1/png", 5);

                conflicts = WaitUntilHasConflict(store2, "users/1");
                Assert.Equal(2, conflicts.Length);
                AssertConflict(conflicts[0], "a1", hash1, "a1/png", 3);
                AssertConflict(conflicts[1], "a1", hash2, "a1/png", 5);

                await ResolveConflict(store1, store2, conflicts[1].Doc, "a1", hash2, "a1/png", 5);
            }
        }

        private void AssertConflict(GetConflictsResult.Conflict conflict, string name, string hash, string contentType, long size)
        {
            Assert.True(conflict.Doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));

            Assert.True(metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments));
            var attachment = (BlittableJsonReaderObject)attachments.Single();

            Assert.True(attachment.TryGet(nameof(AttachmentName.Name), out string actualName));
            Assert.Equal(name, actualName);
            Assert.True(attachment.TryGet(nameof(AttachmentName.Hash), out string actualHash));
            Assert.Equal(hash, actualHash);
            Assert.True(attachment.TryGet(nameof(AttachmentName.ContentType), out string actualContentType));
            Assert.Equal(contentType, actualContentType);
            Assert.True(attachment.TryGet(nameof(AttachmentName.Size), out long actualSize));
            Assert.Equal(size, actualSize);
        }

        private void AssertConflictNoAttachment(GetConflictsResult.Conflict conflict)
        {
            Assert.True(conflict.Doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
            Assert.False(metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray _));
        }

        [Fact]
        public async Task PutAndDeleteAttachmentsShouldNotConflict()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await SetDatabaseId(store1, new Guid("00000000-48c4-421e-9466-000000000000"));
                await SetDatabaseId(store2, new Guid("99999999-48c4-421e-9466-999999999999"));

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a1/png"));
                    }

                    await session.StoreAsync(new User { Name = "Marker 1" }, "marker 1");
                    await session.SaveChangesAsync();
                }
                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a2 = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                    {
                        store2.Operations.Send(new PutAttachmentOperation("users/1", "a1", a2, "a1/png"));
                    }
                    store2.Operations.Send(new DeleteAttachmentOperation("users/1", "a1"));

                    await session.StoreAsync(new User { Name = "Marker 2" }, "marker 2");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                Assert.True(WaitForDocument(store2, "marker 1"));
                Assert.True(WaitForDocument(store1, "marker 2"));

                await AssertAttachments(store1, new[] { "a1" });
                await AssertAttachments(store2, new[] { "a1" });
            }
        }

        [Fact]
        public async Task PutAndDeleteAttachmentsShouldNotConflict_OnDocumentWithoutMetadata()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await SetDatabaseId(store1, new Guid("00000000-48c4-421e-9466-000000000000"));
                await SetDatabaseId(store2, new Guid("99999999-48c4-421e-9466-999999999999"));

                using (var session = store1.OpenAsyncSession())
                {
                    using (var commands = store1.Commands())
                    {
                        await commands.PutAsync("users/1", null, new User { Name = "Fitzchak" });
                    }

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a1/png"));
                    }

                    await session.StoreAsync(new User { Name = "Marker 1" }, "marker 1");
                    await session.SaveChangesAsync();
                }
                using (var session = store2.OpenAsyncSession())
                {
                    using (var commands = store2.Commands())
                    {
                        await commands.PutAsync("users/1", null, new User { Name = "Fitzchak" });
                    }

                    using (var a2 = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                    {
                        await store2.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a2, "a1/png"));
                    }
                    await store2.Operations.SendAsync(new DeleteAttachmentOperation("users/1", "a1"));

                    await session.StoreAsync(new User { Name = "Marker 2" }, "marker 2");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                Assert.True(WaitForDocument(store2, "marker 1"));
                Assert.True(WaitForDocument(store1, "marker 2"));

                await AssertAttachments(store1, new[] { "a1" });
                await AssertAttachments(store2, new[] { "a1" });
            }
        }

        [Fact]
        public async Task RavenDB_13535()
        {
            var co = new ServerCreationOptions
            {
                RunInMemory = false,
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Replication.MaxSizeToSend)] = 1.ToString()
                },
                RegisterForDisposal = false
            };
            using (var server = GetNewServer(co))
            using (var store1 = GetDocumentStore(new Options { Server = server, RunInMemory = false }))
            using (var store2 = GetDocumentStore(new Options { Server = server, RunInMemory = false }))
            using (var store3 = GetDocumentStore(new Options { Server = server, RunInMemory = false }))
            {
                using (var session = store1.OpenAsyncSession())
                using (var a1 = new MemoryStream(new byte[2 * 1024 * 1024]))
                {
                    var user = new User();
                    await session.StoreAsync(user, "foo");
                    session.Advanced.Attachments.Store(user, "dummy", a1);
                    await session.SaveChangesAsync();
                }

                using (var session = store1.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("foo");
                    user.Name = "Karmel";
                    await session.SaveChangesAsync();
                }

                var db1 = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database);
                var mre = new ManualResetEventSlim(true);
                db1.ReplicationLoader.DebugWaitAndRunReplicationOnce = mre;

                await SetupReplicationAsync(store1, store2);

                var db2 = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store2.Database);

                var count = WaitForValue(() =>
                {
                    using (db2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        return db2.DocumentsStorage.AttachmentsStorage.GetNumberOfAttachments(ctx).AttachmentCount;
                    }
                }, 1);
                Assert.Equal(1, count);

                db2.ServerStore.DatabasesLandlord.UnloadDirectly(db2.Name);

                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "bar");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store2, store3);

                var db3 = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store3.Database);
                count = WaitForValue(() =>
                {
                    using (db3.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        return db3.DocumentsStorage.AttachmentsStorage.GetNumberOfAttachments(ctx).AttachmentCount;
                    }
                }, 1);
                Assert.Equal(1, count);
            }
        }

        [Fact]
        public async Task RavenDB_13963()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var database1 = await GetDocumentDatabaseInstanceFor(store1);
                database1.Configuration.Replication.MaxItemsCount = 1;
                database1.ReplicationLoader.DebugWaitAndRunReplicationOnce = new ManualResetEventSlim();

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Karmel"}, "users/1");
                    using (var a1 = new MemoryStream(new byte[] {1, 2, 3}))
                    {
                        session.Advanced.Attachments.Store("users/1", "a1", a1, "a1/png");
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/2");
                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3, 4 }))
                    {
                        session.Advanced.Attachments.Store("users/2", "a2", a1, "a2/png");
                        await session.SaveChangesAsync();
                    }
                }

                await SetupReplicationAsync(store1, store2);
                database1.ReplicationLoader.DebugWaitAndRunReplicationOnce.Set();

                Assert.True(WaitForDocument(store2, "users/1"));

                using (var session = store2.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.NotNull(user);

                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Contains(DocumentFlags.HasAttachments.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Assert.Equal(1, attachments.Length);

                    Assert.Null(await session.LoadAsync<User>("users/2"));
                }

                database1.ReplicationLoader.DebugWaitAndRunReplicationOnce.Set();
                Assert.True(WaitForDocument(store2, "users/2"));

                using (var session = store2.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/2");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Contains(DocumentFlags.HasAttachments.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Assert.Equal(1, attachments.Length);
                }
            }
        }

        [Fact]
        public async Task RavenDB_15914()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Replication.RetryReplicateAfter)] = "1";

            var databaseName = GetDatabaseName();
            var cluster = await CreateRaftCluster(3, shouldRunInMemory: false, watcherCluster: true);

            var mainServer = cluster.Nodes[0];
            var toRemove = mainServer.ServerStore.NodeTag;
            using (var store = GetDocumentStore(new Options
            {
                Server = mainServer,
                ModifyDatabaseName = s => databaseName,
                ReplicationFactor = 3,
                ModifyDocumentStore = s=>s.Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }))
            using (var temp = GetDocumentStore(new Options
            {
                Server = cluster.Nodes[1],
                ModifyDatabaseName = s => databaseName,
                CreateDatabase = false
            }))
            {
                await using (var ms = new MemoryStream(new byte[]{1,2,3,4}))
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    var user = new Core.Utils.Entities.User();
                    await session.StoreAsync(user, "users/1");
                    session.Advanced.Attachments.Store(user, "dummy", ms);
                    await session.SaveChangesAsync();
                }

                await using (var ms = new MemoryStream(new byte[]{1,2,3,4,5}))
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    var user = new Core.Utils.Entities.User();
                    await session.StoreAsync(user, "users/2");
                    session.Advanced.Attachments.Store(user, "dummy2", ms);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Advanced.Attachments.Delete("users/1", "dummy");
                    await session.SaveChangesAsync();
                }

                var tasks = new List<Task>
                {
                    EnsureNoReplicationLoop(cluster.Nodes[0], databaseName),
                    EnsureNoReplicationLoop(cluster.Nodes[1], databaseName),
                    EnsureNoReplicationLoop(cluster.Nodes[2], databaseName)
                };
                await Task.WhenAll(tasks);
                tasks.ForEach(t => t.Wait());

                var result = await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: toRemove, timeToWaitForConfirmation: TimeSpan.FromSeconds(60)));
                await mainServer.ServerStore.Cluster.WaitForIndexNotification(result.RaftCommandIndex + 1);

                using (var session = temp.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 1);
                    var user = new Core.Utils.Entities.User();
                    await session.StoreAsync(user, "users/3");
                    await session.SaveChangesAsync();
                }

                await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(databaseName, toRemove));

                Assert.True(await WaitForDocumentInClusterAsync<Core.Utils.Entities.User>(new DatabaseTopology {Members = new List<string> {"A", "B", "C"}}, databaseName, "users/1", null,
                    timeout: TimeSpan.FromSeconds(60)));

                tasks = new List<Task>
                {
                    EnsureNoReplicationLoop(cluster.Nodes[0], databaseName),
                    EnsureNoReplicationLoop(cluster.Nodes[1], databaseName),
                    EnsureNoReplicationLoop(cluster.Nodes[2], databaseName)
                };
                await Task.WhenAll(tasks);
                tasks.ForEach(t => t.Wait());
            }
        }

        [Fact]
        public async Task AttachmentWithDifferentStreamAndSameNameShouldBeResolvedToLatestAfterConflict()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "EGR" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3, 4 }))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a2/jpeg"));
                    }
                }

                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "EGOR" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        store2.Operations.Send(new PutAttachmentOperation("users/1", "a1", a1, "a2/jpeg"));
                    }
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);
                WaitForDocumentWithAttachmentToReplicate<User>(store1, "users/1", "a1", Debugger.IsAttached ? 60000 : 15000);
                WaitForDocumentWithAttachmentToReplicate<User>(store2, "users/1", "a1", Debugger.IsAttached ? 60000 : 15000);
                WaitForMarker(store1, store2);

                using (var session = store1.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("EGOR", user.Name);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(1, attachments.Length);

                    Assert.Equal("a1", attachments[0].Name);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[0].Hash);
                    Assert.Equal("a2/jpeg", attachments[0].ContentType);
                    Assert.Equal(3, attachments[0].Size);
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("EGOR", user.Name);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(1, attachments.Length);
                    Assert.Equal("a1", attachments[0].Name);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[0].Hash);
                    Assert.Equal("a2/jpeg", attachments[0].ContentType);
                    Assert.Equal(3, attachments[0].Size);
                }
            }
        }

        [Fact]
        public async Task SameAttachmentWithDuplicateNameShouldBeNotChangeAfterConflict()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "EGR" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a2/jpeg"));
                    }
                }

                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "EGOR" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        store2.Operations.Send(new PutAttachmentOperation("users/1", "a1", a1, "a2/jpeg"));
                    }
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);
                WaitForDocumentWithAttachmentToReplicate<User>(store1, "users/1", "a1", Debugger.IsAttached ? 60000 : 15000);
                WaitForDocumentWithAttachmentToReplicate<User>(store2, "users/1", "a1", Debugger.IsAttached ? 60000 : 15000);
                WaitForMarker(store1, store2);

                using (var session = store1.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("EGOR", user.Name);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(1, attachments.Length);
                    Assert.Equal("a1", attachments[0].Name);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[0].Hash);
                    Assert.Equal("a2/jpeg", attachments[0].ContentType);
                    Assert.Equal(3, attachments[0].Size);
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("EGOR", user.Name);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(1, attachments.Length);
                    Assert.Equal("a1", attachments[0].Name);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[0].Hash);
                    Assert.Equal("a2/jpeg", attachments[0].ContentType);
                    Assert.Equal(3, attachments[0].Size);
                }
            }
        }

        [Fact]
        public async Task AttachmentWithSameStreamSameNameAndDifferentContentTypeShouldBeResolvedToLatestAfterConflict()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "EGR" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a1/jpeg"));
                    }
                }

                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "EGOR" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        store2.Operations.Send(new PutAttachmentOperation("users/1", "a1", a1, "a2/jpeg"));
                    }
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);
                WaitForDocumentWithAttachmentToReplicate<User>(store1, "users/1", "a1", Debugger.IsAttached ? 60000 : 15000);
                WaitForDocumentWithAttachmentToReplicate<User>(store2, "users/1", "a1", Debugger.IsAttached ? 60000 : 15000);
                WaitForMarker(store1, store2);

                using (var session = store1.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("EGOR", user.Name);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(1, attachments.Length);
                    Assert.Equal("a1", attachments[0].Name);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[0].Hash);
                    Assert.Equal("a2/jpeg", attachments[0].ContentType);
                    Assert.Equal(3, attachments[0].Size);
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("EGOR", user.Name);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(1, attachments.Length);
                    Assert.Equal("a1", attachments[0].Name);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[0].Hash);
                    Assert.Equal("a2/jpeg", attachments[0].ContentType);
                    Assert.Equal(3, attachments[0].Size);
                }
            }
        }

        [Fact]
        public async Task ResolvedToLatestOnAttachmentConflictShouldRemoveDuplicateAttachment()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "EGR" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a1/jpeg"));
                    }
                }

                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "EGOR" }, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        store2.Operations.Send(new PutAttachmentOperation("users/1", "a1", a1, "a2/jpeg"));
                    }
                }

                var externalList1 = await SetupReplicationAsync(store1, store2);
                var externalList2 = await SetupReplicationAsync(store2, store1);
                WaitForDocumentWithAttachmentToReplicate<User>(store1, "users/1", "a1", Debugger.IsAttached ? 60000 : 15000);
                WaitForDocumentWithAttachmentToReplicate<User>(store2, "users/1", "a1", Debugger.IsAttached ? 60000 : 15000);
                WaitForMarker(store1, store2);

                using (var session = store1.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("EGOR", user.Name);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(1, attachments.Length);
                    Assert.Equal("a1", attachments[0].Name);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[0].Hash);
                    Assert.Equal("a2/jpeg", attachments[0].ContentType);
                    Assert.Equal(3, attachments[0].Size);
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("EGOR", user.Name);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(1, attachments.Length);
                    Assert.Equal("a1", attachments[0].Name);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[0].Hash);
                    Assert.Equal("a2/jpeg", attachments[0].ContentType);
                    Assert.Equal(3, attachments[0].Size);
                }

                var db1 = await GetDocumentDatabaseInstanceFor(store1);
                var db2 = await GetDocumentDatabaseInstanceFor(store2);
                var replicationConnection1 = db1.ReplicationLoader.OutgoingHandlers.First();
                var replicationConnection2 = db2.ReplicationLoader.OutgoingHandlers.First();

                var external1 = new ExternalReplication(store1.Database, $"ConnectionString-{store2.Identifier}")
                {
                    TaskId = externalList1.First().TaskId,
                    Disabled = true
                };
                var external2 = new ExternalReplication(store2.Database, $"ConnectionString-{store1.Identifier}")
                {
                    TaskId = externalList2.First().TaskId,
                    Disabled = true
                };
                var res1 = await store1.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external1));
                Assert.Equal(externalList1.First().TaskId, res1.TaskId);
                var res2 = await store2.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external2));
                Assert.Equal(externalList2.First().TaskId, res2.TaskId);

                await db1.ServerStore.Cluster.WaitForIndexNotification(res1.RaftCommandIndex);
                await db2.ServerStore.Cluster.WaitForIndexNotification(res1.RaftCommandIndex);
                await db1.ServerStore.Cluster.WaitForIndexNotification(res2.RaftCommandIndex);
                await db2.ServerStore.Cluster.WaitForIndexNotification(res2.RaftCommandIndex);

                Assert.True(await WaitForValueAsync(() => replicationConnection1.IsConnectionDisposed, true));
                Assert.True(await WaitForValueAsync(() => replicationConnection2.IsConnectionDisposed, true));

                using (var a1 = new MemoryStream(new byte[] { 1, 2, 3, 4 }))
                {
                    await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a1/jpeg"));
                }
                using (var a1 = new MemoryStream(new byte[] { 1, 2, 3, 5 }))
                {
                    await store2.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a1/jpeg"));
                }
                external1.Disabled = false;
                external2.Disabled = false;

                var res3 = await store1.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external1));
                Assert.Equal(externalList1.First().TaskId, res3.TaskId);
                var res4 = await store2.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external2));
                Assert.Equal(externalList2.First().TaskId, res4.TaskId);

                await db1.ServerStore.Cluster.WaitForIndexNotification(res3.RaftCommandIndex);
                await db2.ServerStore.Cluster.WaitForIndexNotification(res3.RaftCommandIndex);
                await db1.ServerStore.Cluster.WaitForIndexNotification(res4.RaftCommandIndex);
                await db2.ServerStore.Cluster.WaitForIndexNotification(res4.RaftCommandIndex);

                WaitForDocumentWithAttachmentToReplicate<User>(store1, "users/1", "a1", Debugger.IsAttached ? 60000 : 15000);
                WaitForDocumentWithAttachmentToReplicate<User>(store2, "users/1", "a1", Debugger.IsAttached ? 60000 : 15000);
                WaitForMarker(store1, store2);
                using (var session = store1.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("EGOR", user.Name);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(1, attachments.Length);
                    Assert.Equal("a1", attachments[0].Name);
                    Assert.Equal("XiUNwy+pPQdTVBunU26rVydiLOd3Iqgtz4lkmZVfSs4=", attachments[0].Hash);
                    Assert.Equal("a1/jpeg", attachments[0].ContentType);
                    Assert.Equal(4, attachments[0].Size);
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("EGOR", user.Name);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(1, attachments.Length);
                    Assert.Equal("a1", attachments[0].Name);
                    Assert.Equal("XiUNwy+pPQdTVBunU26rVydiLOd3Iqgtz4lkmZVfSs4=", attachments[0].Hash);
                    Assert.Equal("a1/jpeg", attachments[0].ContentType);
                    Assert.Equal(4, attachments[0].Size);
                }
            }
        }

        [Fact]
        public async Task ScriptResolver_ShouldNotRenameAttachment_ShouldRenameAllMissingAttachmentsAndMergeWithOtherAttachmentsOnResolvedDocument()
        {
            using (var store1 = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                        {
                            {
                                "Users", new ScriptResolver()
                                {
                                    Script = @"docs[0]['@metadata']['@attachments'][0]['Name'] = 'newName';
                                               docs[0].Name = docs[0].Name + '_RESOLVED';
                                               return docs[0];
                                               "
                                }
                            }
                        }
                    };
                }
            }))
            using (var store2 = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                        {
                            {
                                "Users", new ScriptResolver()
                                {
                                    Script = @"docs[0]['@metadata']['@attachments'][0]['Name'] = 'newName';
                                               docs[0].Name = docs[0].Name + '_RESOLVED';
                                               return docs[0];
                                               "
                                }
                            }
                        }
                    };
                }
            }))
            {
                var cvs = new List<(string, string)>();
                using (var session = store1.OpenAsyncSession())
                {
                    var u = new User { Name = "EGR" };
                    await session.StoreAsync(u, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3, 4 }))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a2/jpeg"));
                    }

                    await session.Advanced.RefreshAsync(u);
                    var cv = session.Advanced.GetChangeVectorFor(u);
                    cvs.Add(("EGR", cv));
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var u = new User {Name = "EGOR"};
                    await session.StoreAsync(u, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        store2.Operations.Send(new PutAttachmentOperation("users/1", "a1", a1, "a2/jpeg"));
                    }

                    await session.Advanced.RefreshAsync(u);
                    var cv = session.Advanced.GetChangeVectorFor(u);
                    cvs.Add(("EGOR", cv));
                }

                cvs.Sort((x, y) => string.Compare(x.Item2, y.Item2, StringComparison.Ordinal));
                var orderedDocsByEtag = cvs;
                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);
                WaitForDocumentWithAttachmentToReplicate<User>(store1, "users/1", "RESOLVED_#1_a1", Debugger.IsAttached ? 60000 : 15000);
                WaitForDocumentWithAttachmentToReplicate<User>(store2, "users/1", "RESOLVED_#1_a1", Debugger.IsAttached ? 60000 : 15000);
                WaitForMarker(store1, store2);

                var name = orderedDocsByEtag.First().Item1;
                var expectedName = name + "_RESOLVED";
                using (var session = store1.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal(expectedName, user.Name);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(2, attachments.Length);

                    Assert.Equal("a1", attachments[0].Name);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[0].Hash);
                    Assert.Equal("a2/jpeg", attachments[0].ContentType);
                    Assert.Equal(3, attachments[0].Size);

                    Assert.Equal("RESOLVED_#0_a1", attachments[1].Name);
                    Assert.Equal("KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=", attachments[1].Hash);
                    Assert.Equal("a2/jpeg", attachments[1].ContentType);
                    Assert.Equal(4, attachments[1].Size);
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal(expectedName, user.Name);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(2, attachments.Length);

                    Assert.Equal("a1", attachments[0].Name);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[0].Hash);
                    Assert.Equal("a2/jpeg", attachments[0].ContentType);
                    Assert.Equal(3, attachments[0].Size);

                    Assert.Equal("RESOLVED_#0_a1", attachments[1].Name);
                    Assert.Equal("KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=", attachments[1].Hash);
                    Assert.Equal("a2/jpeg", attachments[1].ContentType);
                    Assert.Equal(4, attachments[1].Size);
                }
            }
        }

        [Fact]
        public async Task ScriptResolver_ShouldNotRenameAttachment_ShouldRenameAndMergeAllMissingAttachmentsOnResolvedDocument()
        {
            using (var store1 = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                        {
                            {
                                "Users", new ScriptResolver()
                                {
                                    Script = @"docs[0]['@metadata']['@attachments'][0]['Name'] = 'newName';
                                               docs[0].Name = docs[0].Name + '_RESOLVED';
                                               return docs[0];
                                               "
                                }
                            }
                        }
                    };
                }
            }))
            using (var store2 = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                        {
                            {
                                "Users", new ScriptResolver()
                                {
                                    Script = @"docs[0]['@metadata']['@attachments'][0]['Name'] = 'newName';
                                               docs[0].Name = docs[0].Name + '_RESOLVED';
                                               return docs[0];
                                               "
                                }
                            }
                        }
                    };
                }
            }))
            {
                var cvs = new List<(string, string)>();
                using (var session = store1.OpenAsyncSession())
                {
                    var u = new User { Name = "EGOR" };
                    await session.StoreAsync(u, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3, 4 }))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a2/jpeg"));
                        a1.Position = 0;
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a10", a1, "a2/jpeg"));
                    }

                    await session.Advanced.RefreshAsync(u);
                    var cv = session.Advanced.GetChangeVectorFor(u);
                    cvs.Add(("EGOR", cv));
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var u = new User { Name = "EGR" };
                    await session.StoreAsync(u, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        store2.Operations.Send(new PutAttachmentOperation("users/1", "a1", a1, "a2/jpeg"));
                    }

                    await session.Advanced.RefreshAsync(u);
                    var cv = session.Advanced.GetChangeVectorFor(u);
                    cvs.Add(("EGR", cv));
                }

                cvs.Sort((x, y) => string.Compare(x.Item2, y.Item2, StringComparison.Ordinal));
                var orderedDocsByEtag = cvs;
                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);
                WaitForDocumentWithAttachmentToReplicate<User>(store1, "users/1", "RESOLVED_#1_a1", Debugger.IsAttached ? 60000 : 15000);
                WaitForDocumentWithAttachmentToReplicate<User>(store2, "users/1", "RESOLVED_#1_a1", Debugger.IsAttached ? 60000 : 15000);
                WaitForMarker(store1, store2);

                var name = orderedDocsByEtag.First().Item1;
                var expectedName = name + "_RESOLVED";
                using (var session = store1.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal(expectedName, user.Name);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(3, attachments.Length);

                    Assert.Equal("a1", attachments[0].Name);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[0].Hash);
                    Assert.Equal("a2/jpeg", attachments[0].ContentType);
                    Assert.Equal(3, attachments[0].Size);

                    Assert.Equal("a10", attachments[1].Name);
                    Assert.Equal("KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=", attachments[1].Hash);
                    Assert.Equal("a2/jpeg", attachments[1].ContentType);
                    Assert.Equal(4, attachments[1].Size);

                    Assert.Equal("RESOLVED_#0_a1", attachments[2].Name);
                    Assert.Equal("KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=", attachments[2].Hash);
                    Assert.Equal("a2/jpeg", attachments[2].ContentType);
                    Assert.Equal(4, attachments[2].Size);
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal(expectedName, user.Name);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(3, attachments.Length);

                    Assert.Equal("a1", attachments[0].Name);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[0].Hash);
                    Assert.Equal("a2/jpeg", attachments[0].ContentType);
                    Assert.Equal(3, attachments[0].Size);

                    Assert.Equal("a10", attachments[1].Name);
                    Assert.Equal("KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=", attachments[1].Hash);
                    Assert.Equal("a2/jpeg", attachments[1].ContentType);
                    Assert.Equal(4, attachments[1].Size);

                    Assert.Equal("RESOLVED_#0_a1", attachments[2].Name);
                    Assert.Equal("KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=", attachments[2].Hash);
                    Assert.Equal("a2/jpeg", attachments[2].ContentType);
                    Assert.Equal(4, attachments[2].Size);
                }
            }
        }

        [Fact]
        public async Task ScriptResolver_ShouldNotRemoveAttachment_ShouldRenameAndMergeAllMissingAttachmentsOnResolvedDocument()
        {
            using (var store1 = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                        {
                            {
                                "Users", new ScriptResolver()
                                {
                                    Script = @"docs[0]['@metadata']['@attachments'].pop();
                                               docs[0].Name = docs[0].Name + '_RESOLVED';
                                               return docs[0];
                                               "
                                }
                            }
                        }
                    };
                }
            }))
            using (var store2 = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                        {
                            {
                                "Users", new ScriptResolver()
                                {
                                    Script = @"docs[0]['@metadata']['@attachments'].pop();
                                               docs[0].Name = docs[0].Name + '_RESOLVED';
                                               return docs[0];
                                               "
                                }
                            }
                        }
                    };
                }
            }))
            {
                var cvs = new List<(string, string, string)>();
                using (var session = store1.OpenAsyncSession())
                {
                    var u = new User {Name = "EGR"};
                    await session.StoreAsync(u, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3, 4 }))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a2/jpeg"));
                        a1.Position = 0;
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a10", a1, "a2/jpeg"));
                    }

                    await session.Advanced.RefreshAsync(u);
                    var cv = session.Advanced.GetChangeVectorFor(u);
                    cvs.Add(("EGR", cv, "KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I="));
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var u = new User {Name = "EGOR"};
                    await session.StoreAsync(u, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        store2.Operations.Send(new PutAttachmentOperation("users/1", "a1", a1, "a2/jpeg"));
                    }

                    await session.Advanced.RefreshAsync(u);
                    var cv = session.Advanced.GetChangeVectorFor(u);
                    cvs.Add(("EGOR", cv, "EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo="));
                }

                cvs.Sort((x, y) => string.Compare(x.Item2, y.Item2, StringComparison.Ordinal));
                var orderedDocsByEtag = cvs;
                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);
                WaitForDocumentWithAttachmentToReplicate<User>(store1, "users/1", "a1", Debugger.IsAttached ? 60000 : 15000);
                WaitForDocumentWithAttachmentToReplicate<User>(store2, "users/1", "a1", Debugger.IsAttached ? 60000 : 15000);
                WaitForMarker(store1, store2);

                var name = orderedDocsByEtag.First().Item1;
                var expectedName = name + "_RESOLVED";
                using (var session = store1.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal(expectedName, user.Name);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(3, attachments.Length);
                    if (name == "EGOR")
                    {
                        Assert.Equal("a1", attachments[0].Name);
                        Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[0].Hash);
                        Assert.Equal("a2/jpeg", attachments[0].ContentType);
                        Assert.Equal(3, attachments[0].Size);

                        Assert.Equal("a10", attachments[1].Name);
                        Assert.Equal("KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=", attachments[1].Hash);
                        Assert.Equal("a2/jpeg", attachments[1].ContentType);
                        Assert.Equal(4, attachments[1].Size);

                        Assert.Equal("RESOLVED_#0_a1", attachments[2].Name);
                        Assert.Equal("KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=", attachments[2].Hash);
                        Assert.Equal("a2/jpeg", attachments[2].ContentType);
                        Assert.Equal(4, attachments[2].Size);
                    }
                    else
                    {
                        Assert.Equal("a1", attachments[0].Name);
                        Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[0].Hash);
                        Assert.Equal("a2/jpeg", attachments[0].ContentType);
                        Assert.Equal(3, attachments[0].Size);

                        Assert.Equal("a10", attachments[1].Name);
                        Assert.Equal("KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=", attachments[1].Hash);
                        Assert.Equal("a2/jpeg", attachments[1].ContentType);
                        Assert.Equal(4, attachments[1].Size);

                        Assert.Equal("RESOLVED_#0_a1", attachments[2].Name);
                        Assert.Equal("KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=", attachments[2].Hash);
                        Assert.Equal("a2/jpeg", attachments[2].ContentType);
                        Assert.Equal(4, attachments[2].Size);
                    }
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal(expectedName, user.Name);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(3, attachments.Length);

                    if (name == "EGOR")
                    {
                        Assert.Equal("a1", attachments[0].Name);
                        Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[0].Hash);
                        Assert.Equal("a2/jpeg", attachments[0].ContentType);
                        Assert.Equal(3, attachments[0].Size);

                        Assert.Equal("a10", attachments[1].Name);
                        Assert.Equal("KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=", attachments[1].Hash);
                        Assert.Equal("a2/jpeg", attachments[1].ContentType);
                        Assert.Equal(4, attachments[1].Size);

                        Assert.Equal("RESOLVED_#0_a1", attachments[2].Name);
                        Assert.Equal("KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=", attachments[2].Hash);
                        Assert.Equal("a2/jpeg", attachments[2].ContentType);
                        Assert.Equal(4, attachments[2].Size);
                    }
                    else
                    {
                        Assert.Equal("a1", attachments[0].Name);
                        Assert.Equal("KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=", attachments[0].Hash);
                        Assert.Equal("a2/jpeg", attachments[0].ContentType);
                        Assert.Equal(4, attachments[0].Size);

                        Assert.Equal("a10", attachments[1].Name);
                        Assert.Equal("KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=", attachments[1].Hash);
                        Assert.Equal("a2/jpeg", attachments[1].ContentType);
                        Assert.Equal(4, attachments[1].Size);

                        Assert.Equal("RESOLVED_#0_a1", attachments[2].Name);
                        Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[2].Hash);
                        Assert.Equal("a2/jpeg", attachments[2].ContentType);
                        Assert.Equal(3, attachments[2].Size);
                    }
                }
            }
        }

        [Fact]
        public async Task ScriptResolver_ShouldNotAddAttachment_ShouldRenameAndMergeDuplicateAttachments()
        {
            using (var store1 = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                        {
                            {
                                "Users", new ScriptResolver()
                                {
                                    Script = @"var attachment = {Name:'newnewnew', Hash:'322+228skHmEGRg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=', Size:50, ContentType:'image/gif'};
                                               docs[0]['@metadata']['@attachments'].push(attachment);
                                               docs[0].Name = docs[0].Name + '_RESOLVED';
                                               return docs[0];
                                               "
                                }
                            }
                        }
                    };
                }
            }))
            using (var store2 = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                        {
                            {
                                "Users", new ScriptResolver()
                                {
                                    Script = @"var attachment = {Name:'newnewnew', Hash:'322+228skHmEGRg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=', Size:50, ContentType:'image/gif'};
                                               docs[0]['@metadata']['@attachments'].push(attachment);
                                               docs[0].Name = docs[0].Name + '_RESOLVED';
                                               return docs[0];
                                               "
                                }
                            }
                        }
                    };
                }
            }))
            {
                var cvs = new List<(string, string, string)>();
                using (var session = store1.OpenAsyncSession())
                {
                    var u = new User { Name = "EGOR" };
                    await session.StoreAsync(u, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3, 4 }))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a2/jpeg"));
                        a1.Position = 0;
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a10", a1, "a2/jpeg"));
                    }

                    await session.Advanced.RefreshAsync(u);
                    var cv = session.Advanced.GetChangeVectorFor(u);
                    cvs.Add(("EGOR", cv, "KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I="));
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var u = new User {Name = "EGR"};
                    await session.StoreAsync(u, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        store2.Operations.Send(new PutAttachmentOperation("users/1", "a1", a1, "a2/jpeg"));
                    }

                    await session.Advanced.RefreshAsync(u);
                    var cv = session.Advanced.GetChangeVectorFor(u);
                    cvs.Add(("EGR", cv, "EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo="));
                }

                cvs.Sort((x, y) => string.Compare(x.Item2, y.Item2, StringComparison.Ordinal));
                var orderedDocsByEtag = cvs;
                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);
                WaitForDocumentWithAttachmentToReplicate<User>(store1, "users/1", "RESOLVED_#0_a1", Debugger.IsAttached ? 60000 : 15000);
                WaitForDocumentWithAttachmentToReplicate<User>(store2, "users/1", "RESOLVED_#0_a1", Debugger.IsAttached ? 60000 : 15000);
                WaitForMarker(store1, store2);

                var name = orderedDocsByEtag.First().Item1;
                var expectedName = name + "_RESOLVED";
                using (var session = store1.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal(expectedName, user.Name);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(3, attachments.Length);

                    Assert.Equal("a1", attachments[0].Name);
                    Assert.Equal(orderedDocsByEtag.First().Item3, attachments[0].Hash);
                    Assert.Equal("a2/jpeg", attachments[0].ContentType);
                    Assert.Equal(orderedDocsByEtag.First().Item1.Length, attachments[0].Size);

                    Assert.Equal("a10", attachments[1].Name);
                    Assert.Equal("KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=", attachments[1].Hash);
                    Assert.Equal("a2/jpeg", attachments[1].ContentType);
                    Assert.Equal(4, attachments[1].Size);

                    Assert.Equal("RESOLVED_#0_a1", attachments[2].Name);
                    Assert.Equal(orderedDocsByEtag.Last().Item3, attachments[2].Hash);
                    Assert.Equal("a2/jpeg", attachments[2].ContentType);
                    Assert.Equal(orderedDocsByEtag.Last().Item1.Length, attachments[2].Size);
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal(expectedName, user.Name);
                    var attachments = session.Advanced.Attachments.GetNames(user);
                    Assert.Equal(3, attachments.Length);

                    Assert.Equal("a1", attachments[0].Name);
                    Assert.Equal(orderedDocsByEtag.First().Item3, attachments[0].Hash);
                    Assert.Equal("a2/jpeg", attachments[0].ContentType);
                    Assert.Equal(orderedDocsByEtag.First().Item1.Length, attachments[0].Size);

                    Assert.Equal("a10", attachments[1].Name);
                    Assert.Equal("KFF+TN9skHmMGpg7A3J8p3Q8IaOIBnJCnM/FvRXqX3I=", attachments[1].Hash);
                    Assert.Equal("a2/jpeg", attachments[1].ContentType);
                    Assert.Equal(4, attachments[1].Size);

                    Assert.Equal("RESOLVED_#0_a1", attachments[2].Name);
                    Assert.Equal(orderedDocsByEtag.Last().Item3, attachments[2].Hash);
                    Assert.Equal("a2/jpeg", attachments[2].ContentType);
                    Assert.Equal(orderedDocsByEtag.Last().Item1.Length, attachments[2].Size);
                }
            }
        }
    }
}
