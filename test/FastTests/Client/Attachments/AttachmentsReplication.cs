using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Documents.Versioning;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations;
using Xunit;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Server.Documents;
using Sparrow.Json;

namespace FastTests.Client.Attachments
{
    public class AttachmentsReplication : ReplicationTestsBase
    {
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void PutAttachments(bool replicateDocumentFirst)
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }
                if (replicateDocumentFirst)
                {
                    SetupAttachmentReplication(store1, store2, false);
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
                    Assert.Equal(2 + (replicateDocumentFirst ? 2 : 0), result.Etag);
                    Assert.Equal(names[0], result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("image/png", result.ContentType);
                    Assert.Equal("JCS/B3EIIB2gNVjsXTCD1aXlTgzuEz50", result.Hash);
                }
                using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                {
                    var result = store1.Operations.Send(new PutAttachmentOperation("users/1", names[1], backgroundStream, "ImGgE/jPeG"));
                    Assert.Equal(4 + (replicateDocumentFirst ? 2 : 0), result.Etag);
                    Assert.Equal(names[1], result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("ImGgE/jPeG", result.ContentType);
                    Assert.Equal("mpqSy7Ky+qPhkBwhLiiM2no82Wvo9gQw", result.Hash);
                }
                using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    var result = store1.Operations.Send(new PutAttachmentOperation("users/1", names[2], fileStream, null));
                    Assert.Equal(6 + (replicateDocumentFirst ? 2 : 0), result.Etag);
                    Assert.Equal(names[2], result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("", result.ContentType);
                    Assert.Equal("PN5EZXRY470m7BLxu9MsOi/WwIRIq4WN", result.Hash);
                }

                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Marker" }, "marker");
                    session.SaveChanges();
                }
                if (replicateDocumentFirst == false)
                {
                    SetupAttachmentReplication(store1, store2, false);
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
                        Assert.Equal(name, attachment.GetString(nameof(Attachment.Name)));
                        var hash = attachment.GetString(nameof(AttachmentResult.Hash));
                        if (i == 0)
                        {
                            Assert.Equal("mpqSy7Ky+qPhkBwhLiiM2no82Wvo9gQw", hash);
                        }
                        else if (i == 1)
                        {
                            Assert.Equal("PN5EZXRY470m7BLxu9MsOi/WwIRIq4WN", hash);
                        }
                        else if (i == 2)
                        {
                            Assert.Equal("JCS/B3EIIB2gNVjsXTCD1aXlTgzuEz50", hash);
                        }
                    }
                }

                AttachmentsCrud.AssertAttachmentCount(store2, 3, 3, 2);

                using (var session = store2.OpenSession())
                {
                    var readBuffer = new byte[8];
                    for (var i = 0; i < names.Length; i++)
                    {
                        var name = names[i];
                        using (var attachmentStream = new MemoryStream(readBuffer))
                        {
                            var attachment = session.Advanced.GetAttachment("users/1", name, (result, stream) => stream.CopyTo(attachmentStream));
                            if (replicateDocumentFirst)
                            {
                                if (i == 0)
                                {
                                    Assert.Equal(2, attachment.Etag);
                                }
                                else if (i == 1)
                                {
                                    Assert.Equal(4, attachment.Etag);
                                }
                                else if (i == 2)
                                {
                                    // TODO: Investigate why we have this unstability in etag
                                    Assert.True(attachment.Etag == 6 || attachment.Etag == 5, $"actual etag is: {attachment.Etag}");
                                }
                            }
                            else
                            {
                                Assert.Equal(i + 1, attachment.Etag);
                            }
                            Assert.Equal(name, attachment.Name);
                            Assert.Equal(i == 0 ? 3 : 5, attachmentStream.Position);
                            if (i == 0)
                            {
                                Assert.Equal(new byte[] { 1, 2, 3 }, readBuffer.Take(3));
                                Assert.Equal("image/png", attachment.ContentType);
                                Assert.Equal("JCS/B3EIIB2gNVjsXTCD1aXlTgzuEz50", attachment.Hash);
                            }
                            else if (i == 1)
                            {
                                Assert.Equal(new byte[] { 10, 20, 30, 40, 50 }, readBuffer.Take(5));
                                Assert.Equal("ImGgE/jPeG", attachment.ContentType);
                                Assert.Equal("mpqSy7Ky+qPhkBwhLiiM2no82Wvo9gQw", attachment.Hash);
                            }
                            else if (i == 2)
                            {
                                Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, readBuffer.Take(5));
                                Assert.Equal("", attachment.ContentType);
                                Assert.Equal("PN5EZXRY470m7BLxu9MsOi/WwIRIq4WN", attachment.Hash);
                            }
                        }
                    }

                    using (var attachmentStream = new MemoryStream(readBuffer))
                    {
                        var notExistsAttachment = session.Advanced.GetAttachment("users/1", "not-there", (result, stream) => stream.CopyTo(attachmentStream));
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
        public void PutAndGetSpecialChar(string nameAndContentType, string expectedContentType)
        {
            var name = "aA" + nameAndContentType;
            if (expectedContentType != null)
                expectedContentType = "aA" + expectedContentType;

            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }
                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    var result = store1.Operations.Send(new PutAttachmentOperation("users/1", name, profileStream, name));
                    Assert.Equal(2, result.Etag);
                    Assert.Equal(name, result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal(name, result.ContentType);
                }

                SetupAttachmentReplication(store1, store2, false);
                Assert.True(WaitForDocument(store2, "users/1"));

                using (var session = store2.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal((DocumentFlags.HasAttachments | DocumentFlags.FromReplication).ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    var attachment = attachments.Single();
                    Assert.Equal(name, attachment.GetString(nameof(Attachment.Name)));
                }

                using (var session = store2.OpenSession())
                {
                    var readBuffer = new byte[8];
                    using (var attachmentStream = new MemoryStream(readBuffer))
                    {
                        var attachment = session.Advanced.GetAttachment("users/1", name, (result, stream) => stream.CopyTo(attachmentStream));
                        Assert.Equal(name, attachment.Name);
                        Assert.Equal(new byte[] { 1, 2, 3 }, readBuffer.Take(3));
                        Assert.Equal(expectedContentType, attachment.ContentType);
                    }
                }
            }
        }

        [Fact]
        public void DeleteAttachments()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
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
                AttachmentsCrud.AssertAttachmentCount(store1, 3);

                store1.Operations.Send(new DeleteAttachmentOperation("users/1", "file2"));

                SetupAttachmentReplication(store1, store2);
                AttachmentsCrud.AssertAttachmentCount(store2, 2);

                using (var session = store2.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal((DocumentFlags.HasAttachments | DocumentFlags.FromReplication).ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Assert.Equal(2, attachments.Length);
                    Assert.Equal("file1", attachments[0].GetString(nameof(Attachment.Name)));
                    Assert.Equal("JCS/B3EIIB2gNVjsXTCD1aXlTgzuEz50", attachments[0].GetString(nameof(AttachmentResult.Hash)));
                    Assert.Equal("file3", attachments[1].GetString(nameof(Attachment.Name)));
                    Assert.Equal("5VAt5Ayu6fKD6IGJimMLj73IlN8kgtGd", attachments[1].GetString(nameof(AttachmentResult.Hash)));
                }

                using (var session = store2.OpenSession())
                {
                    var readBuffer = new byte[16];
                    using (var attachmentStream = new MemoryStream(readBuffer))
                    {
                        var attachment = session.Advanced.GetAttachment("users/1", "file1", (result, stream) => stream.CopyTo(attachmentStream));
                        Assert.Equal(1, attachment.Etag);
                        Assert.Equal("file1", attachment.Name);
                        Assert.Equal("JCS/B3EIIB2gNVjsXTCD1aXlTgzuEz50", attachment.Hash);
                        Assert.Equal(3, attachmentStream.Position);
                        Assert.Equal(new byte[] { 1, 2, 3 }, readBuffer.Take(3));
                    }
                    using (var attachmentStream = new MemoryStream(readBuffer))
                    {
                        var attachment = session.Advanced.GetAttachment("users/1", "file3", (result, stream) => stream.CopyTo(attachmentStream));
                        Assert.Equal(2, attachment.Etag);
                        Assert.Equal("file3", attachment.Name);
                        Assert.Equal("5VAt5Ayu6fKD6IGJimMLj73IlN8kgtGd", attachment.Hash);
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
                AttachmentsCrud.AssertAttachmentCount(store2, 0);
            }
        }

        [Fact]
        public void PutAndDeleteAttachmentsWithTheSameStream_AlsoTestBigStreams()
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

                    // Use 128 KB file to test hashing a big file (> 32 KB)
                    using (var stream1 = new MemoryStream(Enumerable.Range(1, 128 * 1024).Select(x => (byte)x).ToArray()))
                        store1.Operations.Send(new PutAttachmentOperation("users/" + i, "file" + i, stream1, "image/png"));
                }
                using (var stream2 = new MemoryStream(Enumerable.Range(1, 999 * 1024).Select(x => (byte)x).ToArray()))
                    store1.Operations.Send(new PutAttachmentOperation("users/1", "big-file", stream2, "image/png"));

                SetupAttachmentReplication(store1, store2);
                AttachmentsCrud.AssertAttachmentCount(store2, 2, 4);

                using (var session = store2.OpenSession())
                {
                    var readBuffer = new byte[1024 * 1024];
                    using (var attachmentStream = new MemoryStream(readBuffer))
                    {
                        var attachment = session.Advanced.GetAttachment("users/3", "file3", (result, stream) => stream.CopyTo(attachmentStream));
                        Assert.Equal(4, attachment.Etag);
                        Assert.Equal("file3", attachment.Name);
                        Assert.Equal("fLtSLG1vPKEedr7AfTOgijyIw3ppa4h6", attachment.Hash);
                        Assert.Equal(128 * 1024, attachmentStream.Position);
                        var expected = Enumerable.Range(1, 128 * 1024).Select(x => (byte)x);
                        var actual = readBuffer.Take((int)attachmentStream.Position);
                        Assert.Equal(expected, actual);
                    }
                    using (var attachmentStream = new MemoryStream(readBuffer))
                    {
                        var attachment = session.Advanced.GetAttachment("users/1", "big-file", (result, stream) => stream.CopyTo(attachmentStream));
                        Assert.Equal(6, attachment.Etag);
                        Assert.Equal("big-file", attachment.Name);
                        Assert.Equal("OLSEi3K4Iio9JV3ymWJeF12Nlkjakwer", attachment.Hash);
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
            }
        }

        private void SetupAttachmentReplication(DocumentStore store1, DocumentStore store2, bool waitOnMarker = true)
        {
            var database1 = GetDocumentDatabaseInstanceFor(store1).Result;
            database1.Configuration.Replication.MaxItemsCount = null;
            database1.Configuration.Replication.MaxSizeToSend = null;
            SetupReplication(store1, store2);

            if (waitOnMarker)
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User {Name = "Marker"}, "marker");
                    session.SaveChanges();
                }
                Assert.True(WaitForDocument(store2, "marker"));
            }
        }

        private void AssertDelete(DocumentStore store1, DocumentStore store2, string name, long expectedUniqueAttachments, long? expectedAttachments = null)
        {
            using (var session = store1.OpenSession())
            {
                session.Store(new User {Name = "Marker " + name}, "marker-" + name);
                session.SaveChanges();
            }
            Assert.True(WaitForDocument(store2, "marker-" + name));
            AttachmentsCrud.AssertAttachmentCount(store2, expectedUniqueAttachments, expectedAttachments);
        }

        [Fact]
        public void DeleteDocumentWithAttachmentsThatHaveTheSameStream()
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

                SetupAttachmentReplication(store1, store2);
                AttachmentsCrud.AssertAttachmentCount(store2, 2, 4);

                store1.Commands().Delete("users/2", null);
                AssertDelete(store1, store2, "#1", 2, 3);

                store1.Commands().Delete("users/1", null);
                AssertDelete(store1, store2, "#2", 1);

                store1.Commands().Delete("users/3", null);
                AssertDelete(store1, store2, "#3", 0);
            }
        }

        [Fact(Skip = "WIP")]
        public async Task AttachmentsVersioningReplication()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await VersioningHelper.SetupVersioning(store1, false, 4);
                await VersioningHelper.SetupVersioning(store2, false, 4);

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
                    Assert.Equal(4, result.Etag);
                    Assert.Equal(names[0], result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("image/png", result.ContentType);
                    Assert.Equal("JCS/B3EIIB2gNVjsXTCD1aXlTgzuEz50", result.Hash);
                }
                using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                {
                    var result = store1.Operations.Send(new PutAttachmentOperation("users/1", names[1], backgroundStream, "ImGgE/jPeG"));
                    Assert.Equal(8, result.Etag);
                    Assert.Equal(names[1], result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("ImGgE/jPeG", result.ContentType);
                    Assert.Equal("mpqSy7Ky+qPhkBwhLiiM2no82Wvo9gQw", result.Hash);
                }
                using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    var result = store1.Operations.Send(new PutAttachmentOperation("users/1", names[2], fileStream, null));
                    Assert.Equal(13, result.Etag);
                    Assert.Equal(names[2], result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("", result.ContentType);
                    Assert.Equal("PN5EZXRY470m7BLxu9MsOi/WwIRIq4WN", result.Hash);
                }
                SetupAttachmentReplication(store1, store2);

                WaitForUserToContinueTheTest(store2);

                AttachmentsVersioning.AssertRevisions(store2, names, (session, revisions) =>
                {
                    AttachmentsVersioning.AssertRevisionAttachments(names, 3, revisions[0], session);
                    AttachmentsVersioning.AssertRevisionAttachments(names, 2, revisions[1], session);
                    AttachmentsVersioning.AssertRevisionAttachments(names, 1, revisions[2], session);
                    AttachmentsVersioning.AssertNoRevisionAttachment(revisions[3], session);
                }, 9, expectedCountOfDocuments: 3);

                // Delete document should delete all the attachments
                store1.Commands().Delete("users/1", null);
                AttachmentsVersioning.AssertRevisions(store2, names, (session, revisions) =>
                {
                    AttachmentsVersioning.AssertRevisionAttachments(names, 3, revisions[0], session);
                    AttachmentsVersioning.AssertRevisionAttachments(names, 2, revisions[1], session);
                    AttachmentsVersioning.AssertRevisionAttachments(names, 1, revisions[2], session);
                    AttachmentsVersioning.AssertNoRevisionAttachment(revisions[3], session);
                }, 6, expectedCountOfDocuments: 2);

                // Create another revision which should delete old revision
                using (var session = store1.OpenSession()) // This will delete the revision #1 which is without attachment
                {
                    session.Store(new User { Name = "Fitzchak 2" }, "users/1");
                    session.SaveChanges();
                }
                AttachmentsVersioning.AssertRevisions(store2, names, (session, revisions) =>
                {
                    AttachmentsVersioning.AssertNoRevisionAttachment(revisions[0], session);
                    AttachmentsVersioning.AssertRevisionAttachments(names, 3, revisions[1], session);
                    AttachmentsVersioning.AssertRevisionAttachments(names, 2, revisions[2], session);
                    AttachmentsVersioning.AssertRevisionAttachments(names, 1, revisions[3], session);
                }, 6);

                using (var session = store1.OpenSession()) // This will delete the revision #2 which is with attachment
                {
                    session.Store(new User { Name = "Fitzchak 3" }, "users/1");
                    session.SaveChanges();
                }
                AttachmentsVersioning.AssertRevisions(store2, names, (session, revisions) =>
                {
                    AttachmentsVersioning.AssertNoRevisionAttachment(revisions[0], session);
                    AttachmentsVersioning.AssertNoRevisionAttachment(revisions[1], session);
                    AttachmentsVersioning.AssertRevisionAttachments(names, 3, revisions[2], session);
                    AttachmentsVersioning.AssertRevisionAttachments(names, 2, revisions[3], session);
                }, 5);

                using (var session = store1.OpenSession()) // This will delete the revision #3 which is with attachment
                {
                    session.Store(new User { Name = "Fitzchak 4" }, "users/1");
                    session.SaveChanges();
                }
                AttachmentsVersioning.AssertRevisions(store2, names, (session, revisions) =>
                {
                    AttachmentsVersioning.AssertNoRevisionAttachment(revisions[0], session);
                    AttachmentsVersioning.AssertNoRevisionAttachment(revisions[1], session);
                    AttachmentsVersioning.AssertNoRevisionAttachment(revisions[2], session);
                    AttachmentsVersioning.AssertRevisionAttachments(names, 3, revisions[2], session);
                }, 3);

                using (var session = store1.OpenSession()) // This will delete the revision #4 which is with attachment
                {
                    session.Store(new User { Name = "Fitzchak 5" }, "users/1");
                    session.SaveChanges();
                }
                AttachmentsVersioning.AssertRevisions(store2, names, (session, revisions) =>
                {
                    AttachmentsVersioning.AssertNoRevisionAttachment(revisions[0], session);
                    AttachmentsVersioning.AssertNoRevisionAttachment(revisions[1], session);
                    AttachmentsVersioning.AssertNoRevisionAttachment(revisions[2], session);
                    AttachmentsVersioning.AssertNoRevisionAttachment(revisions[3], session);
                }, 0, expectedCountOfUniqueAttachments: 0);
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
                    await session.StoreAsync(new User {Name = "Fitzchak"}, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] {1, 2, 3}))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a1/png"));
                    }

                    await session.StoreAsync(new User {Name = "Marker 1"}, "marker 1");
                    await session.SaveChangesAsync();
                }
                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Fitzchak"}, "users/1");
                    await session.SaveChangesAsync();

                    using (var a2 = new MemoryStream(new byte[] {1, 2, 3, 4, 5}))
                    {
                        store2.Operations.Send(new PutAttachmentOperation("users/1", "a2", a2, "a2/jpeg"));
                    }

                    await session.StoreAsync(new User {Name = "Marker 2"}, "marker 2");
                    await session.SaveChangesAsync();
                }

                SetupReplication(store1, store2);
                SetupReplication(store2, store1);

                Assert.True(WaitForDocument(store2, "marker 1"));
                Assert.True(WaitForDocument(store1, "marker 2"));

                await AssertAttachments(store1, new[] {"a1", "a2"});
                await AssertAttachments(store2, new[] {"a1", "a2"});
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
                    Assert.Equal(names[i], attachments[i].GetString(nameof(Attachment.Name)));
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

                SetupReplication(store1, store2);
                SetupReplication(store2, store1);

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
                
                SetupReplication(store1, store2);
                SetupReplication(store2, store1);

                Assert.True(WaitForDocument(store2, "marker 1"));
                Assert.True(WaitForDocument(store1, "marker 2"));

                await AssertAttachments(store1, new[] { "a1" });
                await AssertAttachments(store2, new[] { "a1" });
            }
        }

        [Fact]
        public async Task PutSameAttachmentsDifferentContentTypeShouldConflict()
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
                
                SetupReplication(store1, store2);
                SetupReplication(store2, store1);

                var conflicts = WaitUntilHasConflict(store1, "users/1");
                Assert.Equal(2, conflicts.Results.Length);
                AssertConflict(conflicts.Results[0], "a1", "JCS/B3EIIB2gNVjsXTCD1aXlTgzuEz50", "a1/png", 3);
                AssertConflict(conflicts.Results[1], "a1", "JCS/B3EIIB2gNVjsXTCD1aXlTgzuEz50", "a2/jpeg", 3);

                conflicts = WaitUntilHasConflict(store2, "users/1");
                Assert.Equal(2, conflicts.Results.Length);
                AssertConflict(conflicts.Results[0], "a1", "JCS/B3EIIB2gNVjsXTCD1aXlTgzuEz50", "a1/png", 3);
                AssertConflict(conflicts.Results[1], "a1", "JCS/B3EIIB2gNVjsXTCD1aXlTgzuEz50", "a2/jpeg", 3);
            }
        }

        [Fact]
        public async Task PutDifferentAttachmentsShouldConflict()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await SetDatabaseId(store1, new Guid("00000000-48c4-421e-9466-000000000000"));
                await SetDatabaseId(store2, new Guid("99999999-48c4-421e-9466-999999999999"));

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Fitzchak"}, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] {1, 2, 3}))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a1/png"));
                    }
                }
                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Fitzchak"}, "users/1");
                    await session.SaveChangesAsync();

                    using (var a2 = new MemoryStream(new byte[] {1, 2, 3, 4, 5}))
                    {
                        store2.Operations.Send(new PutAttachmentOperation("users/1", "a1", a2, "a1/png"));
                    }
                }

                SetupReplication(store1, store2);
                SetupReplication(store2, store1);

                var hash1 = "JCS/B3EIIB2gNVjsXTCD1aXlTgzuEz50";
                var hash2 = "PN5EZXRY470m7BLxu9MsOi/WwIRIq4WN";

                var conflicts = WaitUntilHasConflict(store1, "users/1");
                Assert.Equal(2, conflicts.Results.Length);
                AssertConflict(conflicts.Results[0], "a1", hash1, "a1/png", 3);
                AssertConflict(conflicts.Results[1], "a1", hash2, "a1/png", 5);

                conflicts = WaitUntilHasConflict(store2, "users/1");
                Assert.Equal(2, conflicts.Results.Length);
                AssertConflict(conflicts.Results[0], "a1", hash1, "a1/png", 3);
                AssertConflict(conflicts.Results[1], "a1", hash2, "a1/png", 5);
            }
        }

        private async Task SetDatabaseId(DocumentStore store, Guid dbId)
        {
            var database = await GetDocumentDatabaseInstanceFor(store);
            var type = database.GetAllStoragesEnvironment().Single(t => t.Type == StorageEnvironmentWithType.StorageEnvironmentType.Documents);
            type.Environment.DbId = dbId;
        }

        private void AssertConflict(GetConflictsResult.Conflict conflict, string name, string hash, string contentType, long size)
        {
            Assert.True(conflict.Doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));

            Assert.True(metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments));
            var attachment = (BlittableJsonReaderObject)attachments.Single();

            Assert.True(attachment.TryGet(nameof(AttachmentResult.Name), out string actualName));
            Assert.Equal(name, actualName);
            Assert.True(attachment.TryGet(nameof(AttachmentResult.Hash), out string actualHash));
            Assert.Equal(hash, actualHash);
            Assert.True(attachment.TryGet(nameof(AttachmentResult.ContentType), out string actualContentType));
            Assert.Equal(contentType, actualContentType);
            Assert.True(attachment.TryGet(nameof(AttachmentResult.Size), out long actualSize));
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
                    await session.StoreAsync(new User {Name = "Fitzchak"}, "users/1");
                    await session.SaveChangesAsync();

                    using (var a1 = new MemoryStream(new byte[] {1, 2, 3}))
                    {
                        await store1.Operations.SendAsync(new PutAttachmentOperation("users/1", "a1", a1, "a1/png"));
                    }

                    await session.StoreAsync(new User {Name = "Marker 1"}, "marker 1");
                    await session.SaveChangesAsync();
                }
                using (var session = store2.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Fitzchak"}, "users/1");
                    await session.SaveChangesAsync();

                    using (var a2 = new MemoryStream(new byte[] {1, 2, 3, 4, 5}))
                    {
                        store2.Operations.Send(new PutAttachmentOperation("users/1", "a1", a2, "a1/png"));
                    }
                    store2.Operations.Send(new DeleteAttachmentOperation("users/1", "a1"));

                    await session.StoreAsync(new User {Name = "Marker 2"}, "marker 2");
                    await session.SaveChangesAsync();
                }

                SetupReplication(store1, store2);
                SetupReplication(store2, store1);

                Assert.True(WaitForDocument(store2, "marker 1"));
                Assert.True(WaitForDocument(store1, "marker 2"));

                await AssertAttachments(store1, new[] {"a1"});
                await AssertAttachments(store2, new[] {"a1"});
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
                        await commands.PutAsync("users/1", null, new User {Name = "Fitzchak"});
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

                    using (var a2 = new MemoryStream(new byte[] {1, 2, 3, 4, 5}))
                    {
                        store2.Operations.Send(new PutAttachmentOperation("users/1", "a1", a2, "a1/png"));
                    }
                    store2.Operations.Send(new DeleteAttachmentOperation("users/1", "a1"));

                    await session.StoreAsync(new User {Name = "Marker 2"}, "marker 2");
                    await session.SaveChangesAsync();
                }

                SetupReplication(store1, store2);
                SetupReplication(store2, store1);

                Assert.True(WaitForDocument(store2, "marker 1"));
                Assert.True(WaitForDocument(store1, "marker 2"));

                await AssertAttachments(store1, new[] { "a1" });
                await AssertAttachments(store2, new[] { "a1" });
            }
        }
    }
}