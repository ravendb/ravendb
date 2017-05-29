using System;
using System.IO;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents;
using Xunit;

namespace FastTests.Client.Attachments
{
    public class AttachmentsSession : RavenTestBase
    {
        [Fact]
        public void PutAttachments()
        {
            using (var store = GetDocumentStore())
            {
                var names = new[]
                {
                    "profile.png",
                    "background-photo.jpg",
                    "fileNAME_#$1^%_בעברית.txt"
                };

                using (var session = store.OpenSession())
                using (var profileStream = new MemoryStream(new byte[] {1, 2, 3}))
                using (var backgroundStream = new MemoryStream(new byte[] {10, 20, 30, 40, 50}))
                using (var fileStream = new MemoryStream(new byte[] {1, 2, 3, 4, 5}))
                {
                    var user = new User {Name = "Fitzchak"};
                    session.Store(user, "users/1");

                    session.Advanced.StoreAttachment("users/1", names[0], profileStream, "image/png");
                    session.Advanced.StoreAttachment(user, names[1], backgroundStream, "ImGgE/jPeG");
                    session.Advanced.StoreAttachment(user, names[2], fileStream);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Assert.Equal(3, attachments.Length);
                    var orderedNames = names.OrderBy(x => x).ToArray();
                    for (var i = 0; i < names.Length; i++)
                    {
                        var name = orderedNames[i];
                        var attachment = attachments[i];
                        Assert.Equal(name, attachment.GetString(nameof(AttachmentResult.Name)));
                        var hash = attachment.GetString(nameof(AttachmentResult.Hash));
                        if (i == 0)
                        {
                            Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", hash);
                            Assert.Equal(5, attachment.GetNumber(nameof(AttachmentResult.Size)));
                        }
                        else if (i == 1)
                        {
                            Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", hash);
                            Assert.Equal(5, attachment.GetNumber(nameof(AttachmentResult.Size)));
                        }
                        else if (i == 2)
                        {
                            Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                            Assert.Equal(3, attachment.GetNumber(nameof(AttachmentResult.Size)));
                        }
                    }

                    AttachmentsCrud.AssertAttachmentCount(store, 3, 3);

                    var readBuffer = new byte[8];
                    for (var i = 0; i < names.Length; i++)
                    {
                        var name = names[i];
                        using (var attachmentStream = new MemoryStream(readBuffer))
                        {
                            var attachment = session.Advanced.GetAttachment(user, name, (result, stream) => stream.CopyTo(attachmentStream));
                            Assert.Equal(2 + 2 * i, attachment.Etag);
                            Assert.Equal(name, attachment.Name);
                            Assert.Equal(i == 0 ? 3 : 5, attachmentStream.Position);
                            if (i == 0)
                            {
                                Assert.Equal(new byte[] { 1, 2, 3 }, readBuffer.Take(3));
                                Assert.Equal("image/png", attachment.ContentType);
                                Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachment.Hash);
                                Assert.Equal(3, attachment.Size);
                            }
                            else if (i == 1)
                            {
                                Assert.Equal(new byte[] { 10, 20, 30, 40, 50 }, readBuffer.Take(5));
                                Assert.Equal("ImGgE/jPeG", attachment.ContentType);
                                Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", attachment.Hash);
                                Assert.Equal(5, attachment.Size);
                            }
                            else if (i == 2)
                            {
                                Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, readBuffer.Take(5));
                                Assert.Equal("", attachment.ContentType);
                                Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", attachment.Hash);
                                Assert.Equal(5, attachment.Size);
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

        [Fact(Skip = "TODO")]
        public void ThrowIfStreamIsDisposed()
        {
            using (var store = GetDocumentStore())
            {
                var names = new[]
                {
                    "profile.png",
                    "background-photo.jpg",
                    "fileNAME_#$1^%_בעברית.txt"
                };

                using (var session = store.OpenSession())
                {
                    var user = new User {Name = "Fitzchak"};
                    session.Store(user, "users/1");
                    
                    using (var profileStream = new MemoryStream(new byte[] {1, 2, 3}))
                        session.Advanced.StoreAttachment(user, names[0], profileStream, "image/png");
                    using (var backgroundStream = new MemoryStream(new byte[] {10, 20, 30, 40, 50}))
                        session.Advanced.StoreAttachment(user, names[1], backgroundStream, "ImGgE/jPeG");
                    using (var fileStream = new MemoryStream(new byte[] {1, 2, 3, 4, 5}))
                        session.Advanced.StoreAttachment(user, names[2], fileStream, null);

                    session.SaveChanges();
                }
            }
        }

        [Fact(Skip = "TODO")]
        public void TwoAttachmentsWithTheSameName()
        {
        }

        [Fact]
        public void PutDocumentAndAttachmentAndDeleteShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    var user = new User { Name = "Fitzchak" };
                    session.Store(user, "users/1");

                    session.Advanced.StoreAttachment(user, "profile.png", profileStream, "image/png");

                    session.Delete(user);

                    var exception = Assert.Throws<InvalidOperationException>(() => session.SaveChanges());
                    Assert.Equal("Cannot perform save because document users/1 has been deleted by the session and is also taking part in deferred AttachmentPUT command", exception.Message);
                }
            }
        }

        [Fact]
        public void PutAttachmentAndDeleteShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User {Name = "Fitzchak"};
                    session.Store(user, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    var user = session.Load<User>("users/1");
                    session.Advanced.StoreAttachment(user, "profile.png", profileStream, "image/png");
                    session.Delete(user);

                    var exception = Assert.Throws<InvalidOperationException>(() => session.SaveChanges());
                    Assert.Equal("Cannot perform save because document users/1 has been deleted by the session and is also taking part in deferred AttachmentPUT command", exception.Message);
                }
            }
        }

        [Fact]
        public void DeleteAttachments()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User {Name = "Fitzchak"};
                    session.Store(user, "users/1");

                    using (var stream1 = new MemoryStream(Enumerable.Range(1, 3).Select(x => (byte)x).ToArray()))
                    using (var stream2 = new MemoryStream(Enumerable.Range(1, 6).Select(x => (byte)x).ToArray()))
                    using (var stream3 = new MemoryStream(Enumerable.Range(1, 9).Select(x => (byte)x).ToArray()))
                    using (var stream4 = new MemoryStream(Enumerable.Range(1, 12).Select(x => (byte)x).ToArray()))
                    {
                        session.Advanced.StoreAttachment(user, "file1", stream1, "image/png");
                        session.Advanced.StoreAttachment(user, "file2", stream2, "image/png");
                        session.Advanced.StoreAttachment(user, "file3", stream3, "image/png");
                        session.Advanced.StoreAttachment(user, "file4", stream4, "image/png");

                        session.SaveChanges();
                    }
                }

                AttachmentsCrud.AssertAttachmentCount(store, 4, documentsCount: 1);

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    session.Advanced.DeleteAttachment("users/1", "file2");
                    session.Advanced.DeleteAttachment(user, "file4");

                    session.SaveChanges();
                }
                AttachmentsCrud.AssertAttachmentCount(store, 2, documentsCount: 1);

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Assert.Equal(2, attachments.Length);
                    Assert.Equal("file1", attachments[0].GetString(nameof(AttachmentResult.Name)));
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[0].GetString(nameof(AttachmentResult.Hash)));
                    Assert.Equal("file3", attachments[1].GetString(nameof(AttachmentResult.Name)));
                    Assert.Equal("NRQuixiqj+xvEokF6MdQq1u+uH1dk/gk2PLChJQ58Vo=", attachments[1].GetString(nameof(AttachmentResult.Hash)));
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    var readBuffer = new byte[16];
                    using (var attachmentStream = new MemoryStream(readBuffer))
                    {
                        var attachment = session.Advanced.GetAttachment("users/1", "file1", (result, stream) => stream.CopyTo(attachmentStream));
                        Assert.Equal(2, attachment.Etag);
                        Assert.Equal("file1", attachment.Name);
                        Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachment.Hash);
                        Assert.Equal(3, attachmentStream.Position);
                        Assert.Equal(new byte[] { 1, 2, 3 }, readBuffer.Take(3));
                    }
                    using (var attachmentStream = new MemoryStream(readBuffer))
                    {
                        var attachment = session.Advanced.GetAttachment(user, "file2", (result, stream) => stream.CopyTo(attachmentStream));
                        Assert.Null(attachment);
                        Assert.Equal(0, attachmentStream.Position);
                    }
                    using (var attachmentStream = new MemoryStream(readBuffer))
                    {
                        var attachment = session.Advanced.GetAttachment(user, "file3", (result, stream) => stream.CopyTo(attachmentStream));
                        Assert.Equal(6, attachment.Etag);
                        Assert.Equal("file3", attachment.Name);
                        Assert.Equal("NRQuixiqj+xvEokF6MdQq1u+uH1dk/gk2PLChJQ58Vo=", attachment.Hash);
                        Assert.Equal(9, attachmentStream.Position);
                        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 }, readBuffer.Take(9));
                    }
                    using (var attachmentStream = new MemoryStream(readBuffer))
                    {
                        var attachment = session.Advanced.GetAttachment(user, "file4", (result, stream) => stream.CopyTo(attachmentStream));
                        Assert.Null(attachment);
                        Assert.Equal(0, attachmentStream.Position);
                    }

                    // Delete document should delete all the attachments
                    session.Delete(user);
                    session.SaveChanges();
                }
                AttachmentsCrud.AssertAttachmentCount(store, 0, documentsCount: 0);
            }
        }

        [Fact]
        public void DeleteDocumentAndThanItsAttachments_ThisIsNoOpButShouldBeSupported()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User {Name = "Fitzchak"};
                    session.Store(user, "users/1");

                    using (var stream = new MemoryStream(Enumerable.Range(1, 3).Select(x => (byte)x).ToArray()))
                    {
                        session.Advanced.StoreAttachment(user, "file", stream, "image/png");
                        session.SaveChanges();
                    }
                }

                AttachmentsCrud.AssertAttachmentCount(store, 1, documentsCount: 1);

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    session.Delete(user);
                    session.Advanced.DeleteAttachment(user, "file");
                    session.Advanced.DeleteAttachment(user, "file"); // this should be no-op

                    session.SaveChanges();
                }
                AttachmentsCrud.AssertAttachmentCount(store, 0, documentsCount: 0);
            }
        }

        [Fact]
        public void DeleteDocumentByCommandAndThanItsAttachments_ThisIsNoOpButShouldBeSupported()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Fitzchak"}, "users/1");

                    using (var stream = new MemoryStream(Enumerable.Range(1, 3).Select(x => (byte)x).ToArray()))
                    {
                        session.Advanced.StoreAttachment("users/1", "file", stream, "image/png");
                        session.SaveChanges();
                    }
                }

                AttachmentsCrud.AssertAttachmentCount(store, 1, documentsCount: 1);

                using (var session = store.OpenSession())
                {
                    session.Advanced.Defer(new DeleteCommandData("users/1", null));
                    session.Advanced.DeleteAttachment("users/1", "file");
                    session.Advanced.DeleteAttachment("users/1", "file"); // this should be no-op

                    session.SaveChanges();
                }
                AttachmentsCrud.AssertAttachmentCount(store, 0, documentsCount: 0);
            }
        }
    }
}