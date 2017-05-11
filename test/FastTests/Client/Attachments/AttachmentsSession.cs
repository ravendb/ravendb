using System;
using System.IO;
using System.Linq;
using Raven.Client;
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
                            Assert.Equal("mpqSy7Ky+qPhkBwhLiiM2no82Wvo9gQw", hash);
                            Assert.Equal(5, attachment.GetNumber(nameof(AttachmentResult.Size)));
                        }
                        else if (i == 1)
                        {
                            Assert.Equal("PN5EZXRY470m7BLxu9MsOi/WwIRIq4WN", hash);
                            Assert.Equal(5, attachment.GetNumber(nameof(AttachmentResult.Size)));
                        }
                        else if (i == 2)
                        {
                            Assert.Equal("JCS/B3EIIB2gNVjsXTCD1aXlTgzuEz50", hash);
                            Assert.Equal(3, attachment.GetNumber(nameof(AttachmentResult.Size)));
                        }
                    }
                }

                AttachmentsCrud.AssertAttachmentCount(store, 3, 3);

                using (var session = store.OpenSession())
                {
                    var readBuffer = new byte[8];
                    for (var i = 0; i < names.Length; i++)
                    {
                        var name = names[i];
                        using (var attachmentStream = new MemoryStream(readBuffer))
                        {
                            var attachment = session.Advanced.GetAttachment("users/1", name, (result, stream) => stream.CopyTo(attachmentStream));
                            Assert.Equal(2 + 2 * i, attachment.Etag);
                            Assert.Equal(name, attachment.Name);
                            Assert.Equal(i == 0 ? 3 : 5, attachmentStream.Position);
                            if (i == 0)
                            {
                                Assert.Equal(new byte[] { 1, 2, 3 }, readBuffer.Take(3));
                                Assert.Equal("image/png", attachment.ContentType);
                                Assert.Equal("JCS/B3EIIB2gNVjsXTCD1aXlTgzuEz50", attachment.Hash);
                                Assert.Equal(3, attachment.Size);
                            }
                            else if (i == 1)
                            {
                                Assert.Equal(new byte[] { 10, 20, 30, 40, 50 }, readBuffer.Take(5));
                                Assert.Equal("ImGgE/jPeG", attachment.ContentType);
                                Assert.Equal("mpqSy7Ky+qPhkBwhLiiM2no82Wvo9gQw", attachment.Hash);
                                Assert.Equal(5, attachment.Size);
                            }
                            else if (i == 2)
                            {
                                Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, readBuffer.Take(5));
                                Assert.Equal("", attachment.ContentType);
                                Assert.Equal("PN5EZXRY470m7BLxu9MsOi/WwIRIq4WN", attachment.Hash);
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
                    session.Store(new User {Name = "Fitzchak"}, "users/1");
                    
                    using (var profileStream = new MemoryStream(new byte[] {1, 2, 3}))
                        session.Advanced.StoreAttachment("users/1", names[0], profileStream, "image/png");
                    using (var backgroundStream = new MemoryStream(new byte[] {10, 20, 30, 40, 50}))
                        session.Advanced.StoreAttachment("users/1", names[1], backgroundStream, "ImGgE/jPeG");
                    using (var fileStream = new MemoryStream(new byte[] {1, 2, 3, 4, 5}))
                        session.Advanced.StoreAttachment("users/1", names[2], fileStream, null);

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
                    Assert.Equal("Cannot perfrom save because document users/1 has been deleted by the session and is also taking part in deferred AttachmentPUT command", exception.Message);
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
                    Assert.Equal("Cannot perfrom save because document users/1 has been deleted by the session and is also taking part in deferred AttachmentPUT command", exception.Message);
                }
            }
        }
    }
}