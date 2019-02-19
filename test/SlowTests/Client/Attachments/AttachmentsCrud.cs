using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Xunit;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents;
using Raven.Tests.Core.Utils.Entities;

namespace SlowTests.Client.Attachments
{
    public class AttachmentsCrud : RavenTestBase
    {
        [Fact]
        public async Task PutAttachments()
        {
            using (var store = GetDocumentStore())
            {
                var dbId1 = new Guid("00000000-48c4-421e-9466-000000000000");
                await SetDatabaseId(store, dbId1);

                using (var session = store.OpenSession())
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
                    var result = store.Operations.Send(new PutAttachmentOperation("users/1", names[0], profileStream, "image/png"));
                    Assert.Contains("A:2", result.ChangeVector);
                    Assert.Equal(names[0], result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("image/png", result.ContentType);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", result.Hash);
                    Assert.Equal(3, result.Size);
                }
                using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                {
                    var result = store.Operations.Send(new PutAttachmentOperation("users/1", names[1], backgroundStream, "ImGgE/jPeG"));
                    Assert.Contains("A:4", result.ChangeVector);
                    Assert.Equal(names[1], result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("ImGgE/jPeG", result.ContentType);
                    Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", result.Hash);
                    Assert.Equal(5, result.Size);
                }
                using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    var result = store.Operations.Send(new PutAttachmentOperation("users/1", names[2], fileStream));
                    Assert.Contains("A:6", result.ChangeVector);
                    Assert.Equal(names[2], result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("", result.ContentType);
                    Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", result.Hash);
                    Assert.Equal(5, result.Size);
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
                        Assert.Equal(name, attachment.GetString(nameof(AttachmentName.Name)));
                        var hash = attachment.GetString(nameof(AttachmentName.Hash));
                        if (i == 0)
                        {
                            Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", hash);
                            Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                        }
                        else if (i == 1)
                        {
                            Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", hash);
                            Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                        }
                        else if (i == 2)
                        {
                            Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                            Assert.Equal(3, attachment.GetLong(nameof(AttachmentName.Size)));
                        }
                    }
                }

                AssertAttachmentCount(store, 3, 3);

                using (var session = store.OpenSession())
                {
                    var readBuffer = new byte[8];
                    for (var i = 0; i < names.Length; i++)
                    {
                        var name = names[i];
                        using (var attachmentStream = new MemoryStream(readBuffer))
                        using (var attachment = session.Advanced.Attachments.Get("users/1", name))
                        {
                            attachment.Stream.CopyTo(attachmentStream);
                            Assert.Contains("A:" + (2 + 2 * i), attachment.Details.ChangeVector);
                            Assert.Equal(name, attachment.Details.Name);
                            Assert.Equal(i == 0 ? 3 : 5, attachmentStream.Position);
                            if (i == 0)
                            {
                                Assert.Equal(new byte[] { 1, 2, 3 }, readBuffer.Take(3));
                                Assert.Equal("image/png", attachment.Details.ContentType);
                                Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachment.Details.Hash);
                                Assert.Equal(3, attachment.Details.Size);
                            }
                            else if (i == 1)
                            {
                                Assert.Equal(new byte[] { 10, 20, 30, 40, 50 }, readBuffer.Take(5));
                                Assert.Equal("ImGgE/jPeG", attachment.Details.ContentType);
                                Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", attachment.Details.Hash);
                                Assert.Equal(5, attachment.Details.Size);
                            }
                            else if (i == 2)
                            {
                                Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, readBuffer.Take(5));
                                Assert.Equal("", attachment.Details.ContentType);
                                Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", attachment.Details.Hash);
                                Assert.Equal(5, attachment.Details.Size);
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

        [Fact]
        public void PreserveAttachmentsInMetadataAfterPutDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();

                    using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        store.Operations.Send(new PutAttachmentOperation("users/1", "pic", profileStream, "image/png"));
                    }
                }

                ValidateMetadata_PutAttachmentAndPutDocument_ShouldHaveHasAttachmentsFlag(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak 2" }, "users/1");
                    session.SaveChanges();
                }

                ValidateMetadata_PutAttachmentAndPutDocument_ShouldHaveHasAttachmentsFlag(store);
            }
        }

        [Fact]
        public void PreserveAttachmentsInMetadataAfterPutDocumentWithoutMetadata()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put("users/1", null, new User { Name = "Fitzchak" });
                    using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        store.Operations.Send(new PutAttachmentOperation("users/1", "pic", profileStream, "image/png"));
                    }
                }

                ValidateMetadata_PutAttachmentAndPutDocument_ShouldHaveHasAttachmentsFlag(store);

                using (var commands = store.Commands())
                {
                    commands.Put("users/1", null, new User { Name = "Fitzchak 2" });
                }

                ValidateMetadata_PutAttachmentAndPutDocument_ShouldHaveHasAttachmentsFlag(store);
            }
        }

        private static void ValidateMetadata_PutAttachmentAndPutDocument_ShouldHaveHasAttachmentsFlag(DocumentStore store)
        {
            AssertAttachmentCount(store, 1, 1);

            // We must create a new session to check the flags, 
            // since we do not remove the document from the session cache after we put an attachment
            using (var session = store.OpenSession())
            {
                var user = session.Load<User>("users/1");
                var metadata = session.Advanced.GetMetadataFor(user);
                Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                var attachment = metadata.GetObjects(Constants.Documents.Metadata.Attachments).Single();
                Assert.Equal("pic", attachment.GetString(nameof(AttachmentName.Name)));
                Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachment.GetString(nameof(AttachmentName.Hash)));
                Assert.Equal("image/png", attachment.GetString(nameof(AttachmentName.ContentType)));
                Assert.Equal(3, attachment.GetLong(nameof(AttachmentName.Size)));
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

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    var result = store.Operations.Send(new PutAttachmentOperation("users/1", name, profileStream, name));
                    Assert.True(result.ChangeVector.StartsWith("A:2"));
                    Assert.Equal(name, result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal(name, result.ContentType);
                    Assert.Equal(3, result.Size);
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    var attachment = attachments.Single();
                    Assert.Equal(name, attachment.GetString(nameof(AttachmentName.Name)));
                }

                using (var session = store.OpenSession())
                {
                    var readBuffer = new byte[8];
                    using (var attachmentStream = new MemoryStream(readBuffer))
                    using (var attachment = session.Advanced.Attachments.Get("users/1", name))
                    {
                        attachment.Stream.CopyTo(attachmentStream);
                        Assert.Equal(name, attachment.Details.Name);
                        Assert.Equal(new byte[] { 1, 2, 3 }, readBuffer.Take(3));
                        Assert.Equal(expectedContentType, attachment.Details.ContentType);
                        Assert.Equal(3, attachment.Details.Size);
                    }
                }
            }
        }

        [Fact]
        public async Task DeleteAttachments()
        {
            using (var store = GetDocumentStore())
            {
                var dbId1 = new Guid("00000000-48c4-421e-9466-000000000000");
                await SetDatabaseId(store, dbId1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }

                for (int i = 1; i <= 3; i++)
                {
                    using (var profileStream = new MemoryStream(Enumerable.Range(1, 3 * i).Select(x => (byte)x).ToArray()))
                        store.Operations.Send(new PutAttachmentOperation("users/1", "file" + i, profileStream, "image/png"));
                }
                AssertAttachmentCount(store, 3);

                store.Operations.Send(new DeleteAttachmentOperation("users/1", "file2"));
                AssertAttachmentCount(store, 2);

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Assert.Equal(2, attachments.Length);
                    Assert.Equal("file1", attachments[0].GetString(nameof(AttachmentName.Name)));
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachments[0].GetString(nameof(AttachmentName.Hash)));
                    Assert.Equal("file3", attachments[1].GetString(nameof(AttachmentName.Name)));
                    Assert.Equal("NRQuixiqj+xvEokF6MdQq1u+uH1dk/gk2PLChJQ58Vo=", attachments[1].GetString(nameof(AttachmentName.Hash)));
                }

                using (var session = store.OpenSession())
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
                store.Commands().Delete("users/1", null);
                AssertAttachmentCount(store, 0, documentsCount: 0);
            }
        }

        public static void AssertAttachmentCount(DocumentStore store, long uniqueAttachmentCount, long? attachmentCount = null, long documentsCount = 1)
        {
            var statistics = store.Maintenance.Send(new GetStatisticsOperation());
            Assert.Equal(documentsCount, statistics.CountOfDocuments);
            Assert.Equal(0, statistics.CountOfRevisionDocuments);
            Assert.Equal(attachmentCount ?? uniqueAttachmentCount, statistics.CountOfAttachments);
            Assert.Equal(uniqueAttachmentCount, statistics.CountOfUniqueAttachments);
        }

        [Fact]
        public void PutAndDeleteAttachmentsWithTheSameStream_AlsoTestBigStreams()
        {
            using (var store = GetDocumentStore())
            {
                for (int i = 1; i <= 3; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "Fitzchak " + i }, "users/" + i);
                        session.SaveChanges();
                    }

                    // Use 128 KB file to test hashing a big file (> 32 KB)
                    using (var stream1 = new MemoryStream(Enumerable.Range(1, 128 * 1024).Select(x => (byte)x).ToArray()))
                        store.Operations.Send(new PutAttachmentOperation("users/" + i, "file" + i, stream1, "image/png"));
                }
                using (var stream2 = new MemoryStream(Enumerable.Range(1, 999 * 1024).Select(x => (byte)x).ToArray()))
                    store.Operations.Send(new PutAttachmentOperation("users/1", "big-file", stream2, "image/png"));
                AssertAttachmentCount(store, 2, 4, 3);

                using (var session = store.OpenSession())
                {
                    var readBuffer = new byte[1024 * 1024];
                    using (var attachmentStream = new MemoryStream(readBuffer))
                    using (var attachment = session.Advanced.Attachments.Get("users/3", "file3"))
                    {
                        attachment.Stream.CopyTo(attachmentStream);
                        Assert.True(attachment.Details.ChangeVector.StartsWith("A:8"));
                        Assert.Equal("file3", attachment.Details.Name);
                        Assert.Equal("uuBtr5rVX6NAXzdW2DhuG04MGGyUzFzpS7TelHw3fJQ=", attachment.Details.Hash);
                        Assert.Equal(128 * 1024, attachmentStream.Position);
                        Assert.Equal(128 * 1024, attachment.Details.Size);
                        Assert.Equal(Enumerable.Range(1, 128 * 1024).Select(x => (byte)x), readBuffer.Take((int)attachmentStream.Position));
                    }
                    using (var attachmentStream = new MemoryStream(readBuffer))
                    using (var attachment = session.Advanced.Attachments.Get("users/1", "big-file"))
                    {
                        attachment.Stream.CopyTo(attachmentStream);
                        Assert.True(attachment.Details.ChangeVector.StartsWith("A:10"));
                        Assert.Equal("big-file", attachment.Details.Name);
                        Assert.Equal("zKHiLyLNRBZti9DYbzuqZ/EDWAFMgOXB+SwKvjPAINk=", attachment.Details.Hash);
                        Assert.Equal(999 * 1024, attachmentStream.Position);
                        Assert.Equal(999 * 1024, attachment.Details.Size);
                        Assert.Equal(Enumerable.Range(1, 999 * 1024).Select(x => (byte)x), readBuffer.Take((int)attachmentStream.Position));
                    }
                }

                store.Operations.Send(new DeleteAttachmentOperation("users/1", "file1"));
                AssertAttachmentCount(store, 2, 3, 3);

                store.Operations.Send(new DeleteAttachmentOperation("users/2", "file2"));
                AssertAttachmentCount(store, 2, 2, 3);

                store.Operations.Send(new DeleteAttachmentOperation("users/3", "file3"));
                AssertAttachmentCount(store, 1, 1, 3);

                store.Operations.Send(new DeleteAttachmentOperation("users/1", "big-file"));
                AssertAttachmentCount(store, 0, 0, 3);

                for (int i = 1; i <= 3; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var user = session.Load<User>("users/" + i);
                        var metadata = session.Advanced.GetMetadataFor(user);
                        Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.Flags));
                        Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.Attachments));
                    }
                }
            }
        }

        [Fact]
        public void DeleteDocumentWithAttachmentsThatHaveTheSameStream()
        {
            using (var store = GetDocumentStore())
            {
                for (int i = 1; i <= 3; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "Fitzchak " + i }, "users/" + i);
                        session.SaveChanges();
                    }

                    using (var profileStream = new MemoryStream(Enumerable.Range(1, 3).Select(x => (byte)x).ToArray()))
                        store.Operations.Send(new PutAttachmentOperation("users/" + i, "file" + i, profileStream, "image/png"));
                }
                using (var profileStream = new MemoryStream(Enumerable.Range(1, 17).Select(x => (byte)x).ToArray()))
                    store.Operations.Send(new PutAttachmentOperation("users/1", "second-file", profileStream, "image/png"));
                AssertAttachmentCount(store, 2, 4, 3);

                store.Commands().Delete("users/2", null);
                AssertAttachmentCount(store, 2, 3, 2);

                store.Commands().Delete("users/1", null);
                AssertAttachmentCount(store, 1, 1, 1);

                store.Commands().Delete("users/3", null);
                AssertAttachmentCount(store, 0, 0, 0);
            }
        }

        [Fact]
        public async Task CanPatchWithoutConflictsOnAttachments()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    store.Operations.Send(new PutAttachmentOperation("users/1", "profile", profileStream, "image/png"));
                }

                await store.Operations.SendAsync(new PatchOperation("users/1", null, new PatchRequest
                {
                    Script = "this.LastName = args.newUser.LastName;",
                    Values =
                    {
                        {"newUser", new {LastName = "Yitzchaki"}}
                    }
                }));

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.Equal("Yitzchaki", user.LastName);

                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    var attachment = attachments.Single();
                    Assert.Equal("profile", attachment.GetString(nameof(AttachmentName.Name)));
                    var hash = attachment.GetString(nameof(AttachmentName.Hash));
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                }
            }
        }

        [Fact]
        public void PutAttachmentAndModifyDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    store.Operations.Send(new PutAttachmentOperation("users/1", "Profile", profileStream, "image/png"));
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    user.LastName = "Yitzchaki";
                    session.SaveChanges();
                }
            }
        }

        [Fact]
        public void PutAttachmentThanEvictAndModifyDocumentInTheSameSession()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "Fitzchak" };
                    session.Store(user, "users/1");
                    session.SaveChanges();

                    using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        store.Operations.Send(new PutAttachmentOperation("users/1", "Profile", profileStream, "image/png"));
                    }

                    session.Advanced.Evict(user);
                    user = session.Load<User>("users/1");
                    user.LastName = "Yitzchaki";
                    session.SaveChanges();
                }
            }
        }

        [Fact]
        public void PutAttachmentAndModifyDocumentInTheSameSession()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "Fitzchak" };
                    session.Store(user, "users/1");
                    session.SaveChanges();

                    using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        store.Operations.Send(new PutAttachmentOperation("users/1", "Profile", profileStream, "image/png"));
                    }

                    user = session.Load<User>("users/1");
                    user.LastName = "Yitzchaki";
                    session.SaveChanges();
                }
            }
        }

        [Fact]
        public void PutAttachmentAndModifyDocumentInTheSameSession_WithoutAnotherLoad()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "Fitzchak" };
                    session.Store(user, "users/1");
                    session.SaveChanges();

                    using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        store.Operations.Send(new PutAttachmentOperation("users/1", "Profile", profileStream, "image/png"));
                    }

                    user.LastName = "Yitzchaki";
                    session.SaveChanges();
                }
            }
        }

        [Fact]
        public void OverwriteAttachmentWithStreamOnly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    store.Operations.Send(new PutAttachmentOperation("users/1", "Profile", profileStream, "image/png"));
                }

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    store.Operations.Send(new PutAttachmentOperation("users/1", "Profile", profileStream, "image/png"));
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    var attachment = attachments.Single();
                    Assert.Equal("Profile", attachment.GetString(nameof(AttachmentName.Name)));
                    Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", attachment.GetString(nameof(AttachmentName.Hash)));
                    Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                    Assert.Equal("image/png", attachment.GetString(nameof(AttachmentName.ContentType)));
                }

                AssertAttachmentCount(store, 1);
            }
        }

        [Fact]
        public void PutSameAttachmentTwiceDifferentContentType()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    store.Operations.Send(new PutAttachmentOperation("users/1", "Profile", profileStream, "image/png"));

                    Assert.Equal(3, profileStream.Position);

                    profileStream.Position = 0;
                    store.Operations.Send(new PutAttachmentOperation("users/1", "Profile", profileStream, "image/jpeg"));
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    var attachment = attachments.Single();
                    Assert.Equal("Profile", attachment.GetString(nameof(AttachmentName.Name)));
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachment.GetString(nameof(AttachmentName.Hash)));
                    Assert.Equal(3, attachment.GetLong(nameof(AttachmentName.Size)));
                    Assert.Equal("image/jpeg", attachment.GetString(nameof(AttachmentName.ContentType)));
                }

                AssertAttachmentCount(store, 1);
            }
        }

        [Fact]
        public void PutSameAttachmentTwice_AlsoMakeSureThatTheStreamIsNotDisposedAfterCallingPutAttachment()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    var result = store.Operations.Send(new PutAttachmentOperation("users/1", "Profile", profileStream, "IMAGE/png"));
                    Assert.True(result.ChangeVector.StartsWith("A:2"));
                    Assert.Equal("Profile", result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("IMAGE/png", result.ContentType);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", result.Hash);
                    Assert.Equal(3, result.Size);
                    Assert.Equal(3, profileStream.Position);

                    profileStream.Position = 0;
                    result = store.Operations.Send(new PutAttachmentOperation("users/1", "PROFILE", profileStream, "image/PNG"));
                    Assert.True(result.ChangeVector.StartsWith("A:4"));
                    Assert.Equal("PROFILE", result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("image/PNG", result.ContentType);
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", result.Hash);
                    Assert.Equal(3, result.Size);
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    var attachment = attachments.Single();
                    Assert.Equal("PROFILE", attachment.GetString(nameof(AttachmentName.Name)));
                    Assert.Equal("image/PNG", attachment.GetString(nameof(AttachmentName.ContentType)));
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachment.GetString(nameof(AttachmentName.Hash)));
                    Assert.Equal(3, attachment.GetLong(nameof(AttachmentName.Size)));
                }

                AssertAttachmentCount(store, 1);
            }
        }

        [Fact]
        public void ThrowNotReadableStream()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    profileStream.Dispose();
                    var exceptoin = Assert.Throws<InvalidOperationException>(
                        () => store.Operations.Send(new PutAttachmentOperation("users/1", "Profile", profileStream, "image/png")));
                    Assert.Equal("Cannot put an attachment with a not readable stream. Make sure that the specified stream is readable and was not disposed.", exceptoin.Message);
                }
            }
        }

        [Fact]
        public void ThrowIfStreamWithPositionNotZero()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }

                using (var stream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    stream.Position = 2;

                    var exceptoin = Assert.Throws<InvalidOperationException>(
                        () => store.Operations.Send(new PutAttachmentOperation("users/1", "Profile", stream, "image/jpeg")));
                    Assert.Equal($"Cannot put an attachment with a stream that have position which isn't zero (The position is: {2}) since this is most of the time not intended and it is a common mistake.", exceptoin.Message);
                }
            }
        }
    }
}
