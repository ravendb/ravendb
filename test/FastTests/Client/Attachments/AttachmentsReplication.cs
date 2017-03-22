using System.IO;
using System.Linq;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations;
using Xunit;
using Raven.Client;
using Raven.Server.Documents;

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
                    var database1 = GetDocumentDatabaseInstanceFor(store1).Result;
                    database1.Configuration.Replication.MaxItemsCount = null;
                    database1.Configuration.Replication.MaxSizeToSend = null;
                    SetupReplication(store1, store2);
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
                    var database1 = GetDocumentDatabaseInstanceFor(store1).Result;
                    database1.Configuration.Replication.MaxItemsCount = null;
                    database1.Configuration.Replication.MaxSizeToSend = null;
                    SetupReplication(store1, store2);
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

                var statistics = store2.Admin.Send(new GetStatisticsOperation());
                Assert.Equal(3, statistics.CountOfAttachments);
                Assert.Equal(2, statistics.CountOfDocuments);
                Assert.Equal(0, statistics.CountOfIndexes);

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

        private class User
        {
            public string Name { get; set; }
        }
    }
}