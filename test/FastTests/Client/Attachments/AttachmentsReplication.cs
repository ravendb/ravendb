using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations;
using Xunit;
using Raven.Client;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace FastTests.Client.Attachments
{
    public class AttachmentsReplication : ReplicationTestsBase
    {
        [Fact(Skip = "Work in progres")]
        public void PutAttachments()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
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
                    Assert.Equal(2, result.Etag);
                    Assert.Equal(names[0], result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("image/png", result.ContentType);
                }
                using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                {
                    var result = store1.Operations.Send(new PutAttachmentOperation("users/1", names[1], backgroundStream, "image/jpeg"));
                    Assert.Equal(4, result.Etag);
                    Assert.Equal(names[1], result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("image/jpeg", result.ContentType);
                }
                using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    var result = store1.Operations.Send(new PutAttachmentOperation("users/1", names[2], fileStream, null));
                    Assert.Equal(6, result.Etag);
                    Assert.Equal(names[2], result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("", result.ContentType);
                }

                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Marker" }, "marker");
                    session.SaveChanges();
                }

                SetupReplication(store1, store2);
                Assert.True(WaitForDocument(store2, "marker"));

                using (var session = store2.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    Assert.Equal(string.Join(",", names.OrderBy(x => x)), metadata[Constants.Documents.Metadata.Attachments]);
                }

                using (var session = store2.OpenSession())
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
                                Assert.Equal(new byte[] {1, 2, 3}, readBuffer.Take(3));
                            else if (i == 1)
                                Assert.Equal(new byte[] {10, 20, 30, 40, 50}, readBuffer.Take(5));
                            else if (i == 2)
                                Assert.Equal(new byte[] {1, 2, 3, 4, 5}, readBuffer.Take(5));
                        }
                    }
                    using (var attachmentStream = new MemoryStream(readBuffer))
                    {
                        var notExistsAttachment = session.Advanced.GetAttachment("users/1", "not-there", (result, stream) => stream.CopyTo(attachmentStream));
                        Assert.Null(notExistsAttachment);
                    }
                }

                var statistics = store2.Admin.Send(new GetStatisticsOperation());
                Assert.Equal(3, statistics.CountOfAttachments);
                Assert.Equal(1, statistics.CountOfDocuments);
                Assert.Equal(0, statistics.CountOfIndexes);
            }
        }

        private class User
        {
            public string Name { get; set; }
        }
    }
}