using System.IO;
using System.Linq;
using Raven.Client.Documents.Operations;
using Xunit;
using Raven.Client;
using Raven.Server.Documents;

namespace FastTests.Client.Attachments
{
    public class AttachmentsCrud : RavenTestBase
    {
        [Fact]
        public void PutAttachments()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }

                var names = new[]
                {
                    "profile.png",
                    "background-photo.jpg",
                    //TODO Not working with hebrew id: "fileNANE_#$1^%_בעברית.txt"
                    "fileNANE.txt"
                };
                using (var profileStream = new MemoryStream(new byte[] {1, 2, 3}))
                using (var backgroundStream = new MemoryStream(new byte[] {10, 20, 30, 40, 50}))
                using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    store.Operations.Send(new PutAttachmentOperation("users/1", names[0], profileStream, "image/png"));
                    store.Operations.Send(new PutAttachmentOperation("users/1", names[1], backgroundStream, "image/jpeg"));
                    store.Operations.Send(new PutAttachmentOperation("users/1", names[2], fileStream, null));
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    Assert.Equal(string.Join(",", names.OrderBy(x => x)), metadata[Constants.Documents.Metadata.Attachments]);
                }

                var readBuffer = new byte[8];
                for (var i = 0; i < names.Length; i++)
                {
                    var name = names[i];
                    using (var attachmentStream = new MemoryStream(readBuffer))
                    {
                        var attachment = store.Operations.Send(new GetAttachmentOperation("users/1", name, stream => stream.CopyTo(attachmentStream)));
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
            }
        }

        private class User
        {
            public string Name { get; set; }
        }
    }
}