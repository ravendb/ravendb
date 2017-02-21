using System.IO;
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
                    "fileNANE_#$1^%_בעברית.txt"
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

                    Assert.Equal(string.Join(",", names), metadata[Constants.Documents.Metadata.Attachments]);
                }

                /*  using (var ctx = FilesOperationContext.ShortTermSingleUse(database))
                                {
                                    ctx.OpenReadTransaction();

                                    var file = database.DocumentsStorage.Get(ctx, name);
                                    var stream = database.DocumentsStorage.GetStream(ctx, file.StreamIdentifier);
                                    Assert.NotNull(file);
                                    Assert.Equal(1, file.Etag);
                                    Assert.Equal(name, file.Name);
                                    Assert.Equal(5, stream.Length);
                                    var readBuffer = new byte[5];
                                    Assert.Equal(5, stream.Read(readBuffer, 0, 5));
                                    Assert.Equal(new byte[] {1, 2, 3, 4, 5}, readBuffer);
                                }*/
            }
        }

        private class User
        {
            public string Name { get; set; }
        }
    }
}