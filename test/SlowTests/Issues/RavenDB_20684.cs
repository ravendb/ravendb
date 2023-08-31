using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Attachments;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20684 : RavenTestBase
    {
        public RavenDB_20684(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task AttachmentStreamShouldExistInRevisionAfterDeleteAttachment()
        {
            using (var store = GetDocumentStore())
            {
                string changeVector = null;
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Shiran" }, "users/1");
                    session.SaveChanges();
                }

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    var result = store.Operations.Send(new PutAttachmentOperation("users/1", "profile.png", profileStream, "image/png"));
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", result.Hash);
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    changeVector = session.Advanced.GetChangeVectorFor(user);
                }

                await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore);

                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Delete("users/1", "profile.png");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var revision = session.Advanced.Revisions.Get<User>(changeVector);
                    Assert.NotNull(revision);

                    var readBuffer = new byte[8];

                    using (var attachmentStream = new MemoryStream(readBuffer))
                    using (var attachment = session.Advanced.Attachments.GetRevision("users/1", "profile.png", changeVector))
                    {
                        Assert.NotNull(attachment);
                        Assert.NotNull(attachment.Stream);

                        await attachment.Stream.CopyToAsync(attachmentStream);

                        Assert.Equal(new byte[] { 1, 2, 3 }, readBuffer.Take(3));
                        Assert.Equal("image/png", attachment.Details.ContentType);
                        Assert.Equal(3, attachmentStream.Position);
                        Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", attachment.Details.Hash);
                    }
                }
            }
        }
    }
}
