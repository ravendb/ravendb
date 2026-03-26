using System.IO;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.Attachments
{
    public class AttachmentsPatchSlowTests : RemoteAttachmentsS3Base
    {
        public AttachmentsPatchSlowTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Attachments | RavenTestCategory.Patching)]
        public async Task CanDeleteAttachmentsInPatch()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/1";
                const string attachmentName1 = "file1.txt";
                const string attachmentName2 = "file2.txt";

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "John" }, id);
                    await session.SaveChangesAsync();
                }

                using (var stream1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                using (var stream2 = new MemoryStream(new byte[] { 4, 5, 6 }))
                {
                    await store.Operations.SendAsync(new PutAttachmentOperation(id, attachmentName1, stream1, "text/plain"));
                    await store.Operations.SendAsync(new PutAttachmentOperation(id, attachmentName2, stream2, "text/plain"));
                }

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    var metadata = session.Advanced.GetMetadataFor(user);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Assert.Equal(2, attachments.Length);
                }

                var patch = new PatchRequest
                {
                    Script = "attachments(this, args.name).delete();",
                    Values =
                    {
                        { "name", attachmentName1 }
                    }
                };

                var result = await store.Operations.SendAsync(new PatchOperation(id, null, patch));
                Assert.Equal(PatchStatus.Patched, result);

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    var metadata = session.Advanced.GetMetadataFor(user);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Assert.Equal(1, attachments.Length);
                    Assert.Equal(attachmentName2, attachments[0][nameof(AttachmentName.Name)].ToString());
                }

                using (var attachmentResult = await store.Operations.SendAsync(new GetAttachmentOperation(id, attachmentName1, AttachmentType.Document, null)))
                {
                    Assert.Null(attachmentResult);
                }

                using (var attachmentResult = await store.Operations.SendAsync(new GetAttachmentOperation(id, attachmentName2, AttachmentType.Document, null)))
                {
                    Assert.NotNull(attachmentResult);
                    Assert.Equal(attachmentName2, attachmentResult.Details.Name);
                }
            }
        }
    }
}
