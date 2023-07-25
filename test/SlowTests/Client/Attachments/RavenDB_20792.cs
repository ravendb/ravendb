using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Client.Attachments
{
    public class RavenDB_20792 : ReplicationTestBase
    {
        public RavenDB_20792(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Attachments)]
        public async Task CanUpdateAttachmentWithOptimisticConcurrency()
        {
            using var store = GetDocumentStore();

            const string id = "users/1";
            const string attachmentName = "profile.png";

            using (var session = store.OpenAsyncSession())
            using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
            {
                await session.StoreAsync(new User(), id);
                session.Advanced.Attachments.Store(id, attachmentName, profileStream, contentType: "image/png");

                await session.SaveChangesAsync();
            }

            string changeVector;
            using (var session = store.OpenAsyncSession())
            {
                var attachmentResult = await session.Advanced.Attachments.GetAsync(id, attachmentName);
                changeVector = attachmentResult.Details.ChangeVector;
            }

            using (var newProfileStream = new MemoryStream(new byte[] { 4, 5, 6 }))
            {
                // update attachment stream and content type with optimistic concurrency 
                // should not throw concurrency exception

                var operation = new PutAttachmentOperation(id, attachmentName, newProfileStream, contentType: "image/jpg", changeVector: changeVector);
                await store.Operations.SendAsync(operation);
            }
        }
    }
}
