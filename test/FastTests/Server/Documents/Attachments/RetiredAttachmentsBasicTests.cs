using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Attachments.Retired;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Attachments
{
    public class RetiredAttachmentsBasicTests : RavenTestBase
    {
        public RetiredAttachmentsBasicTests(ITestOutputHelper output) : base(output)
        {
        }

      

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task ShouldThrowUsingRetiredAttachmentsApiOnRegularAttachment()
        {
            using (var store = GetDocumentStore())
            {
                var id = "Orders/3";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Query.Order
                    {
                        Id = id,
                        OrderedAt = new DateTime(2024, 1, 1),
                        ShipVia = $"Shippers/2",
                        Company = $"Companies/2"
                    });

                    await session.SaveChangesAsync();
                }

                using var profileStream = new MemoryStream([1, 2, 3]);
                await store.Operations.SendAsync(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));

                var res = await store.Operations.SendAsync(new GetAttachmentOperation(id, "test.png", AttachmentType.Document, null));
                Assert.Equal("test.png", res.Details.Name);

                await Assert.ThrowsAsync(typeof(RavenException),
                    async () => await store.Operations.SendAsync(new GetRetiredAttachmentOperation(id, "test.png")));
                await Assert.ThrowsAsync(typeof(RavenException),
                    async () => await store.Operations.SendAsync(new DeleteRetiredAttachmentOperation(id, "test.png")));
                await Assert.ThrowsAsync(typeof(RavenException),
                    async () => await store.Operations.SendAsync(new GetRetiredAttachmentsOperation(new List<AttachmentRequest> { new(id, "test.png") })));
            }
        }

    }
}
