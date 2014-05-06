// -----------------------------------------------------------------------
//  <copyright file="Crud.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;
using Raven.Json.Linq;
using Xunit;
using Raven.Abstractions.Data;

namespace Raven.Tests.Core.Commands
{
    public class Attachments : RavenCoreTestBase
    {
        [Fact]
        public async Task CanPutUpdateMetadataAndDeleteAttachment()
        {
            using (var store = GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAttachmentAsync("items/1", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());
                await store.AsyncDatabaseCommands.PutAttachmentAsync("items/2", null, new MemoryStream(new byte[] { 4, 5, 6 }), new RavenJObject());

                Assert.NotNull(await store.AsyncDatabaseCommands.GetAttachmentAsync("items/1"));
                Assert.NotNull(await store.AsyncDatabaseCommands.GetAttachmentAsync("items/2"));

                var attachments = await store.AsyncDatabaseCommands.GetAttachmentsAsync(Etag.Empty, 10);
                Assert.Equal(2, attachments.Length);

                await store.AsyncDatabaseCommands.UpdateAttachmentMetadataAsync("items/1", null, new RavenJObject() { { "attachment_key", "value" } });
                await store.AsyncDatabaseCommands.UpdateAttachmentMetadataAsync("items/2", null, new RavenJObject() { { "attachment_key2", "value2" } });

                var attachment = await store.AsyncDatabaseCommands.GetAttachmentAsync("items/1");
                Assert.Equal("value", attachment.Metadata.Value<string>("attachment_key"));
                var attachmentMetadata = await store.AsyncDatabaseCommands.HeadAttachmentAsync("items/1");
                Assert.Equal("value", attachmentMetadata.Metadata.Value<string>("attachment_key"));

                var attachmentsMetadata = await store.AsyncDatabaseCommands.GetAttachmentHeadersStartingWithAsync("items", 0, 5);
                await attachmentsMetadata.MoveNextAsync();
                Assert.Equal("value", attachmentsMetadata.Current.Metadata.Value<string>("attachment_key"));
                await attachmentsMetadata.MoveNextAsync();
                Assert.Equal("value2", attachmentsMetadata.Current.Metadata.Value<string>("attachment_key2"));

                await store.AsyncDatabaseCommands.DeleteAttachmentAsync("items/1", null);
                Assert.Null(await store.AsyncDatabaseCommands.GetAttachmentAsync("items/1"));
            }
        }
    }
}
