// -----------------------------------------------------------------------
//  <copyright file="RDBQA_16.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;

using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RDBQA_16 : RavenTest
    {
        [Fact]
        public async Task AttachmentMetadataShouldBeCaseInsensitive()
        {
            using (var store = NewDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAttachmentAsync("items/1", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());
                await store.AsyncDatabaseCommands.UpdateAttachmentMetadataAsync("items/1", null, new RavenJObject { { "attachment_key", "value" } });

                var attachment = await store.AsyncDatabaseCommands.GetAttachmentAsync("items/1");
                Assert.Equal("value", attachment.Metadata.Value<string>("attachment_key"));

                var attachmentsMetadata = await store.AsyncDatabaseCommands.GetAttachmentHeadersStartingWithAsync("items", 0, 5);
                await attachmentsMetadata.MoveNextAsync();
                Assert.Equal("value", attachmentsMetadata.Current.Metadata.Value<string>("attachment_key"));
            }
        }
    }
}
