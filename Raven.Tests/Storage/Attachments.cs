//-----------------------------------------------------------------------
// <copyright file="Attachments.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.IO;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using System.Linq;
using Raven.Abstractions.Extensions;
using Task = System.Threading.Tasks.Task;

namespace Raven.Tests.Storage
{
    public class Attachments : RavenTest
    {
        [Fact]
        public void CanAddAndReadAttachments()
        {
            using (var tx = NewTransactionalStorage())
            {
                tx.Batch(accessor => accessor.Attachments.AddAttachment("Ayende", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject()));

                Attachment attachment = null;
                tx.Batch(viewer =>
                {
                    attachment = viewer.Attachments.GetAttachment("Ayende");
                });

                tx.Batch(_ => Assert.Equal(new byte[] { 1, 2, 3 }, attachment.Data().ReadData()));
            }
        }

        [Fact]
        public async Task CanAddAndReadAttachmentsAsyncInEmbedded()
        {
            using (var store = NewDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAttachmentAsync("Ayende", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());

                Attachment attachment = await store.AsyncDatabaseCommands.GetAttachmentAsync("Ayende");
                Assert.Equal(new byte[] {1, 2, 3}, attachment.Data().ReadData());
            }
        }

        [Fact]
        public async Task CanAddAndReadAttachmentsAsyncInEmbedded2()
        {
            using (var store = NewDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAttachmentAsync("messagebodies/3f0240a7-9b2e-4e2a-ab39-6114932adad1\\2055783", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());

                Attachment attachment = await store.AsyncDatabaseCommands.GetAttachmentAsync("messagebodies/3f0240a7-9b2e-4e2a-ab39-6114932adad1\\2055783");
                Assert.Equal(new byte[] { 1, 2, 3 }, attachment.Data().ReadData());
            }
        }

        [Fact]
        public void CanScanAttachments()
        {
            using (var tx = NewTransactionalStorage())
            {
                tx.Batch(accessor =>
                {
                    accessor.Attachments.AddAttachment("1", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());
                    accessor.Attachments.AddAttachment("2", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());
                    accessor.Attachments.AddAttachment("3", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());
                });

                tx.Batch(viewer =>
                {
                    Assert.Equal(new[] { "3", "2", "1" }, viewer.Attachments.GetAttachmentsByReverseUpdateOrder(0).Select(x => x.Key).ToArray());
                    Assert.Equal(new[] { "2", "1" }, viewer.Attachments.GetAttachmentsByReverseUpdateOrder(1).Select(x => x.Key).ToArray());
                    Assert.Equal(new[] { "1" }, viewer.Attachments.GetAttachmentsByReverseUpdateOrder(2).Select(x => x.Key).ToArray());
                    Assert.Equal(new string[] { }, viewer.Attachments.GetAttachmentsByReverseUpdateOrder(3).Select(x => x.Key).ToArray());
                });

            }
        }


        [Fact]
        public void CanScanAttachmentsByEtag()
        {
            using (var tx = NewTransactionalStorage())
            {
                tx.Batch(accessor =>
                {
                    accessor.Attachments.AddAttachment("1", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());
                    accessor.Attachments.AddAttachment("2", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());
                    accessor.Attachments.AddAttachment("3", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject());
                });

                tx.Batch(viewer =>
                {
                    var attachments = viewer.Attachments.GetAttachmentsAfter(Etag.Empty, 100, long.MaxValue).ToArray();
                    var strings = viewer.Attachments.GetAttachmentsAfter(Etag.Empty, 100, long.MaxValue).Select(x => x.Key).ToArray();
                    Assert.Equal(new[] { "1", "2", "3" }, strings);
                    Assert.Equal(new[] { "2", "3" }, viewer.Attachments.GetAttachmentsAfter(attachments[0].Etag, 100, long.MaxValue).Select(x => x.Key).ToArray());
                    Assert.Equal(new[] { "3" }, viewer.Attachments.GetAttachmentsAfter(attachments[1].Etag, 100, long.MaxValue).Select(x => x.Key).ToArray());
                    Assert.Equal(new string[] { }, viewer.Attachments.GetAttachmentsAfter(attachments[2].Etag, 100, long.MaxValue).Select(x => x.Key).ToArray());
                });

            }
        }


        [Fact]
        public void CanAddAndReadAttachmentsAfterReopen()
        {
            string dataDir = NewDataPath();
            using (var tx = NewTransactionalStorage(dataDir: dataDir, runInMemory: false))
            {
                tx.Batch(accessor => accessor.Attachments.AddAttachment("Ayende", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject()));
            }

            using (var tx = NewTransactionalStorage(dataDir: dataDir, runInMemory: false))
            {
                Attachment attachment = null;
                tx.Batch(viewer =>
                {
                    attachment = viewer.Attachments.GetAttachment("Ayende");
                });

                tx.Batch(_ =>
                {
                    Assert.Equal(new byte[] { 1, 2, 3 }, attachment.Data().ReadData());					
                });
            }
        }

        [Fact]
        public void CanDeleteAttachment()
        {
            string dataDir = NewDataPath();
            using (var tx = NewTransactionalStorage(dataDir: dataDir))
            {
                tx.Batch(accessor => accessor.Attachments.AddAttachment("Ayende", null, new MemoryStream(new byte[] { 1, 2, 3 }), new RavenJObject()));
                tx.Batch(accessor => accessor.Attachments.DeleteAttachment("Ayende", null));
            }

            using (var tx = NewTransactionalStorage(dataDir: dataDir))
            {
                tx.Batch(viewer => Assert.Null(viewer.Attachments.GetAttachment("Ayende")));
            }
        }
    }
}
