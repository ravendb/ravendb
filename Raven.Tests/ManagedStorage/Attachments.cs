//-----------------------------------------------------------------------
// <copyright file="Attachments.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Xunit;
using System.Linq;

namespace Raven.Tests.ManagedStorage
{
    public class Attachments : TxStorageTest
    {
        [Fact]
        public void CanAddAndReadAttachments()
        {
            using (var tx = NewTransactionalStorage())
            {
                tx.Batch(accessor => accessor.Attachments.AddAttachment("Ayende", null, new byte[] { 1, 2, 3 }, new JObject()));

                Attachment attachment = null;
                tx.Batch(viewer =>
                {
                    attachment = viewer.Attachments.GetAttachment("Ayende");
                });

                Assert.Equal(new byte[] { 1, 2, 3 }, attachment.Data);
            }
        }

        [Fact]
        public void CanScanAttachments()
        {
            using (var tx = NewTransactionalStorage())
            {
                tx.Batch(accessor =>
                {
                    accessor.Attachments.AddAttachment("1", null, new byte[] { 1, 2, 3 }, new JObject());
                    accessor.Attachments.AddAttachment("2", null, new byte[] { 1, 2, 3 }, new JObject());
                    accessor.Attachments.AddAttachment("3", null, new byte[] { 1, 2, 3 }, new JObject());
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
                    accessor.Attachments.AddAttachment("1", null, new byte[] { 1, 2, 3 }, new JObject());
                    accessor.Attachments.AddAttachment("2", null, new byte[] { 1, 2, 3 }, new JObject());
                    accessor.Attachments.AddAttachment("3", null, new byte[] { 1, 2, 3 }, new JObject());
                });

                tx.Batch(viewer =>
                {
                    var attachments = viewer.Attachments.GetAttachmentsAfter(Guid.Empty).ToArray();
                    var strings = viewer.Attachments.GetAttachmentsAfter(Guid.Empty).Select(x => x.Key).ToArray();
                    Assert.Equal(new[] { "1", "2", "3" }, strings);
                    Assert.Equal(new[] { "2", "3" }, viewer.Attachments.GetAttachmentsAfter(attachments[0].Etag).Select(x => x.Key).ToArray());
                    Assert.Equal(new[] {  "3" }, viewer.Attachments.GetAttachmentsAfter(attachments[1].Etag).Select(x => x.Key).ToArray());
                    Assert.Equal(new string[] {  }, viewer.Attachments.GetAttachmentsAfter(attachments[2].Etag).Select(x => x.Key).ToArray());
                });

            }
        }


        [Fact]
        public void CanAddAndReadAttachmentsAfterReopen()
        {
            using (var tx = NewTransactionalStorage())
            {
                tx.Batch(accessor => accessor.Attachments.AddAttachment("Ayende", null, new byte[] { 1, 2, 3 }, new JObject()));
            }

            using (var tx = NewTransactionalStorage())
            {
                Attachment attachment = null;
                tx.Batch(viewer =>
                {
                    attachment = viewer.Attachments.GetAttachment("Ayende");
                });

                Assert.Equal(new byte[] { 1, 2, 3 }, attachment.Data);
            }
        }

        [Fact]
        public void CanDeleteAttachment()
        {
            using (var tx = NewTransactionalStorage())
            {
                tx.Batch(accessor => accessor.Attachments.AddAttachment("Ayende", null, new byte[] { 1, 2, 3 }, new JObject()));
                tx.Batch(accessor => accessor.Attachments.DeleteAttachment("Ayende", null));
            }

            using (var tx = NewTransactionalStorage())
            {
                tx.Batch(viewer => Assert.Null(viewer.Attachments.GetAttachment("Ayende")));
            }
        }
    }
}