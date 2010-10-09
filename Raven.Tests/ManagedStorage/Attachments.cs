using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Storage.Managed;
using Xunit;

namespace Raven.Storage.Tests
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