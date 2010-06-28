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
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(accessor => accessor.Attachments.AddAttachment("Ayende", null, new byte[] { 1, 2, 3 }, new JObject()));

				Attachment attachment = null;
				tx.Read(viewer =>
				{
					attachment = viewer.Attachments.GetAttachment("Ayende");
				});

				Assert.Equal(new byte[] { 1, 2, 3 }, attachment.Data);
			}
		}

		[Fact]
		public void CanAddAndReadAttachmentsAfterReopen()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(accessor => accessor.Attachments.AddAttachment("Ayende", null, new byte[] { 1, 2, 3 }, new JObject()));
			}

			using (var tx = new TransactionalStorage("test"))
			{
				Attachment attachment = null;
				tx.Read(viewer =>
				{
					attachment = viewer.Attachments.GetAttachment("Ayende");
				});

				Assert.Equal(new byte[] { 1, 2, 3 }, attachment.Data);
			}
		}

		[Fact]
		public void CanDeleteAttachment()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(accessor => accessor.Attachments.AddAttachment("Ayende", null, new byte[] { 1, 2, 3 }, new JObject()));
				tx.Write(accessor => accessor.Attachments.DeleteAttachment("Ayende", null));
			}

			using (var tx = new TransactionalStorage("test"))
			{
				tx.Read(viewer => Assert.Null(viewer.Attachments.GetAttachment("Ayende")));
			}
		}
	}
}