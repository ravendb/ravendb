using System;
using System.IO;
using Raven.Json.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class EmptyAttachments : RavenTest
	{
		[Fact]
		public void CanSaveAndLoad()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutAttachment("a", null, new MemoryStream(), new RavenJObject());

				var attachment = store.DatabaseCommands.GetAttachment("a");

				Assert.Equal(0, attachment.Data().Length);
			}
		}

		[Fact]
		public void CanSaveAndIterate()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutAttachment("a", null, new MemoryStream(), new RavenJObject());

				store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
				{
					accessor.Attachments.GetAttachmentsAfter(Guid.Empty, 100, long.MaxValue).ToList();
				});
			}
		}
	}
}