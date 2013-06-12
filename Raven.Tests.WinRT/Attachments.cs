using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Extensions;

namespace Raven.Tests.WinRT
{
	[TestClass]
	public class Attachments : RavenTestBase
	{
		[TestMethod]
		public async Task CanPutAndGetAnAttachment()
		{
			using (var store = NewDocumentStore())
			{
				var dbname = GenerateNewDatabaseName("CanPutAndGetAnAttachment.CanPutAndGetAnAttachment");
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				const string someData = "The quick brown fox jumps over the lazy dog";
				var encoding = new UTF8Encoding();
				var bytes = encoding.GetBytes(someData);

				await store.AsyncDatabaseCommands
				           .ForDatabase(dbname)
				           .PutAttachmentAsync("123", Etag.Empty, bytes, null);

				var attachment = await store.AsyncDatabaseCommands
				                            .ForDatabase(dbname)
				                            .GetAttachmentAsync("123");

				var returnedBytes = attachment.Data().ReadData();
				var returned = encoding.GetString(returnedBytes, 0, returnedBytes.Length);

				Assert.AreEqual(someData, returned);
			}
		}

		[TestMethod]
		public async Task CanDeleteAnAttachment()
		{
			using (var store = NewDocumentStore())
			{
				var dbname = GenerateNewDatabaseName("CanPutAndGetAnAttachment.CanDeleteAnAttachment");
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				const string someData = "The quick brown fox jumps over the lazy dog";
				var encoding = new UTF8Encoding();
				var bytes = encoding.GetBytes(someData);

				await store.AsyncDatabaseCommands
				           .ForDatabase(dbname)
				           .PutAttachmentAsync("123", Etag.Empty, bytes, null);

				await store.AsyncDatabaseCommands
				           .ForDatabase(dbname)
				           .DeleteAttachmentAsync("123", null);

				var attachment = await store.AsyncDatabaseCommands
				                     .ForDatabase(dbname)
				                     .GetAttachmentAsync("123");

				Assert.AreEqual(null, attachment);
			}
		}
	}
}