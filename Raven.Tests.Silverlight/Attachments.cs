using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Silverlight.Testing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Document;
using Raven.Client.Extensions;

namespace Raven.Tests.Silverlight
{
	public class Attachments : RavenTestBase
	{
		[Asynchronous]
		public IEnumerable<Task> CanPutAndGetAnAttachment()
		{
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				var dbname = GenerateNewDatabaseName();
				yield return documentStore.AsyncDatabaseCommands.GlobalAdmin.EnsureDatabaseExistsAsync(dbname);

				const string someData = "The quick brown fox jumps over the lazy dog";
				var encoding = new UTF8Encoding();
				var bytes = encoding.GetBytes(someData);

				yield return documentStore.AsyncDatabaseCommands
					.ForDatabase(dbname)
					.PutAttachmentAsync("123", Etag.Empty, bytes, null);

				var get = documentStore.AsyncDatabaseCommands
					.ForDatabase(dbname)
					.GetAttachmentAsync("123");
				yield return get;

				var returnedBytes = get.Result.Data().ReadData();
				var returned = encoding.GetString(returnedBytes, 0, returnedBytes.Length);

				Assert.AreEqual(someData, returned);
			}
		}

		[Asynchronous]
		public IEnumerable<Task> CanDeleteAnAttachment()
		{
			using (var documentStore = new DocumentStore {Url = Url + Port}.Initialize())
			{
				var dbname = GenerateNewDatabaseName();
				yield return documentStore.AsyncDatabaseCommands.GlobalAdmin.EnsureDatabaseExistsAsync(dbname);

				const string someData = "The quick brown fox jumps over the lazy dog";
				var encoding = new UTF8Encoding();
				var bytes = encoding.GetBytes(someData);

				yield return documentStore.AsyncDatabaseCommands
					.ForDatabase(dbname)
					.PutAttachmentAsync("123", Etag.Empty, bytes, null);

				yield return documentStore.AsyncDatabaseCommands
					.ForDatabase(dbname)
					.DeleteAttachmentAsync("123", null);

				var get = documentStore.AsyncDatabaseCommands
					.ForDatabase(dbname)
					.GetAttachmentAsync("123");
				yield return get;

				Assert.AreEqual(null, get.Result);
			}
		}
	}
}