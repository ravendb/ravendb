namespace Raven.Tests.Silverlight
{
	using System;
	using System.Collections.Generic;
	using System.Text;
	using System.Threading.Tasks;
	using Client.Document;
	using Client.Extensions;
	using Microsoft.Silverlight.Testing;
	using Microsoft.VisualStudio.TestTools.UnitTesting;

	public class Attachments : RavenTestBase
	{
		[Asynchronous]
		public IEnumerable<Task> Can_put_and_get_an_attachment()
		{
			var store = new DocumentStore {Url = Url + Port};
			store.Initialize();

			var dbname = GenerateNewDatabaseName();
			yield return store.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			const string someData = "The quick brown fox jumps over the lazy dog";
			var encoding = new UTF8Encoding();
			var bytes = encoding.GetBytes(someData);

			yield return store.AsyncDatabaseCommands
				.ForDatabase(dbname)
				.PutAttachmentAsync("123", Guid.Empty, bytes, null);

			var get = store.AsyncDatabaseCommands
				.ForDatabase(dbname)
				.GetAttachmentAsync("123");
			yield return get;
			
			var returnedBytes = get.Result.Data;
			var returned = encoding.GetString(returnedBytes,0,returnedBytes.Length);

			Assert.AreEqual(someData, returned);
		}

		[Asynchronous]
		public IEnumerable<Task> Can_delete_an_attachment()
		{
			var store = new DocumentStore {Url = Url + Port};
			store.Initialize();

			var dbname = GenerateNewDatabaseName();
			yield return store.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			const string someData = "The quick brown fox jumps over the lazy dog";
			var encoding = new UTF8Encoding();
			var bytes = encoding.GetBytes(someData);

			yield return store.AsyncDatabaseCommands
				.ForDatabase(dbname)
				.PutAttachmentAsync("123", Guid.Empty, bytes, null);

			yield return store.AsyncDatabaseCommands
				.ForDatabase(dbname)
				.DeleteAttachmentAsync("123", null);

			var get = store.AsyncDatabaseCommands
				.ForDatabase(dbname)
				.GetAttachmentAsync("123");
			yield return get;
			
			Assert.AreEqual(null, get.Result);
		}
	}
}