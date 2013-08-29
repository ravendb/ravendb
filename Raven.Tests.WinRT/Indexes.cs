using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Tests.WinRT
{
	[TestClass]
	public class Indexes : RavenTestBase
	{
		[TestMethod]
		public async Task ParseJson()
		{
			string text = string.Format(@"{{
  ""AlbumArtUrl"": ""/Content/Images/placeholder.gif"",
  ""Genre"": {{
    ""Id"": ""genres/1"",
    ""Name"": ""Rock""
  }},
  ""Price"": 8.99,
  ""Title"": ""No More Tears (Remastered)"",
  ""CountSold"": 0,
  ""Artist"": {{
    ""Id"": ""artists/114"",
    ""Name"": ""Ozzy Osbourne""
  }}
}}");
   
			var reader = new JsonTextReaderAsync(new StringReader(text));
			var result = await RavenJObject.LoadAsync(reader);

			Assert.IsNotNull(result);
		}


		[TestMethod]
		public async Task TaskIsFaultedWhenDeletingIndexFails()
		{
			using (var store = new DocumentStore
			{
				Url = Url,
				Conventions =
				{
					FailoverBehavior = FailoverBehavior.FailImmediately
				}
			}.Initialize())
			{
				try
				{
					await store.AsyncDatabaseCommands.ForDatabase("NonExistent").DeleteIndexAsync("NonExistent");
				}
				catch (Exception e)
				{
					return;
				}
				throw new AssertFailedException("Should throw exception, but did not.");
			}
		}

		[TestMethod]
		public async Task CanGetIndexNamesAsync()
		{
			var dbname = GenerateNewDatabaseName("Indexes.CanGetIndexNamesAsync");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);
				await store.AsyncDatabaseCommands.ForDatabase(dbname).PutIndexAsync("test", new IndexDefinition
				{
					Map = "from doc in docs select new { doc.Name}"
				}, true);

				var result = await store.AsyncDatabaseCommands.ForDatabase(dbname).GetIndexNamesAsync(0, 25);
				Assert.IsTrue(result.Any(x => x == "test"));
			}
		}

		[TestMethod]
		public async Task CanGetIndexesAsync()
		{
			var dbname = GenerateNewDatabaseName("Indexes.CanGetIndexesAsync");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				await store.AsyncDatabaseCommands.ForDatabase(dbname).PutIndexAsync("test", new IndexDefinition
				{
					Map = "from doc in docs select new { doc.Name}"
				}, true);

				var indexes = await store.AsyncDatabaseCommands.ForDatabase(dbname).GetIndexesAsync(0, 25);
				Assert.IsTrue(indexes.Any(x => x.Name == "test"));
			}
		}

		[TestMethod]
		public async Task CanPutAnIndexAsync()
		{
			var dbname = GenerateNewDatabaseName("Indexes.CanPutAnIndexAsync");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				await store.AsyncDatabaseCommands
				           .ForDatabase(dbname)
				           .PutIndexAsync("Test", new IndexDefinition
				           {
					           Map = "from doc in docs.Companies select new { doc.Name }"
				           }, true);

				var names = await store.AsyncDatabaseCommands
				                       .ForDatabase(dbname)
				                       .GetIndexNamesAsync(0, 25);

				Assert.IsTrue(names.Contains("Test"));
			}
		}

		[TestMethod]
		public async Task CanDeleteAnIndexAsync()
		{
			var dbname = GenerateNewDatabaseName("Indexes.CanDeleteAnIndexAsync");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				await store.AsyncDatabaseCommands
				           .ForDatabase(dbname)
				           .PutIndexAsync("Test", new IndexDefinition
				           {
					           Map = "from doc in docs.Companies select new { doc.Name }"
				           }, true);

				var verify_put = await store.AsyncDatabaseCommands
				                            .ForDatabase(dbname)
				                            .GetIndexNamesAsync(0, 25);

				Assert.IsTrue(verify_put.Contains("Test"));

				await store.AsyncDatabaseCommands
				           .ForDatabase(dbname)
				           .DeleteIndexAsync("Test");

				var verify_delete = await store.AsyncDatabaseCommands
				                               .ForDatabase(dbname)
				                               .GetIndexNamesAsync(0, 25);

				//NOTE: this is failing because Silverlight is caching the response from the first verification
				Assert.IsFalse(verify_delete.Contains("Test"));
			}
		}

		[TestMethod]
		public async Task CanGetASingleIndexByName()
		{
			var dbname = GenerateNewDatabaseName("Indexes.CanGetASingleIndexByName");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				await store.AsyncDatabaseCommands
				           .ForDatabase(dbname)
				           .PutIndexAsync("Test", new IndexDefinition
				           {
					           Map = "from doc in docs.Companies select new { doc.Name }"
				           }, true);

				var result = await store.AsyncDatabaseCommands.ForDatabase(dbname).GetIndexAsync("Test");
				Assert.AreEqual("Test", result.Name);
			}
		}
	}
}