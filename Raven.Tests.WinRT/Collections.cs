using System.Threading.Tasks;
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
using Raven.Client.Connection.Async;
using Raven.Client.Extensions;
using Raven.Tests.Document;
using Raven.Tests.WinRT.Entities;

namespace Raven.Tests.WinRT
{
	[TestClass]
	public class Collections : RavenTestBase
	{
		[TestMethod]
		public async Task Can_get_collections_async()
		{
			var dbname = GenerateNewDatabaseName("Collections.Can_get_collections_async");
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands.Admin.EnsureDatabaseExistsAsync(dbname);

				using (var session = store.OpenAsyncSession(dbname))
				{
					for (int i = 0; i < 25; i++)
					{
						await session.StoreAsync(new Company {Id = "Companies/" + i, Name = i.ToString()});
						await session.StoreAsync(new Order {Id = "Orders/" + i, Note = i.ToString()});
					}

					await session.SaveChangesAsync();
				}


				NameAndCount[] collections;
				do
				{
					collections = await store.AsyncDatabaseCommands
					                  .ForDatabase(dbname)
					                  .GetTermsCount("Raven/DocumentsByEntityName", "Tag", "", 25);
					
					if (collections.Length == 0)
						await TaskEx.Delay(100);
				} while (collections.Length == 0);

				Assert.AreEqual("Companies", collections[0].Name);
				Assert.AreEqual("Orders", collections[1].Name);
				Assert.AreEqual(25, collections[0].Count);
				Assert.AreEqual(25, collections[1].Count);
			}
		}
	}
}