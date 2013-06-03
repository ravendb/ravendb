using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;

namespace Raven.Tests.WinRT
{
	using System.Threading.Tasks;
	using Client.Document;
	using Document;
	using Entities;

	/// <summary>
	/// Not actually a test, just an easy way for me to insert some sample data
	/// </summary>
	public class Generate : RavenTestBase
	{
		[Ignore]
		[TestMethod]
		public async Task Some_sample_data()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenAsyncSession())
				{
					for (int i = 0; i < 25; i++)
					{
						await session.StoreAsync(new Company {Id = "Companies/" + i, Name = i.ToString()});
						await session.StoreAsync(new Order {Id = "Orders/" + i, Note = i.ToString()});
						await session.StoreAsync(new Customer {Name = "Joe " + i});
						await session.StoreAsync(new Contact {FirstName = "Bob" + i, Surname = i.ToString() + "0101001"});
					}

					await session.StoreAsync(new Customer {Name = "Henry"});
					await session.StoreAsync(new Order {Note = "An order"});
					await session.StoreAsync(new Company {Name = "My Company"});

					await session.SaveChangesAsync();
				}
			}
		}

		[Ignore]
		[TestMethod]
		public async Task Some_indexing_errors()
		{
			using (var store = NewDocumentStore())
			{
				await store.AsyncDatabaseCommands
				           .PutIndexAsync("pagesByTitle2",
				                          new IndexDefinition
				                          {
					                          Map = @"
					from doc in docs
					where doc.type == ""page""
					select new {  f = 2 / doc.size };"
				                          }, true);

				await store.AsyncDatabaseCommands
				           .PutAsync("1", null,
				                     RavenJObject.Parse(
					                     @"{
								type: 'page', 
								some: 'val', 
								other: 'var', 
								content: 'this is the content', 
								title: 'hello world', 
								size: 0,
								'@metadata': {'@id': 1}
							}"),
				                     new RavenJObject());

				for (int i = 0; i < 50; i++)
				{
					var result = await store.AsyncDatabaseCommands
					                 .QueryAsync("pagesByTitle2", new IndexQuery
					                 {
						                 Query = "f:val",
						                 Start = 0,
						                 PageSize = 10
					                 }, null);

					if (result.IsStale)
					{
						await TaskEx.Delay(100);
						continue;
					}
					Assert.AreNotEqual(0, result.TotalResults);
					break;
				}
			}
		}
	}
}