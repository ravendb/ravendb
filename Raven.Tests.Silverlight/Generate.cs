using System.Globalization;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Extensions;
using Raven.Json.Linq;

namespace Raven.Tests.Silverlight
{
	using System.Collections.Generic;
	using System.Linq;
	using System.Threading.Tasks;
	using Client.Document;
	using Document;
	using Entities;
	using Microsoft.Silverlight.Testing;
	using Microsoft.VisualStudio.TestTools.UnitTesting;

	/// <summary>
	/// Not actually a test, just an easy way for me to insert some sample data
	/// </summary>
	public class Generate : RavenTestBase
	{
		[Ignore]
		[Asynchronous]
		public IEnumerable<Task> Some_sample_data()
		{
			var store = new DocumentStore {Url = Url + Port};
			store.Initialize();

			using (var session = store.OpenAsyncSession())
			{
				foreach (var i in Enumerable.Range(0, 25))
				{
					yield return session.StoreAsync(new Company {Id = "Companies/" + i, Name = i.ToString(CultureInfo.InvariantCulture)});
				}

				foreach (var i in Enumerable.Range(0, 250))
				{
					yield return session.StoreAsync(new Order {Id = "Orders/" + i, Note = i.ToString(CultureInfo.InvariantCulture)});
				}

				foreach (var i in Enumerable.Range(0, 100))
				{
					yield return session.StoreAsync(new Customer {Name = "Joe " + i});
				}

				foreach (var i in Enumerable.Range(0, 75))
				{
					yield return session.StoreAsync(new Contact {FirstName = "Bob" + i, Surname = i.ToString(CultureInfo.InvariantCulture) + "0101001"});
				}

				yield return session.StoreAsync(new Customer {Name = "Henry"});
				yield return session.StoreAsync(new Order {Note = "An order"});
				yield return session.StoreAsync(new Company {Name = "My Company"});

				yield return session.SaveChangesAsync();
			}
		}

		[Ignore]
		[Asynchronous]
		public IEnumerable<Task> Some_indexing_errors()
		{
			var store = new DocumentStore { Url = Url + Port };
			store.Initialize();

			yield return store.AsyncDatabaseCommands
				.PutIndexAsync("pagesByTitle2",
						new IndexDefinition
						{
							Map = @"
					from doc in docs
					where doc.type == ""page""
					select new {  f = 2 / doc.size };"
						}, true);

			yield return store.AsyncDatabaseCommands
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
				var query = store.AsyncDatabaseCommands
					.QueryAsync("pagesByTitle2", new IndexQuery
						{
							Query = "f:val",
							Start = 0,
							PageSize = 10
						}, null);
				yield return (query);

				if (query.Result.IsStale)
				{
					yield return TaskEx.Delay(100);
					continue;
				}
				Assert.AreNotEqual(0, query.Result.TotalResults);
				yield break;
			}
		}
	}
}