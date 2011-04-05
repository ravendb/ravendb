using Raven.Json.Linq;

namespace Raven.Tests.Silverlight
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Threading;
	using System.Threading.Tasks;
	using Client.Document;
	using Database.Data;
	using Database.Indexing;
	using Document;
	using Entities;
	using Microsoft.Silverlight.Testing;
	using Microsoft.VisualStudio.TestTools.UnitTesting;
	using Newtonsoft.Json.Linq;

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
				Enumerable.Range(0, 25).ToList()
					.ForEach(i => session.Store(new Company {Id = "Companies/" + i, Name = i.ToString()}));

				Enumerable.Range(0, 250).ToList()
					.ForEach(i => session.Store(new Order { Id = "Orders/" + i, Note = i.ToString() }));

				Enumerable.Range(0, 100).ToList()
					.ForEach(i => session.Store(new Customer { Name = "Joe " + i}));

				Enumerable.Range(0, 75).ToList()
					.ForEach(i => session.Store(new Contact { FirstName = "Bob" + i, Surname = i.ToString() + "0101001" }));

				session.Store(new Customer { Name = "Henry"});
				session.Store(new Order { Note = "An order" });
				session.Store(new Company {Name = "My Company"});

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
					yield return Delay(100);
					continue;
				}
				Assert.AreNotEqual(0, query.Result.TotalResults);
				yield break;
			}
		}
	}
}