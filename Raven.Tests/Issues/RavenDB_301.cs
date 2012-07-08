// -----------------------------------------------------------------------
//  <copyright file="RavenDB_301.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_301 : RavenTest
	{
		[Fact]
		public void CanUseTertiaryIncludes()
		{
			using(GetNewServer())
			using(var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				new Index().Execute(store);
				using(var session = store.OpenSession())
				{
					session.Store(new Item
					{
						Name = "Oren",
						Parent = null
					});
					session.Store(new Item
					{
						Name = "Ayende",
						Parent = "items/1"
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var a = session.Query<Item, Index>()
						.Customize(x => x.WaitForNonStaleResults())
						.Single(x => x.Name == "Ayende");

					session.Load<Item>(a.Parent);

					Assert.Equal(1, session.Advanced.NumberOfRequests);
				}
			}
		}

		public class Item
		{
			public string Name { get; set; }
			public string Id { get; set; }
			public string Parent { get; set; }
		}

		public class Index : AbstractIndexCreationTask<Item>
		{
			public Index()
			{
				Map = items => from item in items
				               select new {item.Name};
				TransformResults = (database, items) =>
				                   from item in items
								   let _ = database.Include(item.Parent)
				                   select item;
			}
		}
	}
}