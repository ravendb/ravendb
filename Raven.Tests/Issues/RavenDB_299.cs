// -----------------------------------------------------------------------
//  <copyright file="RavenDB_299.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client.Indexes;
using System.Linq;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_299 : RavenTest
	{
		public class Item
		{
			public Tag[] Tags { get; set; }
		}

		public class Tag
		{
			public string Name;
		}

		public class Index : AbstractIndexCreationTask<Item>
		{
			public Index()
			{
				Map = items =>
					  items.SelectMany(x => x.Tags, (item, tag) => new { tag.Name });
			}
		}

		[Fact]
		public void CanWorkWithSelectManyOverload()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new Item
					{
						Tags = new[]{new Tag{Name = "test"}, }
					});
					session.SaveChanges();
				}
				new Index().Execute(store);

				using (var session = store.OpenSession())
				{
					Assert.NotEmpty(session.Query<Item, Index>()
					                	.Customize(x => x.WaitForNonStaleResults())
					                	.ToList());
				}
			}
		}
	}
}