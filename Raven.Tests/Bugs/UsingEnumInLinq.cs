//-----------------------------------------------------------------------
// <copyright file="UsingEnumInLinq.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class UsingEnumInLinq : RavenTest
	{
		[Fact]
		public void Query()
		{
			using(var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("Items/ByType",
				                                new IndexDefinition
				                                {
				                                	Map = "from i in docs.Items select new { i.Type }"
				                                });

				const ItemType itemType = ItemType.Super;
				using(var session = store.OpenSession())
				{
					session.Store(new Item
					{
						Type = itemType
					});
					session.SaveChanges();
				}

				using(var session = store.OpenSession())
				{
					var q = from item in session.Query<Item>("Items/ByType")
								.Customize(x=>x.WaitForNonStaleResults())
							where item.Type == itemType
					        select item;

					Assert.Equal(1, q.Count());
				}
			}
		}

		public class Item
		{
			public string Id { get; set; }
			public ItemType Type { get; set; }
		}

		public enum ItemType
		{
			Normal,
			Super
		}
	}
}
