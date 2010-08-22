using System.Linq;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
	public class UsingEnumInLinq : LocalClientTest
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