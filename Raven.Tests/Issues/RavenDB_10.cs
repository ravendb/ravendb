using Xunit;
using System.Linq;

namespace Raven.Tests.Issues
{
	public class RavenDB_10 : RavenTest
	{
		public class Item
		{
			public int Age { get; set; }
		}

		[Fact]
		public void ShouldSortCorrectly()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item { Age = 10 });
					session.Store(new Item { Age = 3 });

					session.SaveChanges();
				}
				using(var session = store.OpenSession())
				{
					var items = session.Query<Item>()
						.Customize(x => x.WaitForNonStaleResults())
						.OrderBy(x => x.Age)
						.ToList();


					Assert.Equal(3, items[0].Age);
					Assert.Equal(10, items[1].Age);
				}
			}
		}
	}
}