using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class OrderByCollectionCount : RavenTest
	{
		[Fact]
		public void CanOrderByTheCountOfASubSollection() {
			using (var store = NewDocumentStore()) {
				using (var session = store.OpenSession()) {
					session.Store(new OrderByCollectionCount_User {
						Name = "Test User 1",
						OwnsProducts = new List<OrderByCollectionCount_OwnsProduct>(new[] {
							new OrderByCollectionCount_OwnsProduct()	// Should be second with 1 product
						})
					});
					session.Store(new OrderByCollectionCount_User {
						Name = "Test User 2",
						OwnsProducts = new List<OrderByCollectionCount_OwnsProduct>(new[] {
							new OrderByCollectionCount_OwnsProduct(), new OrderByCollectionCount_OwnsProduct()	// Should be first with 2 products
						})
					});
					session.Store(new OrderByCollectionCount_User {
						Name = "Test User 3",
						OwnsProducts = new List<OrderByCollectionCount_OwnsProduct>()	// Should be filtered out with no products
					});

					session.SaveChanges();

					var users = session.Query<OrderByCollectionCount_User>()
						.Customize(d => d.WaitForNonStaleResults())
						.Where(x => x.OwnsProducts.Count > 0)
						.OrderByDescending(x => x.OwnsProducts.Count)
						.Take(5)
						.ToList();

					Assert.NotNull(users);
					Assert.Equal(2, users.Count);
					Assert.Equal(2, users[0].OwnsProducts.Count);
					Assert.Equal(1, users[1].OwnsProducts.Count);
				}
			}
		}
	}

	public class OrderByCollectionCount_User
	{
		public string Id { get; set; }

		public string Name { get; set; }

		public IList<OrderByCollectionCount_OwnsProduct> OwnsProducts { get; set; }
	}

	public class OrderByCollectionCount_OwnsProduct
	{
		public string Description { get; set; }
	}
}