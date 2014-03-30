// -----------------------------------------------------------------------
//  <copyright file="Aggregation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Raven.Tests.Issues
{
	using System;
	using System.Linq;

	using Raven.Abstractions.Indexing;
	using Raven.Client;
	using Raven.Client.Indexes;

	using Xunit;

	public class RavenDB_1207 : RavenTest
	{
		public class Order
		{
			public string Product { get; set; }
			public decimal Total { get; set; }
			public Currency Currency { get; set; }
			public int Quantity { get; set; }
		}

		public enum Currency
		{
			USD,
			EUR,
			NIS
		}

		public class Orders_All_Without_Sort : AbstractIndexCreationTask<Order>
		{
			public Orders_All_Without_Sort()
			{
				Map = orders =>
					  from order in orders
					  select new { order.Currency, order.Product, order.Total, order.Quantity };
			}
		}

		public class Orders_All_With_Sort : AbstractIndexCreationTask<Order>
		{
			public Orders_All_With_Sort()
			{
				Map = orders =>
					  from order in orders
					  select new { order.Currency, order.Product, order.Total, order.Quantity };

				Sort(x => x.Total, SortOptions.Double);
			}
		}

		[Fact]
		public void ShouldThrowWhenAggregatingOnNumericalFieldWithoutSortingSetInIndex()
		{
			var e = Assert.Throws<InvalidOperationException>(
				() =>
				{
					using (var store = this.NewDocumentStore())
					{
						new Orders_All_Without_Sort().Execute(store);

						using (var session = store.OpenSession())
						{
							session.Store(new Order { Currency = Currency.EUR, Product = "Milk", Total = 3 });
							session.Store(new Order { Currency = Currency.NIS, Product = "Milk", Total = 9 });
							session.Store(new Order { Currency = Currency.EUR, Product = "iPhone", Total = 3333 });
							session.SaveChanges();
						}
						WaitForIndexing(store);
						using (var session = store.OpenSession())
						{
							session.Query<Order, Orders_All_Without_Sort>()
								.AggregateBy(x => x.Product)
								.SumOn(x => x.Total)
								.ToList();
						}
					}
				});

			Assert.Equal("Index 'Orders/All/Without/Sort' does not have sorting enabled for a numerical field 'Total_Range'.", e.Message);
		}

		[Fact]
		public void ShouldNotThrowWhenAggregatingOnNumericalFieldWithSortingSetInIndex()
		{
			Assert.DoesNotThrow(
				() =>
				{
					using (var store = this.NewDocumentStore())
					{
						new Orders_All_With_Sort().Execute(store);

						using (var session = store.OpenSession())
						{
							session.Store(new Order { Currency = Currency.EUR, Product = "Milk", Total = 3 });
							session.Store(new Order { Currency = Currency.NIS, Product = "Milk", Total = 9 });
							session.Store(new Order { Currency = Currency.EUR, Product = "iPhone", Total = 3333 });
							session.SaveChanges();
						}
						WaitForIndexing(store);
						using (var session = store.OpenSession())
						{
							session.Query<Order, Orders_All_With_Sort>()
								.AggregateBy(x => x.Product)
								.SumOn(x => x.Total)
								.ToList();
						}
					}
				});
		}
	}
}