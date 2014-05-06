using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto.Faceted;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1107 : RavenTest
	{
		public class Orders_All : AbstractIndexCreationTask<Order>
		{
			public Orders_All()
			{
				Map = orders =>
					  from order in orders
					  select new { order.Currency, order.Product, order.Total, order.Quantity, order.Region, order.At, order.Tax };

				Sort(x => x.Total, SortOptions.Double);
				Sort(x => x.Quantity, SortOptions.Int);
				Sort(x => x.Region, SortOptions.Long);
				Sort(x => x.Tax, SortOptions.Float);
			}
		}

		[Fact]
		public void CanCalculateMultipleFieldsForOneAggregate()
		{
			using (var store = NewDocumentStore())
			{
				new Orders_All().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new Order { Currency = Currency.EUR, Product = "Milk", Total = 1000, Quantity = 1 });
					session.Store(new Order { Currency = Currency.NIS, Product = "Milk", Total = 2000, Quantity = 2 });
					session.Store(new Order
					{
						Currency = Currency.EUR,
						Product = "iPhone",
						Total = 3000,
						Quantity = 3
					});
					session.SaveChanges();
				}
				WaitForIndexing(store);

				var queries = new List<AggregationQuery>
                {
                    new AggregationQuery
                    {
                        Aggregation = FacetAggregation.Sum,
                        AggregationField = "Total",
                        DisplayName = "Product-Total",
                        Name = "Product",
                    },
                    new AggregationQuery
                    {
                        Aggregation = FacetAggregation.Sum,
                        AggregationField = "Quantity",
                        DisplayName = "Product-Quantity",
                        Name = "Product",
                    },
                };

				var result =
					store.AsyncDatabaseCommands.GetFacetsAsync("Orders/All", new IndexQuery(), AggregationQuery.GetFacets(queries),
															   start: 0, pageSize: 512).Result;

				Assert.Equal(6000, result.Results["Product-Total"].Values.Sum(s => s.Sum));
				Assert.Equal(6, result.Results["Product-Quantity"].Values.Sum(s => s.Sum));
			}
		}
	}
}
