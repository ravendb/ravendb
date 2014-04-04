using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class WhenGroupinByLocation : RavenTest
	{
		[Fact]
		public void CanFindSale()
		{
			using (var d = NewDocumentStore())
			{
				new Sales_ByLocation().Execute(d);
				using (var s = d.OpenSession())
				{
					var sale = new Sale
					{
						Locations =
							new[] { new Sale.Location { Lat = 37.780, Lng = 144.960 }, new Sale.Location { Lat = 37.790, Lng = 144.960 }, },
						Quantity = 10,
						Title = "raven master class"
					};
					s.Store(sale);
					s.Store(new Order { SaleId = sale.Id });
					s.Store(new Order { SaleId = sale.Id });
					s.SaveChanges();

					List<SiteSale> sitesales = s.Query<SiteSale, Sales_ByLocation>().Customize(x => x.WaitForNonStaleResults())
						.ToList();

					Assert.Empty(d.DocumentDatabase.Statistics.Errors);

					Assert.NotEmpty(sitesales);
				}
			}
		}

		#region Nested type: Order

		[Serializable]
		public class Order
		{
			public string Id { get; set; }
			public string SaleId { get; set; }
		}

		#endregion

		#region Nested type: Sale

		[Serializable]
		public class Sale
		{
			public string Id { get; set; }
			public string Title { get; set; }
			public Location[] Locations { get; set; }
			public int Quantity { get; set; }

			#region Nested type: Location

			public class Location
			{
				public double Lat { get; set; }
				public double Lng { get; set; }
			}

			#endregion
		}

		#endregion

		#region Nested type: Sales_ByLocation

		public class Sales_ByLocation : AbstractMultiMapIndexCreationTask<SiteSale>
		{
			public Sales_ByLocation()
			{
				AddMap<Sale>(sales => from sale in sales
									  select new
									  {
										  _ = (object)null,
										  SaleId = sale.Id,
										  Locations = sale.Locations.Select(l => new { l.Lat, l.Lng }).ToArray(),
										  TotalSold = 0
									  });

				AddMap<Order>(orders => from order in orders
										select new
										{
											_ = (object)null,
											order.SaleId,
											Locations = new[] { new { Lat = (double)0, Lng = (double)0 } },
											TotalSold = 1
										});

				Reduce = sitesales => from sitesale in sitesales
									  group sitesale by sitesale.SaleId
										  into sales
										  let locations = sales.SelectMany(x => x.Locations)
										  from sale in sales
										  select new
										  {
											  _ = locations.Select(l => SpatialIndex.Generate(l.Lat, l.Lng)),
											  // marking this as empty works
											  sale.SaleId,
											  Locations = locations,
											  TotalSold = sales.Sum(x => x.TotalSold)
										  };
			}
		}

		#endregion

		#region Nested type: SiteSale

		[Serializable]
		public class SiteSale
		{
			public string SaleId { get; set; }
			public Location[] Locations { get; set; }
			public int TotalSold { get; set; }

			#region Nested type: Location

			public class Location
			{
				public double Lat { get; set; }
				public double Lng { get; set; }
			}

			#endregion
		}

		#endregion
	}
}