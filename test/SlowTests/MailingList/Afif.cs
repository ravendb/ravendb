using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Indexes;
using SlowTests.Utils;
using Xunit;

namespace SlowTests.MailingList
{
    public class WhenGroupinByLocation : RavenTestBase
    {
        [Fact(Skip = "Missing feature: Spatial")]
        public void CanFindSale()
        {
            using (var d = GetDocumentStore())
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

                    TestHelper.AssertNoIndexErrors(d);

                    Assert.NotEmpty(sitesales);
                }
            }
        }

        #region Nested type: Order

        private class Order
        {
            public string Id { get; set; }
            public string SaleId { get; set; }
        }

        #endregion

        #region Nested type: Sale

        private class Sale
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

        private class Sales_ByLocation : AbstractMultiMapIndexCreationTask<SiteSale>
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
                                          _ = locations.Select(l => SpatialGenerate(l.Lat, l.Lng)),
                                          // marking this as empty works
                                          sale.SaleId,
                                          Locations = locations,
                                          TotalSold = sales.Sum(x => x.TotalSold)
                                      };
            }
        }

        #endregion

        #region Nested type: SiteSale

        private class SiteSale
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
