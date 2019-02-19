using System.Collections.Generic;
using FastTests;
using System.Linq;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10636 : RavenTestBase
    {
        class Purchase
        {
            public string Id { get; set; }
            public int TotalQuantity { get; set; }
            public Dictionary<string, int> PurchasedQuantityByUser { get; set; }
              = new Dictionary<string, int>();
        }

        [Fact]
        public void CanProjectOnSelectOnDictionary()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Purchase
                    {
                        TotalQuantity = 5,
                        PurchasedQuantityByUser =
                        {
                            ["users/1"] = 1,
                            ["users/2"] = 2,
                            ["users/3"] = 2
                        }
                    }, "purchases/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = from x in session.Query<Purchase>()
                                  where x.Id == "purchases/1"
                                  let purchasedQuantities = x.PurchasedQuantityByUser
                                  select new
                                  {
                                      Quantity = x.TotalQuantity,
                                      Quantities = purchasedQuantities.Select(a => a.Value),
                                      QuantityTotal = purchasedQuantities.Sum(a => a.Value)
                                  };
                    var item = results.SingleOrDefault();

                    //WaitForUserToContinueTheTest(store);

                    Assert.Equal(5, item.Quantity);
                    Assert.Equal(new[] { 1, 2, 2 }, item.Quantities);
                    Assert.Equal(5, item.QuantityTotal);
                }
            }
        }
    }
}
