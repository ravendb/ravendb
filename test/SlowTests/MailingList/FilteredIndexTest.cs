using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class FilterIndexTest : RavenTestBase
    {
        private class Appointment
        {
            public string Id { get; set; }
            public List<string> ProductIds { get; set; }
        }

        private class Appointments_Index : AbstractIndexCreationTask<Appointment>
        {
            public Appointments_Index()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  doc.ProductIds
                              };
            }
        }

        [Fact]
        public void CanFilterByProductId()
        {
            using (var store = GetDocumentStore())
            {
                var productIds = new string[] { "products/1", "products/2" };
                var productIds2 = new string[] { "products/3", "products/4" };

                using (var session = store.OpenSession())
                {
                    // appointment 1 with products/1 + products/2
                    var appointment = new Appointment()
                    {
                        Id = "appointments/1",
                        ProductIds = new List<string>(productIds)
                    };
                    session.Store(appointment);

                    // appointment 1 with products/3 + products/4
                    var appointment2 = new Appointment()
                    {
                        Id = "appointments/2",
                        ProductIds = new List<string>(productIds2)
                    };
                    session.Store(appointment2);

                    session.SaveChanges();
                }

                new Appointments_Index().Execute(store);

                using (var session = store.OpenSession())
                {
                    // only include appointments matching these products
                    IList<string> filterProductIds = new List<string>();
                    filterProductIds.Add("products/2");

                    var results = session.Query<Appointment, Appointments_Index>()
                                         .Customize(customization => customization.WaitForNonStaleResults())
                                         .Where(x => x.ProductIds.In(filterProductIds))
                                         .OfType<Appointment>()
                                         .ToList();

                    Assert.Equal(1, results.Count);

                    foreach (var result in results)
                    {
                        Assert.True(result.ProductIds.Contains("products/2"));
                    }
                }
            }
        }
    }
}