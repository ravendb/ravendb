using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Orders;
using Raven.Client.Documents;
using SlowTests.Smuggler;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();

            using (var store = new DocumentStore
            {
                Database = "Northwind",
                Url = "http://localhost:8080"
            }.Initialize())
            {
                using (var session = store.OpenSession())
                {
                    var q = 
                        from order in session.Query<Order>()
                        group order by order.Company
                        into g
                        select new
                        {
                            Company = g.Key,
                            TotalAmountPaid = g.Sum(o => o.Lines.Sum(ol => ol.Quantity * ol.PricePerUnit * (1 - ol.Discount))),
                            NumberOfOrders = g.Count()
                        };

                    foreach (var r in q.Where(x=>x.Company == "companies/1"))
                    {
                        Console.WriteLine(r);
                    }

                }
            }
        }
    }
}
