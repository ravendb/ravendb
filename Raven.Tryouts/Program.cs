using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Smuggler;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Powershell;
using Raven.Smuggler;
using Raven.Tests.Bugs;
using Raven.Tests.Common;
using Raven.Tests.FileSystem;
using Raven.Tests.Raft.Client;
using Raven.Tests.Smuggler;
using Raven.Tests.Subscriptions;
using Xunit;
using Order = Raven.Tests.Common.Dto.Faceted.Order;
using Raven.Tests.Raft;
using Raven.Tests.Faceted;
using Raven.Abstractions.Replication;
using Raven.Tests.Bundles.LiveTest;
#if !DNXCORE50
using Raven.Tests.Sorting;
using Raven.SlowTests.RavenThreadPool;
using Raven.Tests.Core;
using Raven.Tests.Core.Commands;
using Raven.Tests.Issues;
using Raven.Tests.MailingList;
using Raven.Tests.FileSystem.ClientApi;
#endif

namespace Raven.Tryouts
{
    public class Order
    {
        public string Id { get; set; }
        public string Company { get; set; }
        public string Employee { get; set; }
        public DateTime OrderedAt { get; set; }
        public DateTime RequireAt { get; set; }
        public DateTime? ShippedAt { get; set; }
        public Address ShipTo { get; set; }
        public string ShipVia { get; set; }
        public decimal Freight { get; set; }
        public List<OrderLine> Lines { get; set; }
    }

    public class Address
    {
        public string Line1 { get; set; }
        public string Line2 { get; set; }
        public string City { get; set; }
        public string Region { get; set; }
        public string PostalCode { get; set; }
        public string Country { get; set; }
    }

    public class OrderLine
    {
        public string Product { get; set; }
        public string ProductName { get; set; }
        public decimal PricePerUnit { get; set; }
        public int Quantity { get; set; }
        public decimal Discount { get; set; }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            for (var i = 0; i < 1000; i++)
            {
                try
                {
                    Console.WriteLine(i);
                    using (var test = new RavenDB_3109())
                    {
                        test.ShouldWork();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    Console.Read();
                }
            }


        }

        private static void InitDBAndDoSomeWork(int i)
        {
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8080",
                DefaultDatabase = "Northwind"
            })
            {
                store.Initialize();
                DoSomeWork(store, i);
                DoSomeWork(store, i);
                DoSomeWork(store, i);
            }
        }

        private static void DoSomeWork(DocumentStore store, int i)
        {
            for (int k = 0; k < 10; k++)
            {
                using (var session = store.OpenSession())
                {
                    for (int j = 1; j < 30; j++)
                    {
                        var order = session.Load<Order>("orders/" + j);
                        order.Freight = i;
                    }

                    session.SaveChanges();
                }
            }
        }

        public static async Task AsyncMain()
        {


            var sp = Stopwatch.StartNew();
            try
            {
                var smugglerApi = new SmugglerFilesApi();
                await smugglerApi.ImportData(importOptions: new SmugglerImportOptions<FilesConnectionStringOptions>()
                {
                    To = new FilesConnectionStringOptions()
                    {
                        DefaultFileSystem = "FS2",
                        Url = "http://localhost:8080",

                    },
                    FromFile = "c:\\Temp\\export.ravendump",
                });
            }
            catch (Exception ex)
            {

                Console.WriteLine(ex);

                Console.WriteLine(ex.StackTrace);
            }

            Console.ReadLine();

            Console.WriteLine(sp.ElapsedMilliseconds);


        }
    }
}
