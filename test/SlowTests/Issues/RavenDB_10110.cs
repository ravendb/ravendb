using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10110 : RavenTestBase
    {
        public RavenDB_10110(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void InvalidOutputReduceToCollectionValidation()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new Invoices_CountByWarehouse());
                store.ExecuteIndex(new Invoices_CountByWarehouse2());
                
                using (var session = store.OpenSession())
                {
                    session.Store(new Warehouse()
                    {
                        Id = "warehouses/1",
                        WarehouseName = "ABC"
                    });

                    session.Store(new Invoice()
                    {
                        WarehouseId = "warehouses/1",
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var indexStats = store.Maintenance.Send(new GetIndexesStatisticsOperation());

                foreach (var stats in indexStats)
                {
                    Assert.Equal(0, stats.ErrorsCount);
                }
            }
        }
    }

    public class Invoice
    {
        public string Id { get; set; }
        public string WarehouseId { get; set; }
    }

    public class Warehouse
    {
        public string Id { get; set; }
        public string WarehouseName { get; set; }
    }

    public class Invoices_CountByWarehouse : AbstractIndexCreationTask<Invoice, Invoices_CountByWarehouse.Result>
    {
        public class Result
        {
            public string WarehouseName { get; set; }
            public int Count { get; set; }
        }

        public Invoices_CountByWarehouse()
        {
            Map = invoices => from invoice in invoices
                select new
                {
                    WarehouseName = LoadDocument<Warehouse>(invoice.WarehouseId).WarehouseName,
                    Count = 1
                };

            Reduce = results => from result in results
                group result by result.WarehouseName
                into g
                select new Result
                {
                    WarehouseName = g.Key,
                    Count = g.Sum(x => x.Count)
                };

            OutputReduceToCollection = "Results";
        }
    }

    public class Invoices_CountByWarehouse2 : AbstractIndexCreationTask<Invoice, Invoices_CountByWarehouse2.Result>
    {
        public class Result
        {
            public string WarehouseName { get; set; }
            public int Count { get; set; }
        }

        public Invoices_CountByWarehouse2()
        {
            Map = invoices => from invoice in invoices
                select new
                {
                    WarehouseName = LoadDocument<Warehouse>(invoice.WarehouseId).WarehouseName,
                    Count = 1
                };

            Reduce = results => from result in results
                group result by result.WarehouseName
                into g
                select new Result
                {
                    WarehouseName = g.Key,
                    Count = g.Sum(x => x.Count)
                };

            OutputReduceToCollection = "Results2";
        }
    }
}
