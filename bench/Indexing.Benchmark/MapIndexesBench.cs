using System;
using System.Diagnostics;
using System.Threading;
using Raven.Client;
using Raven.Client.Indexes;
#if v35
using Raven.Abstractions.Indexing;
#else
using Raven.Client.Indexing;
#endif

namespace Indexing.Benchmark
{
    public class MapIndexesBench : IndexingBenchmark
    {
        public MapIndexesBench(IDocumentStore store) : base(store)
        {
        }

        protected override AbstractIndexCreationTask[] Indexes => new []
        {
            new OrdersTotals(),
        };

        public class OrdersTotals : AbstractIndexCreationTask
        {
            public class Result
            {
                public string Employee { get; set; }

                public string Company { get; set; }

                public double Total { get; set; }
            }

            public override string IndexName
            {
                get
                {
                    return "Orders/Totals";
                }
            }

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    //                    Maps = { @"from order in docs.Orders
                    //select new { order.Employee,  order.Company, Total = order.Lines.Sum(l=>(l.Quantity * l.PricePerUnit) *  ( 1 - l.Discount)) }" },
                    Maps = { @"from order in docs.Orders
select new { order.Employee,  order.Company, Total = order.Lines.Sum(l =>l.PricePerUnit) }" },
                    //Fields =
                    //{
                    //    {
                    //        "Total", new IndexFieldOptions()
                    //        {
                    //            Sort = SortOptions.NumericDouble
                    //        }
                    //    }
                    //}
                };
            }
        }
    }
}