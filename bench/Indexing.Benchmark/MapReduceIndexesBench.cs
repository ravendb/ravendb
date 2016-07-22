using Raven.Client;
using Raven.Client.Indexes;
#if v35
using Raven.Abstractions.Indexing;
#else
using Raven.Client.Indexing;
#endif

namespace Indexing.Benchmark
{
    public class MapReduceIndexesBench : IndexingBenchmark
    {
        public MapReduceIndexesBench(IDocumentStore store) : base(store)
        {
        }

        protected override AbstractIndexCreationTask[] Indexes => new[]
        {
            new Orders_ByCompany(),
        };

        public class Orders_ByCompany : AbstractIndexCreationTask
        {
            public class Result
            {
                public string Company { get; set; }

                public int Count { get; set; }

                public double Total { get; set; }
            }

            public override string IndexName
            {
                get { return "Orders/ByCompany"; }
            }

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = {@"from order in docs.Orders
                            from line in order.Lines
                            select
                            new
                            {
                                order.Company,
                                Count = 1,
                                Total = line.PricePerUnit
                            }"},
                    Reduce = @"from result in results
group result by result.Company into g
select new
{
    Company = g.Key,
    Count = g.Sum(x=> x.Count),
    Total = g.Sum(x=> x.Total)
}"
                };
            }
        }

        //public class Orders_ByCompany : AbstractIndexCreationTask<Order, Orders_ByCompany.Result>
        //{
        //    public class Result
        //    {
        //        public string Company { get; set; }

        //        public int Count { get; set; }

        //        public double Total { get; set; }
        //    }

        //    public Orders_ByCompany()
        //    {
        //        // currently we don't have 
        //        //Map = orders => from order in orders
        //        //                select
        //        //                new
        //        //                {
        //        //                    order.Company,
        //        //                    Count = 1,
        //        //                    Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))
        //        //                };

        //        Map = orders => from order in orders
        //                        from line in order.Lines
        //                        select
        //                        new
        //                        {
        //                            order.Company,
        //                            Count = 1,
        //                            Total = line.PricePerUnit
        //                        };

        //        Reduce = results => from result in results
        //                            group result by result.Company
        //            into g
        //                            select new
        //                            {
        //                                Company = g.Key,
        //                                Count = g.Sum(x => x.Count),
        //                                Total = g.Sum(x => x.Total)
        //                            };
        //    }
        //}
    }
}