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
        private readonly int _numberOfOrdersInDb;

        public MapReduceIndexesBench(IDocumentStore store, int numberOfOrdersInDb) : base(store)
        {
            _numberOfOrdersInDb = numberOfOrdersInDb;
        }

        public override IndexingTestRun[] IndexTestRuns => new []
        {
            new IndexingTestRun
            {
              Index = new Orders_ByCompany(),
              NumberOfRelevantDocs = _numberOfOrdersInDb
            },
            new IndexingTestRun
            {
              Index = new Orders_ByCompany_Fanout(),
              NumberOfRelevantDocs = _numberOfOrdersInDb
            }
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
                    Maps = { @"from order in docs.Orders
                            select
                            new
                            {
                                order.Company,
                                Count = 1,
                                Total = order.Lines.Sum(line => line.PricePerUnit)
                            }" },
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

        public class Orders_ByCompany_Fanout : AbstractIndexCreationTask
        {
            public class Result
            {
                public string Company { get; set; }

                public int Count { get; set; }

                public double Total { get; set; }
            }

            public override string IndexName
            {
                get { return "Orders/ByCompany_Fanout"; }
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
    }
}