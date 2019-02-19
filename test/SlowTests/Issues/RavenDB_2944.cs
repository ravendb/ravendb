// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2944.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_2944 : RavenTestBase
    {
        private const int MaxNumberOfItemsToProcessInTestIndexes = 256;

        //protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        //{
        //    configuration.Settings[Constants.MaxNumberOfItemsToProcessInTestIndexes] = MaxNumberOfItemsToProcessInTestIndexes.ToString(CultureInfo.InvariantCulture);
        //}

        private class Order
        {
            public string Id { get; set; }
            public string Company { get; set; }
            public string Employee { get; set; }
            public DateTime OrderedAt { get; set; }
            public DateTime RequireAt { get; set; }
            public DateTime? ShippedAt { get; set; }
            public string ShipVia { get; set; }
            public decimal Freight { get; set; }
        }

        private class Test_Orders_ByCompany : AbstractIndexCreationTask<Order>
        {
            public Test_Orders_ByCompany()
            {
                Map = orders => from order in orders select new { order.Company };
            }

            public override IndexDefinition CreateIndexDefinition()
            {
                var indexDefinition = base.CreateIndexDefinition();
#if FEATURE_TEST_INDEX
                indexDefinition.IsTestIndex = true;
#endif
                return indexDefinition;
            }
        }

        private class Test_Orders_Count : AbstractIndexCreationTask<Order, Test_Orders_Count.Result>
        {
            public class Result
            {
                public string Company { get; set; }

                public int Count { get; set; }
            }

            public override IndexDefinition CreateIndexDefinition()
            {
                var indexDefinition = base.CreateIndexDefinition();
#if FEATURE_TEST_INDEX
                indexDefinition.IsTestIndex = true;
#endif
                return indexDefinition;
            }

            public Test_Orders_Count()
            {
                Map = orders => from order in orders select new { order.Company, Count = 1 };

                Reduce = results => from result in results group result by result.Company into g select new { Company = g.Key, Count = g.Sum(x => x.Count) };
            }
        }

        [Fact(Skip = "RavenDB-6572")]
        public void CanCreateTestMapIndexes()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                WaitForIndexing(store);

                new Test_Orders_ByCompany().Execute(store);

                for (var i = 0; i < 100; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session
                            .Query<Order, Test_Orders_ByCompany>()
                            .Count();

                        if (count == MaxNumberOfItemsToProcessInTestIndexes)
                            return;
                    }

                    Thread.Sleep(100);
                }

                throw new InvalidOperationException("Should not happen.");
            }
        }

        [Fact(Skip = "RavenDB-6572")]
        public void CanCreateTestMapReduceIndexes()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                WaitForIndexing(store);

                new Test_Orders_Count().Execute(store);

                for (var i = 0; i < 100; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var results = session
                            .Query<Test_Orders_Count.Result, Test_Orders_Count>()
                            .ToList();

                        var count = results.Sum(x => x.Count);

                        if (count == MaxNumberOfItemsToProcessInTestIndexes)
                            return;
                    }

                    Thread.Sleep(100);
                }

                throw new InvalidOperationException("Should not happen.");
            }
        }
    }
}
