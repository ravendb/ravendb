using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using FastTests;
using Nest;
using Orders;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19076 : RavenTestBase
{
    public RavenDB_19076(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void ExplorationQueryShouldReturnSameAmountOfResultsAsLuceneWhenFilterOnPrimitiveTypes()
    {
        using var store = GetDocumentStore();
        store.Maintenance.Send(new CreateSampleDataOperation());
        {
            using var session = store.OpenSession();

            var explorationQueryResults = GetResultPaging("from 'Orders' order by OrderedAt filter ShippedAt != null");
            var indexQueryResults = GetResultPaging("from 'Orders' where ShippedAt != null order by OrderedAt");
            for (int index = 0; index < indexQueryResults.Count; index++)
            {
                Order indexItem = indexQueryResults[index];
                Assert.Equal(indexItem.Id, explorationQueryResults[index].Id);
            }

            Assert.Equal(indexQueryResults.Count, explorationQueryResults.Count);
            List<Order> GetResultPaging(string query)
            {
                var pagging = new List<Order>();
                int pageNumber = 0;
                int pageSize = 101;
                int skippedResults = 0;
                List<Order> results;
                do
                {
                    results = session.Advanced.RawQuery<Orders.Order>(query)
                        .Statistics(out QueryStatistics stats)
                        .Skip((pageNumber * pageSize) + skippedResults)
                        .Take(pageSize)
                        .ToList();

                    pagging.AddRange(results);
                    skippedResults += stats.SkippedResults;
                    pageNumber++;
                } while (results.Count > 0);

                return pagging;
            }
        }
    }
}
