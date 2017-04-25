// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3417.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3417 : RavenTestBase
    {
        [Fact]
        public void GetIndexingPerformanceStatisticsShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new CreateSampleDataOperation());

                WaitForIndexing(store);

                var indexingPerformanceStatistics = store.Admin.Send(new GetIndexPerformanceStatisticsOperation());

                Assert.Equal(3, indexingPerformanceStatistics.Length);

                foreach (var stats in indexingPerformanceStatistics)
                {
                    Assert.True(stats.Etag > 0);
                    Assert.NotNull(stats.Name);
                    Assert.NotNull(stats.Performance);
                    Assert.True(stats.Performance.Length > 0);
                }
            }
        }
    }
}
