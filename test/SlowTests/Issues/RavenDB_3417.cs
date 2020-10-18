// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3417.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3417 : RavenTestBase
    {
        public RavenDB_3417(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void GetIndexingPerformanceStatisticsShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation(Raven.Client.Documents.Smuggler.DatabaseItemType.Documents | Raven.Client.Documents.Smuggler.DatabaseItemType.Indexes));

                WaitForIndexing(store);

                var indexingPerformanceStatistics = store.Maintenance.Send(new GetIndexPerformanceStatisticsOperation());

                Assert.Equal(7, indexingPerformanceStatistics.Length);

                foreach (var stats in indexingPerformanceStatistics)
                {
                    Assert.NotNull(stats.Name);
                    Assert.NotNull(stats.Performance);
                    Assert.True(stats.Performance.Length > 0);
                }
            }
        }
    }
}
