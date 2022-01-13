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

                Assert.True(WaitForValue(() =>
                {
                    var indexingPerformanceStatistics = store.Maintenance.Send(new GetIndexPerformanceStatisticsOperation());

                    if (indexingPerformanceStatistics.Length != 7)
                        return false;

                    foreach (var stats in indexingPerformanceStatistics)
                    {
                        if (stats.Name == null || stats.Performance == null)
                            return false;

                        if (stats.Performance.Length == 0)
                            return false;
                    }

                    return true;
                    
                }, true, 5000));
            }
        }
    }
}
