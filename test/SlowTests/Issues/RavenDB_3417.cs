// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3417.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using FastTests;
using Raven.Client.Documents.Indexes;
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
                store.Maintenance.Send(new CreateSampleDataOperation(Raven.Client.Documents.Smuggler.DatabaseItemType.Documents |
                                                                     Raven.Client.Documents.Smuggler.DatabaseItemType.Indexes));

                Indexes.WaitForIndexing(store);

                var str = string.Empty;
                Assert.True(WaitForValue(() =>
                {
                    var indexingPerformanceStatistics = store.Maintenance.Send(new GetIndexPerformanceStatisticsOperation());

                    if (indexingPerformanceStatistics.Length != 7)
                        str = $"Expected {indexingPerformanceStatistics.Length} length to be 7";

                    foreach (var stats in indexingPerformanceStatistics)
                    {
                        if (stats.Name == null)
                            str = $"{nameof(indexingPerformanceStatistics.Length)} is null";

                        if (stats.Performance == null)
                            str = $"{nameof(stats.Performance)} is null";

                        if (stats.Performance?.Length == 0)
                            str = $"{nameof(stats.Performance.Length)} is 0";
                    }

                    return true;
                }, true, 5000), str);
            }
        }
    }
}
