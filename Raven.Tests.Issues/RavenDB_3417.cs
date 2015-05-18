// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3417.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3417 : RavenTest
	{
		[Fact]
		public void GetIndexingPerformanceStatisticsShouldWork()
		{
			using (var store = NewRemoteDocumentStore())
			{
				DeployNorthwind(store);

				WaitForIndexing(store);

				var indexingPerformanceStatistics = store.DatabaseCommands.GetIndexingPerformanceStatistics();

				Assert.Equal(4, indexingPerformanceStatistics.Length);

				foreach (var stats in indexingPerformanceStatistics)
				{
					Assert.True(stats.IndexId > 0);
					Assert.NotNull(stats.IndexName);
					Assert.NotNull(stats.Performance);
					Assert.True(stats.Performance.Length > 0);
				}
			}
		}
	}
}