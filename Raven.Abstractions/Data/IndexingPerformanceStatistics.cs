// -----------------------------------------------------------------------
//  <copyright file="IndexingPerformanceStatistics.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Abstractions.Data
{
	public class IndexingPerformanceStatistics
	{
		public IndexingPerformanceStatistics()
		{
			Performance = new IndexingPerformanceStats[0];
		}

		public int IndexId { get; set; }

		public string IndexName { get; set; }

		public IndexingPerformanceStats[] Performance { get; set; }
	}
}