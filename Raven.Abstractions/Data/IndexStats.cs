//-----------------------------------------------------------------------
// <copyright file="IndexStats.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Data
{
	public class IndexStats
	{
		public string Name { get; set; }
		public int IndexingAttempts { get; set; }
		public int IndexingSuccesses { get; set; }
		public int IndexingErrors { get; set; }
		public Guid LastIndexedEtag { get; set; }
		public DateTime LastIndexedTimestamp { get; set; }
		public DateTime? LastQueryTimestamp { get; set; }
		public int TouchCount { get; set; }

		public int? ReduceIndexingAttempts { get; set; }
		public int? ReduceIndexingSuccesses { get; set; }
		public int? ReduceIndexingErrors { get; set; }
		public Guid? LastReducedEtag { get; set; }
		public DateTime? LastReducedTimestamp { get; set; }

		public IndexingPerformanceStats[] Performance { get; set; }

		public override string ToString()
		{
			return Name;
		}
	}

	public class IndexingPerformanceStats
	{
		public string Operation { get; set; }
		public int OutputCount { get; set; }
		public int InputCount { get; set; }
		public TimeSpan Duration { get; set; }
		public double DurationMilliseconds { get { return Math.Round(Duration.TotalMilliseconds, 2); } }
	}
}
