//-----------------------------------------------------------------------
// <copyright file="DatabaseStatistics.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Data
{
	public class DatabaseStatistics
	{
		public int CountOfIndexes { get; set; }

		public long ApproximateTaskCount { get; set; }

		public long CountOfDocuments { get; set; }

		public string[] StaleIndexes { get; set; }

		public IndexStats[] Indexes { get; set; }

		public ServerError[] Errors { get; set; }

		public TriggerInfo[] Triggers { get; set; }

		public object Extensions { get; set; }

		public class TriggerInfo
		{
			public string Type { get; set; }
			public string Name { get; set; }
		}
	}
}
