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

		public int ReduceIndexingAttempts { get; set; }
		public int ReduceIndexingSuccesses { get; set; }
		public int ReduceIndexingErrors { get; set; }


		public Guid LastIndexedEtag { get; set; }
		public DateTime LastIndexedTimestamp { get; set; }
	}
}
