// -----------------------------------------------------------------------
//  <copyright file="IndexCommitPoint.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.Indexing
{
	public class IndexCommitPoint
	{
		public Guid HighestCommitedETag { get; set; }

		public IndexSegmentsInfo SegmentsInfo { get; set; }

		public DateTime TimeStamp { get; set; }
	}
}