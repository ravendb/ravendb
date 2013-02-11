// -----------------------------------------------------------------------
//  <copyright file="WritingDocumentsInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.Indexing
{
	public class WritingDocumentsInfo
	{
		public int ChangedDocs { get; set; }

		public Guid? HighestETag { get; set; }

		public bool ShouldStoreCommitPoint { get; set; }

		public static WritingDocumentsInfo WithoutCommitPoint(int changedDocs)
		{
			return new WritingDocumentsInfo
			{
				ChangedDocs = changedDocs,
				HighestETag = null,
				ShouldStoreCommitPoint = false
			};
		}
	}
}