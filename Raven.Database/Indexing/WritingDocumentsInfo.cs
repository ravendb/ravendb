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
		private WritingDocumentsInfo()
		{
		}

		public int ChangedDocs { get; private set; }

		public Guid? HighestETag { get; private set; }

		public bool ShouldStoreCommitPoint { get; private set; }

		public static WritingDocumentsInfo Empty()
		{
			return new WritingDocumentsInfo
			{
				ChangedDocs = 0,
				HighestETag = null,
				ShouldStoreCommitPoint = false
			};
		}

		public static WritingDocumentsInfo ChangedDocsOnly(int changedDocs)
		{
			return new WritingDocumentsInfo
			{
				ChangedDocs = changedDocs,
				HighestETag = null,
				ShouldStoreCommitPoint = false
			};
		}

		public static WritingDocumentsInfo StoredCommitPoint(int changedDocs, Guid highestCommitedETag)
		{
			return new WritingDocumentsInfo
			{
				ChangedDocs = changedDocs,
				HighestETag = highestCommitedETag,
				ShouldStoreCommitPoint = true
			};
		}
	}
}