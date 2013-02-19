// -----------------------------------------------------------------------
//  <copyright file="WritingDocumentsInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.Indexing
{
	public class IndexedItemsInfo
	{
		public int ChangedDocs { get; set; }

		public Guid? HighestETag { get; set; }

		public string[] DeletedKeys { get; set; }
	}
}