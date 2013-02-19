// -----------------------------------------------------------------------
//  <copyright file="WritingDocumentsInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;

namespace Raven.Database.Indexing
{
	public class IndexedItemsInfo
	{
		public int ChangedDocs { get; set; }

		public Etag HighestETag { get; set; }

		public string[] DeletedKeys { get; set; }
	}
}