// -----------------------------------------------------------------------
//  <copyright file="PrecomputedIndexingBatch.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Database.Indexing
{
	public class PrecomputedIndexingBatch
	{
		private Etag maxLastIndexedETag;
		private DateTime maxLastModified;
		public List<JsonDocument> Documents { get; private set; }

		public Etag LastIndexedETag
		{
			get { return Documents.Count == 0 ? maxLastIndexedETag : Etag.Empty; }
		}

		public DateTime LastModified
		{
			get { return Documents.Count == 0 ? maxLastModified : DateTime.MinValue; }
		} 

		public void Set(Etag lastIndexedEtag, DateTime lastModified, List<JsonDocument> documents)
		{
			maxLastIndexedETag = lastIndexedEtag;
			maxLastModified = lastModified;
			Documents = documents;
		}

		public List<JsonDocument> RemoveAndReturnDocuments(int docsToTake)
		{
			var take = Math.Min(docsToTake, Documents.Count);

			var result = Documents.GetRange(0, take);
			Documents.RemoveRange(0, take);

			return result;
		}
	}
}