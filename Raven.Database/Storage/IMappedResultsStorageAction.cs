//-----------------------------------------------------------------------
// <copyright file="IMappedResultsStorageAction.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Json.Linq;

namespace Raven.Database.Storage
{
	public interface IMappedResultsStorageAction
	{
		void PutMappedResult(string view, string docId, string reduceKey, RavenJObject data, byte[] viewAndReduceKeyHashed);
		IEnumerable<RavenJObject> GetMappedResults(params GetMappedResultsParams[] getMappedResultsParams);
		IEnumerable<string> DeleteMappedResultsForDocumentId(string documentId, string view);
		void DeleteMappedResultsForView(string view);
		IEnumerable<MappedResultInfo> GetMappedResultsReduceKeysAfter(string indexName, Guid lastReducedEtag, bool loadData, int take);
	}

	public class MappedResultInfo
	{
		public string ReduceKey { get; set; }
		public DateTime Timestamp { get; set; }
		public Guid Etag { get; set; }

		public RavenJObject Data { get; set; }
		public int Size { get; set; }
	}
}
