//-----------------------------------------------------------------------
// <copyright file="IDocumentStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Storage
{
	public interface IDocumentStorageActions
	{
		IEnumerable<JsonDocument> GetDocumentsByReverseUpdateOrder(int start, int take);
		IEnumerable<JsonDocument> GetDocumentsAfter(Etag etag, int take, long? maxSize = null, Etag untilEtag = null);
		IEnumerable<JsonDocument> GetDocumentsWithIdStartingWith(string idPrefix, int start, int take);

		long GetDocumentsCount();

		JsonDocument DocumentByKey(string key, TransactionInformation transactionInformation);
		JsonDocumentMetadata DocumentMetadataByKey(string key, TransactionInformation transactionInformation);

		bool DeleteDocument(string key, Etag etag, out RavenJObject metadata, out Etag deletedETag);
		AddDocumentResult AddDocument(string key, Etag etag, RavenJObject data, RavenJObject metadata);
		AddDocumentResult PutDocumentMetadata(string key, RavenJObject metadata);

		void IncrementDocumentCount(int value);
		AddDocumentResult InsertDocument(string key, RavenJObject data, RavenJObject metadata, bool checkForUpdates);

		void TouchDocument(string key, out Etag preTouchEtag, out Etag afterTouchEtag);
		Etag GetBestNextDocumentEtag(Etag etag);
	    DebugDocumentStats GetDocumentStatsVerySlowly();
	}

    public class DebugDocumentStats
    {
        public long Total { get; set; }
        public long Tombstones { get; set; }
        public long System { get; set; }
        public long NoCollection { get; set; }
        public Dictionary<string, long> Collections { get; set; }
        public TimeSpan TimeToGenerate { get; set; }

        public DebugDocumentStats()
        {
            Collections = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        }

        public void IncrementCollection(string name)
        {
            long value;
            Collections.TryGetValue(name, out value);
            Collections[name] = value + 1;
        }
    }

	public class AddDocumentResult
	{
		public Etag Etag;
		public Etag PrevEtag;
		public DateTime SavedAt;
		public bool Updated;
	}
}
