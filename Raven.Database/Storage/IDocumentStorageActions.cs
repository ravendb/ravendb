//-----------------------------------------------------------------------
// <copyright file="IDocumentStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using metrics.Core;
using System.Linq;
using Raven.Abstractions.Extensions;
using Raven.Database.Extensions;

namespace Raven.Database.Storage
{
    using System.Threading;

    public interface IDocumentStorageActions 
    {
        IEnumerable<JsonDocument> GetDocumentsByReverseUpdateOrder(int start, int take);
        IEnumerable<JsonDocument> GetDocumentsAfter(
            Etag etag, int take, 
            CancellationToken cancellationToken, 
            long? maxSize = null, 
            Etag untilEtag = null, 
            TimeSpan? timeout = null, 
            Action<Etag> lastProcessedDocument = null,
            Reference<bool> earlyExit = null);
        IEnumerable<JsonDocument> GetDocumentsAfterWithIdStartingWith(Etag etag, string idPrefix, int take, CancellationToken cancellationToken, long? maxSize = null, Etag untilEtag = null, TimeSpan? timeout = null, Action<Etag> lastProcessedDocument = null,
            Reference<bool> earlyExit = null);
        IEnumerable<JsonDocument> GetDocumentsWithIdStartingWith(string idPrefix, int start, int take, string skipAfter);
        Etag GetEtagAfterSkip(Etag etag, int skip, CancellationToken cancellationToken, out int skipped);
        IEnumerable<string> GetDocumentIdsAfterEtag(Etag etag, int maxTake,
            Func<string, RavenJObject, Func<JsonDocument>, bool> filterDocument, 
            Reference<bool> earlyExit, CancellationToken cancellationToken);

        IEnumerable<JsonDocument> GetDocuments(int start);

        long GetDocumentsCount();

        JsonDocument DocumentByKey(string key);

        Stream RawDocumentByKey(string key);

        JsonDocumentMetadata DocumentMetadataByKey(string key);

        bool DeleteDocument(string key, Etag etag, out RavenJObject metadata, out Etag deletedETag);
        AddDocumentResult AddDocument(string key, Etag etag, RavenJObject data, RavenJObject metadata);

        void IncrementDocumentCount(int value);
        AddDocumentResult InsertDocument(string key, RavenJObject data, RavenJObject metadata, bool overwriteExisting);

        void TouchDocument(string key, out Etag preTouchEtag, out Etag afterTouchEtag);
        Etag GetBestNextDocumentEtag(Etag etag);
        DebugDocumentStats GetDocumentStatsVerySlowly(Action<string> progress, CancellationToken token);
    }

    public class CollectionDetails
    {
        public long TotalSize { get; private set; }

        public const int TopDocsLimit = 10;

        [JsonIgnore] 
        private readonly HistogramMetric _stats;
        public HistogramData Stats { get { return _stats.CreateHistogramData(); } }

        [JsonIgnore]
        private readonly SortedList<int, string> _topDocs;

        public List<DocumentAndSize> TopDocs { get
        {
            return _topDocs.Select(x => new DocumentAndSize
            {
                DocId = x.Value,
                Size = x.Key
            }).ToList();
        }}

        public CollectionDetails()
        {
            _stats = new HistogramMetric(HistogramMetric.SampleType.Uniform);
            _topDocs = new SortedList<int, string>(new IntDescComparer());
        }

        public void Update(int documentSize, string docId)
        {
            TotalSize += documentSize;
            _stats.Update(documentSize);
            _topDocs[documentSize] = docId;
            if (_topDocs.Count > TopDocsLimit)
            {
                _topDocs.RemoveAt(TopDocsLimit);
            }
        }
    }

    internal class IntDescComparer : IComparer<int>
    {
        public int Compare(int x, int y)
        {
            return y.CompareTo(x);
        }
    }

    public class DocumentAndSize
    {
        public string DocId { get; set; }
        public int Size { get; set; }
    }

    public class DebugDocumentStatsState : OperationStateBase
    {
        public DebugDocumentStats Stats { get; set; }
    }

    public class DebugDocumentStats
    {
        public long Total { get; set; }
        public long TotalSize { get; set; }
        public long Tombstones { get; set; }

        public CollectionDetails System { get; set; }
        public CollectionDetails NoCollection { get; set; }

        [JsonIgnore]
        private Dictionary<string, CollectionDetails> _collections { get; set; }

        public Dictionary<string, CollectionDetails> Collections
        {
            get { return _collections.OrderByDescending(x => x.Value.TotalSize).ToDictionary(x => x.Key, x => x.Value); }
        }

        //public Dictionary<string, HistogramData> 
        
        public TimeSpan TimeToGenerate { get; set; }

        public DebugDocumentStats()
        {
            _collections = new Dictionary<string, CollectionDetails>(StringComparer.OrdinalIgnoreCase);
            System = new CollectionDetails();
            NoCollection = new CollectionDetails();
        }

        public void IncrementCollection(string name, int size, string docId)
        {
            CollectionDetails value;
            if (_collections.TryGetValue(name, out value) == false)
            {
                _collections[name] = value = new CollectionDetails();
            }
            value.Update(size, docId);
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
