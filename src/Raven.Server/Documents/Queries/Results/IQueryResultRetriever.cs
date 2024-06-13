using System;
using System.Collections.Generic;
using System.Threading;
using Corax.Querying;
using Corax.Mappings;
using Corax.Utils;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Sparrow;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Queries.Results
{
    public interface IQueryResultRetriever
    {
        (Document Document, List<Document> List) Get(ref RetrieverInput retrieverInput, CancellationToken token);

        bool TryGetKeyLucene(ref RetrieverInput retrieverInput, out string key);

        bool TryGetKeyCorax(TermsReader searcher, long id, out UnmanagedSpan key);

        Document DirectGet(ref RetrieverInput retrieverInput, string id, DocumentFields fields);

    }

    public struct RetrieverInput
    {
        public bool IsLuceneDocument() => LuceneDocument != null;

        public IndexFieldsMapping KnownFields;
        
        public EntryTermsReader CoraxTermsReader;

        public IState State;

        public Lucene.Net.Documents.Document LuceneDocument;

        public string DocumentId;

        public ScoreDoc Score;

        public float? CoraxScore;
        
        public Corax.Utils.Spatial.SpatialResult? CoraxDistance;

        public IndexSearcher CoraxIndexSearcher;
        public Func<string, bool> HasTime;

        public RetrieverInput(Lucene.Net.Documents.Document luceneDocument, ScoreDoc score, IState state)
        {
            LuceneDocument = luceneDocument;
            State = state;
            Score = score;
            
            KnownFields = null;
            CoraxTermsReader = default;
            CoraxIndexSearcher = null;
            HasTime = null;
        }

        public RetrieverInput(IndexSearcher searcher, IndexFieldsMapping knownFields, in EntryTermsReader reader, string id, Func<string, bool> hasTime, float? score = null, Corax.Utils.Spatial.SpatialResult? distance = null)
        {
            CoraxTermsReader = reader;
            KnownFields = knownFields;
            DocumentId = id;
            CoraxIndexSearcher = searcher;
            CoraxScore = score;
            CoraxDistance = distance;
            HasTime = hasTime;
            
            State = null;
            Score = null;
            LuceneDocument = null;
        }
    }
}
