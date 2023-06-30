using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax;
using Corax.Mappings;
using Corax.Utils;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Server.Documents.Indexes;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Sparrow;
using Sparrow.Server;

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

        public Corax.IndexSearcher CoraxIndexSearcher;

        public RetrieverInput(Lucene.Net.Documents.Document luceneDocument, ScoreDoc score, IState state)
        {
            LuceneDocument = luceneDocument;
            State = state;
            Score = score;
            
            KnownFields = null;
            CoraxTermsReader = default;
            CoraxIndexSearcher = null;
        }

        public RetrieverInput(Corax.IndexSearcher searcher, IndexFieldsMapping knownFields, EntryTermsReader reader, string id, float? score = null)
        {
            CoraxTermsReader = reader;
            KnownFields = knownFields;
            DocumentId = id;
            CoraxIndexSearcher = searcher;
            CoraxScore = score;

            State = null;
            Score = null;
            LuceneDocument = null;
        }
    }
}
