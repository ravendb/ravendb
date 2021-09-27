using System.Collections.Generic;
using Corax;
using Lucene.Net.Search;
using Lucene.Net.Store;

namespace Raven.Server.Documents.Queries.Results
{
    public interface IQueryResultRetriever
    {
        (Document Document, List<Document> List) Get(ref RetrieverInput retrieverInput);

        bool TryGetKey(ref RetrieverInput retrieverInput, out string key);
    }

    public ref struct RetrieverInput
    {
        public IndexEntryReader CoraxEntry;

        public IState State;
        
        public Lucene.Net.Documents.Document LuceneDocument;

        public string DocumentId;

        public ScoreDoc Score;

        public RetrieverInput(Lucene.Net.Documents.Document luceneDocument, ScoreDoc score, IState state)
        {
            LuceneDocument = luceneDocument;
            CoraxEntry = default;
            State = state;
            Score = score;
            DocumentId = string.Empty;
        }

        public RetrieverInput(IndexEntryReader coraxEntry, string id)
        {
            CoraxEntry = coraxEntry;
            LuceneDocument = null;
            State = null;
            Score = null;
            DocumentId = id;
        }
    }
}
