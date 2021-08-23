using Corax;
using Lucene.Net.Search;
using Lucene.Net.Store;

namespace Raven.Server.Documents.Queries.Results
{
    public interface IQueryResultRetriever
    {
        Document Get(ref RetrieverInput retrieverInput);

        bool TryGetKey(ref RetrieverInput retrieverInput, out string key);
    }

    public ref struct RetrieverInput
    {
        private readonly IndexEntryReader _coraxEntry;
        private readonly Lucene.Net.Documents.Document _luceneDocument;
        private readonly IState _state;
        public IndexEntryReader CoraxEntry => _coraxEntry;
        public IState State => _state;  
        public Lucene.Net.Documents.Document LuceneDocument => _luceneDocument;

        public string DocumentId { get; set; }

        public ScoreDoc Score
        {
            get;
            set;
        }


        public RetrieverInput(Lucene.Net.Documents.Document luceneDocument, ScoreDoc score, IState state)
        {
            _luceneDocument = luceneDocument;
            _coraxEntry = default;
            _state = state;
            Score = score;
            DocumentId = string.Empty;
        }


        public RetrieverInput(IndexEntryReader coraxEntry)
        {
            _coraxEntry = coraxEntry;
            _luceneDocument = null;
            _state = null;
            Score = null;
            DocumentId = string.Empty;
        }
    }
}
