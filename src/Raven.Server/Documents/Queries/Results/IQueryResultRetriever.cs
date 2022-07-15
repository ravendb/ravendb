using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax;
using Corax.Fields;
using Corax.IndexEntry;
using Lucene.Net.Search;
using Lucene.Net.Store;

namespace Raven.Server.Documents.Queries.Results
{
    public interface IQueryResultRetriever
    {
        (Document Document, List<Document> List) Get(ref RetrieverInput retrieverInput, CancellationToken token);

        bool TryGetKey(ref RetrieverInput retrieverInput, out string key);

        Document DirectGet(ref RetrieverInput retrieverInput, string id, DocumentFields fields);

    }

    public ref struct RetrieverInput
    {
        public IndexFieldsMapping KnownFields;
        
        public IndexEntryReader CoraxEntry;

        public IState State;

        public Lucene.Net.Documents.Document LuceneDocument;

        public string DocumentId;

        public ScoreDoc Score;

        public RetrieverInput(Lucene.Net.Documents.Document luceneDocument, ScoreDoc score, IState state)
        {
            LuceneDocument = luceneDocument;
            State = state;
            Score = score;
            KnownFields = null;

            CoraxEntry = default;
            Unsafe.SkipInit(out DocumentId);
        }

        public RetrieverInput(IndexFieldsMapping knownFields, IndexEntryReader coraxEntry, string id)
        {
            CoraxEntry = coraxEntry;
            KnownFields = knownFields;
            DocumentId = id;

            Unsafe.SkipInit(out LuceneDocument);
            Unsafe.SkipInit(out State);
            Unsafe.SkipInit(out Score);
        }
    }
}
