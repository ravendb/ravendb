using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Suggestions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Indexing;
using Sparrow.Json;
using Sparrow.Logging;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public sealed class LuceneSuggestionIndexReader : IndexOperationBase
    {
        private readonly IndexSearcher _searcher;
        private readonly IDisposable _releaseSearcher;
        private readonly IDisposable _releaseReadTransaction;

        private readonly IState _state;

        public LuceneSuggestionIndexReader(Index index, LuceneVoronDirectory directory, IndexSearcherHolder searcherHolder, Transaction readTransaction)
            : base(index, LoggingSource.Instance.GetLogger<LuceneSuggestionIndexReader>(index._indexStorage.DocumentDatabase.Name))
        {
            _releaseReadTransaction = directory.SetTransaction(readTransaction, out _state);
            _releaseSearcher = searcherHolder.GetSearcher(readTransaction, _state, out _searcher);
        }

        public SuggestionResult Suggestions(IndexQueryServerSide query, SuggestionField field, JsonOperationContext documentsContext, CancellationToken token)
        {
            var options = field.GetOptions(documentsContext, query.QueryParameters) ?? new SuggestionOptions();
            var terms = field.GetTerms(documentsContext, query.QueryParameters);

            var result = new SuggestionResult
            {
                Name = field.Alias ?? field.Name
            };

            result.Suggestions.AddRange(terms.Count > 1
                ? QueryOverMultipleWords(field, terms, options)
                : QueryOverSingleWord(field, terms[0], options));

            return result;
        }

        private static readonly string[] EmptyArray = new string[0];

        /// <summary> Field name for each word in the ngram index.</summary>
        private const string FWord = "word";

        /// <summary> Boost value for start and end grams</summary>
        private const float BoostStart = 2.0f;
        private const float BoostEnd = 1.0f;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private string[] QueryOverSingleWord(SuggestionField field, string word, SuggestionOptions options)
        {
            // Perform devirtualization of the distance function when supported. 
            switch (options.Distance)
            {
                case StringDistanceTypes.JaroWinkler:
                    return QueryOverSingleWord(field, word, options, new JaroWinklerDistance());
                case StringDistanceTypes.NGram:
                    return QueryOverSingleWord(field, word, options, new NGramDistance());
                default:
                    return QueryOverSingleWord(field, word, options, new LevenshteinDistance());
            }
        }

        private struct GramKeys
        {
            public string Start;
            public string End;
            public string Gram;
        }

        // Avoiding allocations for string keys that are bounded in the set of potential values. 
        private static readonly GramKeys[] GramsTable;

        static LuceneSuggestionIndexReader()
        {
            GramsTable = new GramKeys[5];

            for (int i = 0; i < GramsTable.Length; i++)
            {
                GramsTable[i] = new GramKeys
                {
                    Start = "start" + i,
                    End = "end" + i,
                    Gram = "gram" + i
                };
            }
        }

        private string[] QueryOverSingleWord<TDistance>(SuggestionField suggestionField, string word, SuggestionOptions options, TDistance sd)
            where TDistance : IStringDistance
        {
            var min = options.Accuracy ?? SuggestionOptions.DefaultAccuracy;
            var field = suggestionField.Name;
            var pageSize = options.PageSize;
            var morePopular = options.SortMode == SuggestionSortMode.Popularity;

            int lengthWord = word.Length;

            var ir = _searcher.IndexReader;

            int freq = (ir != null && field != null) ? ir.DocFreq(new Term(FWord, word), _state) : 0;
            int goalFreq = (morePopular && ir != null && field != null) ? freq : 0;

            // if the word exists in the real index and we don't care for word frequency, return the word itself
            if (!morePopular && freq > 0)
            {
                return new[] { word };
            }

            var query = new BooleanQuery();

            var alreadySeen = new HashSet<string>();

            int ng = GetMin(lengthWord);
            int max = ng + 1;

            var table = GramsTable;
            for (; ng <= max; ng++)
            {
                string[] grams = FormGrams(word, ng);

                if (grams.Length == 0)
                {
                    continue; // hmm
                }

                if (BoostStart > 0)
                {
                    // should we boost prefixes?
                    Add(query, table[ng].Start, grams[0], BoostStart); // matches start of word
                }

                if (BoostEnd > 0)
                {
                    // should we boost suffixes
                    Add(query, table[ng].End, grams[grams.Length - 1], BoostEnd); // matches end of word
                }

                for (int i = 0; i < grams.Length; i++)
                {
                    Add(query, table[ng].Gram, grams[i]);
                }
            }

            int maxHits = 10 * pageSize;

            //    System.out.println("Q: " + query);
            ScoreDoc[] hits = _searcher.Search(query, null, maxHits, _state).ScoreDocs;

            //    System.out.println("HITS: " + hits.length());
            var queue = new SuggestWordQueue(pageSize);

            // go thru more than 'maxr' matches in case the distance filter triggers
            int stop = Math.Min(hits.Length, maxHits);

            var suggestedWord = new SuggestWord();
            for (int i = 0; i < stop; i++)
            {
                suggestedWord.Term = _searcher.Doc(hits[i].Doc, _state).Get(FWord, _state); // get orig word

                // don't suggest a word for itself, that would be silly
                if (suggestedWord.Term.Equals(word, StringComparison.OrdinalIgnoreCase))
                    continue;

                // edit distance
                suggestedWord.Score = sd.GetDistance(word, suggestedWord.Term);
                if (suggestedWord.Score < min)
                    continue;

                if (ir != null && field != null)
                {
                    // use the user index
                    suggestedWord.Freq = _searcher.DocFreq(new Term(FWord, suggestedWord.Term), _state); // freq in the index

                    // don't suggest a word that is not present in the field
                    if ((morePopular && goalFreq > suggestedWord.Freq) || suggestedWord.Freq < 1)
                        continue;
                }

                if (alreadySeen.Add(suggestedWord.Term) == false) // we already seen this word, no point returning it twice
                    continue;

                queue.InsertWithOverflow(suggestedWord);
                if (queue.Size() == pageSize)
                {
                    // if queue full, maintain the minScore score
                    min = queue.Top().Score;
                }

                suggestedWord = new SuggestWord();
            }

            int size = queue.Size();
            if (size == 0)
                return EmptyArray;

            // convert to array string
            string[] list = new string[size];
            for (int i = size - 1; i >= 0; i--)
            {
                list[i] = queue.Pop().Term;
            }

            return list;
        }

        /// <summary> Add a clause to a boolean query.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void Add(BooleanQuery q, string k, string v, float boost)
        {
            Query tq = new TermQuery(new Term(k, v));
            tq.Boost = boost;
            q.Add(new BooleanClause(tq, Occur.SHOULD));
        }

        /// <summary> Add a clause to a boolean query.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void Add(BooleanQuery q, string k, string v)
        {
            q.Add(new BooleanClause(new TermQuery(new Term(k, v)), Occur.SHOULD));
        }

        /// <summary> Form all ngrams for a given word.</summary>
        /// <param name="text">the word to parse
        /// </param>
        /// <param name="ng">the ngram length e.g. 3
        /// </param>
        /// <returns> an array of all ngrams in the word and note that duplicates are not removed
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static string[] FormGrams(string text, int ng)
        {
            int len = text.Length;
            string[] res = new string[len - ng + 1];
            for (int i = 0; i < len - ng + 1; i++)
            {
                res[i] = text.Substring(i, (i + ng) - (i));
            }
            return res;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetMin(int l)
        {
            int r = 1;
            if (l > 5)
                r = 3;
            else if (l == 5)
                r = 2;

            return r;
        }

        private string[] QueryOverMultipleWords(SuggestionField field, List<string> words, SuggestionOptions options)
        {
            options.SortMode = SuggestionSortMode.None;

            var result = new HashSet<string>();

            var pageSize = options.PageSize;
            foreach (var term in words)
            {
                if (pageSize <= 0)
                    break;

                foreach (var suggestion in QueryOverSingleWord(field, term, options))
                {
                    if (result.Add(suggestion) == false)
                        continue;

                    pageSize--;
                    if (pageSize <= 0)
                        break;
                }
            }

            return result.ToArray();
        }

        public override void Dispose()
        {
            _releaseSearcher?.Dispose();
            _releaseReadTransaction?.Dispose();
        }
    }
}
