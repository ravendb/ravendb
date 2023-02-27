using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Mappings;
using Corax.Pipeline;
using Corax.Utils;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Server;
using Sparrow.Server.Strings;

using Voron;
using Voron.Data.CompactTrees;
using static Corax.Constants;

namespace Corax.Queries
{
    public interface IRawTermProvider
    {
        void Next(ref Span<byte> terms, ref Span<Token> tokens, ref Span<float> score);
    }    

    public unsafe partial struct SuggestionTermProvider<TDistanceProvider> : IRawTermProvider
        where TDistanceProvider : IStringDistance
    {
        private readonly IndexSearcher _searcher;
        private readonly int _fieldId;
        private readonly Slice _term;
        private IndexFieldBinding _binding;
        private readonly int _take;
        private readonly bool _sort;
        private readonly float _distance;
        private readonly delegate*<ref SuggestionTermProvider<TDistanceProvider>, ref Span<byte>, ref Span<Token>, ref Span<float>, void> _nextFunc;
        private readonly TDistanceProvider _distanceProvider;

        public SuggestionTermProvider(
            IndexSearcher searcher, int fieldId,
            Slice term, IndexFieldBinding binding, int take, bool sortByPopularity, float accuracy, TDistanceProvider distanceProvider,
            delegate*<ref SuggestionTermProvider<TDistanceProvider>, ref Span<byte>, ref Span<Token>, ref Span<float>, void> nextFunc)
        {            
            _searcher = searcher;
            _fieldId = fieldId;
            _term = term;
            _binding = binding;
            _take = take;
            _distanceProvider = distanceProvider;
            _distance = accuracy;
            _nextFunc = nextFunc;
            _sort = sortByPopularity;
        }

        private struct ScoreComparer : IComparer<(Slice, float)>, IComparer<(Slice, int)>
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare((Slice, float) x, (Slice, float) y)
            {
                float result = y.Item2 - x.Item2;
                return Math.Abs(result) < Constants.Boosting.ScoreEpsilon ? 0 : Math.Sign(result);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare((Slice, int) x, (Slice, int) y)
            {
                return y.Item2 - x.Item2;
            }
        }

        private struct SuggestionsNGramTable : IDisposable
        {
            private int _count;
            private int _currentIdx;
            private int _maxSize;

            private byte[] _storage;

            public readonly int MaxGramSize;

            public int Count => _count;

            public SuggestionsNGramTable(int maxGramSize)
            {
                MaxGramSize = maxGramSize;

                _count = 0;
                _currentIdx = -1;
                _maxSize = 0;

                _storage = null;
            }

            private int WordIdx => 0;
            private int GramPositionIdx => _maxSize;
            private int GramSizeIdx => GramPositionIdx + _maxSize * sizeof(int);
            private int GramBoostIdx => GramSizeIdx + _maxSize * sizeof(int);

            private Span<byte> Word => _storage.AsSpan().Slice(0, _maxSize);
            private Span<float> GramBoost => MemoryMarshal.Cast<byte, float>(_storage.AsSpan().Slice(GramBoostIdx, sizeof(float) * _maxSize));
            private Span<int> GramSize => MemoryMarshal.Cast<byte, int>(_storage.AsSpan().Slice(GramSizeIdx, sizeof(int) * _maxSize));
            private Span<int> GramPosition => MemoryMarshal.Cast<byte, int>(_storage.AsSpan().Slice(GramPositionIdx, sizeof(int) * _maxSize));


            public void Generate(ReadOnlySpan<byte> word)
            {
                // Copy to storage.                
                _maxSize = word.Length * word.Length;

                int minSize = word.Length + (2 * _maxSize * sizeof(int)) + _maxSize * sizeof(float);
                if ( _storage == null )
                {
                    _storage = ArrayPool<byte>.Shared.Rent(minSize);
                }                    
                else if ( _storage.Length < minSize )
                {
                    ArrayPool<byte>.Shared.Return(_storage);
                    _storage =  ArrayPool<byte>.Shared.Rent(minSize);
                }

                word.CopyTo(Word);
                
                var gramBoost = GramBoost;
                var gramPosition = GramPosition;
                var gramSize = GramSize;

                for (int ng = 2; ng <= MaxGramSize; ng++)
                {
                    for (int i = 0; i < word.Length - ng + 1; i++)
                    {
                        gramPosition[_count] = i;
                        gramSize[_count] = ng;
                        gramBoost[_count] = 1;

                        _count++;
                    }
                }
            }

            public void Reset()
            {
                _currentIdx = -1;
            }

            public bool MoveNext(out ReadOnlySpan<byte> ngram, out float boost)
            {
                // We advance one place
                _currentIdx++;

                // Check if we are not done. 
                if (_currentIdx >= _count)
                {
                    boost = 0;
                    ngram = ReadOnlySpan<byte>.Empty;
                    return false;
                }

                ngram = _storage.AsSpan().Slice(GramPosition[_currentIdx], GramSize[_currentIdx]);
                boost = GramBoost[_currentIdx];
                return true;
            }

            public void Dispose()
            {
                if (_storage != null)
                    ArrayPool<byte>.Shared.Return(_storage);
            }
        }


        public static SuggestionTermProvider<TDistanceProvider> YieldSuggestions(IndexSearcher searcher, int fieldId, Slice term, IndexFieldBinding binding, TDistanceProvider distanceProvider, bool sortByPopularity, float distance, int take = -1)
        {
            static void NextNGram(ref SuggestionTermProvider<TDistanceProvider> provider, ref Span<byte> terms, ref Span<Token> tokens, ref Span<float> scores)
            {
                int fieldId = provider._fieldId;
                var allocator = provider._searcher.Allocator;

                Slice.From(allocator, $"__Suggestion_{fieldId}", out var treeName);
                var tree = provider._searcher.Transaction.CompactTreeFor(treeName);

                using var gramTable = new SuggestionsNGramTable(Suggestions.DefaultNGramSize);
                gramTable.Generate(provider._term.AsSpan());

                // We initialize the iterator and store it in the stack memory.
                var iter = tree.Iterate();
                var values = new FastList<(Slice Term, int Popularity)>();
                var dictionary = new Dictionary<uint, int>();
                while (gramTable.MoveNext(out var ngram, out var boost))
                {
                    iter.Seek(ngram);

                    byte lastByte = ngram[^1];

                    while (iter.MoveNext(out var gramKeyScope, out var _))
                    {
                        var gramKey = gramKeyScope.Key.Decoded();
                        if (gramKey[ngram.Length - 1] > lastByte)
                            break;

                        var key = gramKey.Slice(Suggestions.DefaultNGramSize);
                        uint keyHash = Hashing.XXHash32.CalculateInline(key);

                        if (dictionary.TryGetValue(keyHash, out int location) == false)
                        {
                            location = values.Count;

                            Slice.From(allocator, gramKey, out var gramSlice);
                            values.Add((gramSlice, 0));
                            dictionary.Add(keyHash, location);
                        }

                        ref var item = ref values.GetAsRef(location);
                        item.Popularity++;

                        gramKeyScope.Dispose();
                    }
                }

                if (values.Count == 0)
                    goto NoResults;

                if (provider._sort)
                {
                    var sorter = new Sorter<(Slice, int), ScoreComparer>();
                    sorter.Sort(values.AsUnsafeSpan());
                } 

                int take = provider._take;
                if (take == Constants.IndexSearcher.TakeAll)
                    take = int.MaxValue;
                take = Math.Min(Math.Min(take, values.Count), tokens.Length);

                Span<byte> auxTerms = terms;

                NGramDistance nGramDistance = default;

                int tokenTaken = 0;
                int totalTermsBytes = 0;
                var term = provider._term;
                for (int tokensCount = 0; tokensCount < values.Count; tokensCount++)
                {
                    ref var v = ref values.GetAsRef(tokensCount);

                    var termKey = v.Term.AsReadOnlySpan();
                    
                    if (termKey.Length > 1 && termKey[^1] == '\0')
                        termKey = termKey.Slice(0, termKey.Length - 1);

                    if (termKey.Length > Suggestions.DefaultNGramSize)
                        termKey = termKey.Slice(Suggestions.DefaultNGramSize);

                    if (termKey.Length >= auxTerms.Length)
                        break; // No enough space to put it here. 

                    float score = nGramDistance.GetDistance(term, termKey);
                    if (score < provider._distance)
                        continue;

                    termKey.CopyTo(auxTerms);
                    tokens[tokenTaken].Offset = totalTermsBytes;
                    tokens[tokenTaken].Length = (uint)termKey.Length;

                    scores[tokenTaken] = score;

                    auxTerms = auxTerms.Slice(termKey.Length);
                    totalTermsBytes += termKey.Length;

                    tokenTaken++;
                    if (tokenTaken >= take)
                        break;
                }

                terms = terms.Slice(0, totalTermsBytes);
                tokens = tokens.Slice(0, tokenTaken);
                scores = scores.Slice(0, tokenTaken);

                return;


            NoResults:
                terms = terms.Slice(0, 0);
                tokens = tokens.Slice(0, 0);
                scores = scores.Slice(0, 0);
            }

            static void Next(ref SuggestionTermProvider<TDistanceProvider> provider, ref Span<byte> terms, ref Span<Token> tokens, ref Span<float> scores)
            {
                int fieldId = provider._fieldId;
                var term = provider._term;
                var maxDistance = provider._distance;

                // We get the actual field tree. 
                var fields = provider._searcher.Transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
                var fieldTree = fields?.CompactTreeFor(provider._binding.FieldName);
                if (fieldTree == null)
                    goto NoResults;

                // For each term to look for, we will look at every potential hit using fuzzy searching with low threshold unless we are doing it
                // using Levenshtein, in which case we just do it properly.                
                var currentTermSlice = provider._term;                                
                var iter = fieldTree.FuzzyIterate(term, typeof(TDistanceProvider) == typeof(LevenshteinDistance) ? provider._distance : 0.3f);
                iter.Seek(currentTermSlice);

                var values = new FastList<(Slice Term, float)>();

                var allocator = provider._searcher.Allocator;
                TDistanceProvider distance = provider._distanceProvider;
                while (iter.MoveNext(out var keyScope, out var _, out float score))
                {
                    var key = keyScope.Key.Decoded();

                    // The original distance is Levenshtein, therefore we dont need to recompute it. 
                    if (typeof(TDistanceProvider) != typeof(LevenshteinDistance))
                    {
                        score = distance.GetDistance(term, key);
                        if (score < provider._distance)
                            continue;
                    }

                    Slice.From(allocator, key, out var keySlice);
                    values.Add((keySlice, score));
                    keyScope.Dispose();
                }

                if (values.Count == 0)
                    goto NoResults;

                if (provider._sort)
                {
                    var sorter = new Sorter<(Slice, float), ScoreComparer>();
                    sorter.Sort(values.AsUnsafeSpan());
                }

                int take = provider._take;
                if (take == Constants.IndexSearcher.TakeAll)
                    take = int.MaxValue;
                take = Math.Min(take, tokens.Length);

                Span<byte> auxTerms = terms;

                int tokensCount = 0;
                int tokenTaken = 0;
                int totalTermsBytes = 0;                
                for (; tokensCount < values.Count; tokensCount++)
                {
                    ref var v = ref values.GetAsRef(tokensCount);

                    int termSize = v.Term.Size;
                    if (termSize > 1 && v.Term[termSize - 1] == '\0')
                        termSize--; //delete null char from the end

                    Debug.Assert(termSize > 0);

                    if (termSize >= auxTerms.Length)
                        break; // No enough space to put another one. 

                    v.Item1.CopyTo(auxTerms);
                    tokens[tokenTaken].Offset = totalTermsBytes;
                    tokens[tokenTaken].Length = (uint)termSize;

                    scores[tokenTaken] = v.Item2;

                    auxTerms = auxTerms.Slice(termSize);
                    totalTermsBytes += termSize;

                    tokenTaken++;
                    if (tokenTaken >= take)
                        break;
                }

                terms = terms.Slice(0, totalTermsBytes);
                tokens = tokens.Slice(0, tokenTaken);
                scores = scores.Slice(0, tokenTaken);

                return;

            NoResults:
                terms = terms.Slice(0, 0);
                tokens = tokens.Slice(0, 0);
                scores = scores.Slice(0, 0);
            }

            if (typeof(TDistanceProvider) == typeof(LevenshteinDistance))
                return new SuggestionTermProvider<TDistanceProvider>(searcher, fieldId, term, binding, take, sortByPopularity, distance, distanceProvider, &Next);
            else if (typeof(TDistanceProvider) == typeof(NGramDistance))
                return new SuggestionTermProvider<TDistanceProvider>(searcher, fieldId, term, binding, take, sortByPopularity, distance, distanceProvider, &NextNGram);
            else if (typeof(TDistanceProvider) == typeof(JaroWinklerDistance))
                return new SuggestionTermProvider<TDistanceProvider>(searcher, fieldId, term, binding, take, sortByPopularity, distance, distanceProvider, &Next);
            else if (typeof(TDistanceProvider) == typeof(NoStringDistance))
                return new SuggestionTermProvider<TDistanceProvider>(searcher, fieldId, term, binding, take, sortByPopularity, distance, distanceProvider, &NextNGram);
            else
                throw new NotSupportedException($"The distance function is not supported.");
        }       

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Next(ref Span<byte> terms, ref Span<Token> tokens, ref Span<float> scores)
        {
            _nextFunc(ref this, ref terms, ref tokens, ref scores);
        }
    }
}
