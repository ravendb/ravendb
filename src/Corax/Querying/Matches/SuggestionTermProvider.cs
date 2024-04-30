using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Mappings;
using Corax.Pipeline;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Server;
using Sparrow.Server.Strings;
using Voron;
using static Corax.Constants;

namespace Corax.Querying.Matches
{
    public interface IRawTermProvider
    {
        void Next(ref Span<byte> terms, ref Span<Token> tokens, ref Span<float> score);
    }    

    public struct SuggestionTermProvider<TDistanceProvider> : IRawTermProvider
        where TDistanceProvider : IStringDistance
    {
        private readonly Querying.IndexSearcher _searcher;
        private readonly int _fieldId;
        private readonly Slice _term;
        private readonly int _take;
        private readonly bool _sort;
        private readonly float _distance;
        private readonly TDistanceProvider _distanceProvider;

        public SuggestionTermProvider(
            Querying.IndexSearcher searcher, int fieldId,
            Slice term, IndexFieldBinding binding, int take, bool sortByPopularity, float accuracy, TDistanceProvider distanceProvider)
        {            
            _searcher = searcher;
            _fieldId = fieldId;
            _term = term;
            _take = take;
            _distanceProvider = distanceProvider;
            _distance = accuracy;
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

            public SuggestionsNGramTable(int maxGramSize)
            {
                MaxGramSize = maxGramSize;

                _count = 0;
                _currentIdx = -1;
                _maxSize = 0;

                _storage = null;
            }

            private int GramPositionIdx => _maxSize;
            private int GramSizeIdx => GramPositionIdx + _maxSize * sizeof(int);
            private int GramBoostIdx => GramSizeIdx + _maxSize * sizeof(int);

            private Span<byte> Word => _storage.AsSpan().Slice(0, _maxSize);
            private Span<float> GramBoost => MemoryMarshal.Cast<byte, float>(_storage.AsSpan().Slice(GramBoostIdx, sizeof(float) * _maxSize));
            private Span<int> GramSize => MemoryMarshal.Cast<byte, int>(_storage.AsSpan().Slice(GramSizeIdx, sizeof(int) * _maxSize));
            private Span<int> GramPosition => MemoryMarshal.Cast<byte, int>(_storage.AsSpan().Slice(GramPositionIdx, sizeof(int) * _maxSize));


            public void Generate(ReadOnlySpan<byte> word)
            {
                int wordLength = word.Length + 1;

                // Copy to storage.                
                _maxSize = wordLength * wordLength;

                int minSize = _maxSize + (2 * _maxSize * sizeof(int)) + _maxSize * sizeof(float);
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

        public static SuggestionTermProvider<TDistanceProvider> YieldSuggestions(Querying.IndexSearcher searcher, int fieldId, Slice term, IndexFieldBinding binding, TDistanceProvider distanceProvider, bool sortByPopularity, float distance, int take = -1)
        {
            return new SuggestionTermProvider<TDistanceProvider>(searcher, fieldId, term, binding, take, sortByPopularity, distance, distanceProvider);
        }       

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Next(ref Span<byte> terms, ref Span<Token> tokens, ref Span<float> scores)
        {
            var allocator = _searcher.Allocator;

            var values = new FastList<(Slice Term, int Popularity)>();
            var dictionary = new Dictionary<uint, int>();

            using var gramTable = new SuggestionsNGramTable(Suggestions.DefaultNGramSize);

            // We initialize the iterator and store it in the stack memory.
            Slice.From(allocator, $"{Constants.IndexWriter.SuggestionsTreePrefix}{_fieldId}", out var treeName);
            if (_searcher.Transaction.TryGetCompactTreeFor(treeName, out var tree) == false)
                goto NoResults;

            var iter = tree.Iterate();

            gramTable.Generate(_term.AsSpan());
            while (gramTable.MoveNext(out var ngram, out var boost))
            {
                iter.Seek(ngram);

                ref var ngramStart = ref MemoryMarshal.GetReference(ngram);

                ReadOnlySpan<byte> key;
                while (iter.MoveNext(out var gramCompactKey, out _, out _))
                {
                    var gramKey = gramCompactKey.Decoded();

                    // There must be a shared prefix.
                    ref var gramKeyStart = ref MemoryMarshal.GetReference(gramKey);

                    var cmp = Memory.CompareInline(ref ngramStart, ref gramKeyStart, Math.Min(ngram.Length, gramKey.Length));
                    if (cmp > 0)
                        break;
                    if (cmp < 0)
                        continue;

                    if (ngram.Length == Suggestions.DefaultNGramSize)
                    {
                        key = gramKey.Slice(Suggestions.DefaultNGramSize + 1);
                    }
                    else
                    {
                        // This is an ngram prefix, we need to figure out where to cut it.
                        key = gramKey.Slice(gramKey.IndexOf((byte)':') + 1);
                    }

                    uint keyHash = Hashing.XXHash32.CalculateInline(key);

                    if (dictionary.TryGetValue(keyHash, out int location) == false)
                    {
                        location = values.Count;

                        Slice.From(allocator, key, out var gramSlice);
                        values.Add((gramSlice, 0));
                        dictionary.Add(keyHash, location);
                    }

                    ref var item = ref values.GetAsRef(location);
                    item.Popularity++;
                }
            }

            if (values.Count == 0)
                goto NoResults;

            if (_sort)
            {
                var sorter = new Sorter<(Slice, int), ScoreComparer>();
                sorter.Sort(values.AsUnsafeSpan());
            }

            int take = _take;
            if (take == Constants.IndexSearcher.TakeAll)
                take = int.MaxValue;
            take = Math.Min(Math.Min(take, values.Count), tokens.Length);

            Span<byte> auxTerms = terms;

            int tokenTaken = 0;
            int totalTermsBytes = 0;
            for (int tokensCount = 0; tokensCount < values.Count; tokensCount++)
            {
                ref var v = ref values.GetAsRef(tokensCount);
                
                var termKey = v.Term.AsReadOnlySpan();

                if (termKey.Length > 1 && termKey[^1] == '\0')
                    termKey = termKey.Slice(0, termKey.Length - 1);

                if (termKey.Length >= auxTerms.Length)
                    break; // No enough space to put it here. 

                float score = _distanceProvider.GetDistance(_term, termKey);
                if (score < _distance)
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
    }
}
