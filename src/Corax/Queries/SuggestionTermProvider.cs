using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Corax.Pipeline;
using Corax.Utils;
using Microsoft.VisualBasic;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries
{
    public interface IRawTermProvider
    {
        void Next(ref Span<byte> terms, ref Span<Token> tokens);
    }
    

    public unsafe partial struct SuggestionTermProvider<TDistanceProvider> : IRawTermProvider
    where TDistanceProvider : IStringDistance
    {
        public const int DefaultNGramSize = 4;

        private readonly IndexSearcher _searcher;
        private readonly int _fieldId;
        private readonly Slice _term;
        private IndexFieldBinding _binding;
        private readonly int _take;
        private readonly bool _sort;
        private readonly float _accuracy;
        private readonly delegate*<ref SuggestionTermProvider<TDistanceProvider>, ref Span<byte>, ref Span<Token>, void> _nextFunc;
        private readonly TDistanceProvider _distanceProvider;

        public SuggestionTermProvider(
            IndexSearcher searcher, int fieldId,
            Slice term, IndexFieldBinding binding, int take, bool sortByPopularity, float accuracy, TDistanceProvider distanceProvider,
            delegate*<ref SuggestionTermProvider<TDistanceProvider>, ref Span<byte>, ref Span<Token>, void> nextFunc)
        {
            _searcher = searcher;
            _fieldId = fieldId;
            _term = term;
            _binding = binding;
            _take = take;
            _distanceProvider = distanceProvider;
            _accuracy = accuracy;
            _nextFunc = nextFunc;
            _sort = sortByPopularity;
        }

        private struct ScoreComparer : IComparer<(Slice, int)>
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare((Slice, int) x, (Slice, int) y)
            {
                return x.Item2 - y.Item2;
            }
        }

        public static SuggestionTermProvider<TDistanceProvider> YieldFromNGram(IndexSearcher searcher, int fieldId, Slice term, IndexFieldBinding binding, TDistanceProvider distanceProvider, bool sortByPopularity, float accuracy, int take = -1)
        {
            static void Next(ref SuggestionTermProvider<TDistanceProvider> provider, ref Span<byte> terms, ref Span<Token> tokens)
            {
                int fieldId = provider._fieldId;
                var allocator = provider._searcher.Allocator;
                var term = provider._term;

                Slice.From(allocator, $"__Suggestion_{fieldId}", out var treeName);
                var tree = provider._searcher.Transaction.CompactTreeFor(treeName);

                var keys = SuggestionsKeys.Generate(allocator, DefaultNGramSize, term.AsSpan(), out int keysCount).ToReadOnlySpan();
                int keySizes = keys.Length / keysCount;

                var values = new FastList<(Slice Term, int Popularity)>();
                var dictionary = new Dictionary<Slice, int>(SliceComparer.Instance);
                for (int i = 0; i < keysCount; i++)
                {
                    // We initialize the iterator and store it in the stack memory.
                    var iter = tree.Iterate();
                    iter.Seek(keys.Slice(i * keySizes, DefaultNGramSize));

                    byte lastByte = keys[i * keySizes + DefaultNGramSize - 1];
                    while (iter.MoveNext(out Slice key, out var _))
                    {
                        if (key[DefaultNGramSize - 1] != lastByte)
                            break;

                        var actualKey = key.Skip(allocator, DefaultNGramSize, ByteStringType.Immutable);
                        if (dictionary.TryGetValue(actualKey, out int location) == false)
                        {
                            location = values.Count;
                            values.Add((actualKey, 0));
                            dictionary.Add(actualKey, location);
                        }

                        ref var item = ref values.GetAsRef(location);
                        item.Popularity++;
                    }
                }

                if (provider._sort)
                {
                    var sorter = new Sorter<(Slice, int), ScoreComparer>();
                    sorter.Sort(values.AsUnsafeSpan());
                }

                int take = provider._take;
                if (take == Constants.IndexSearcher.TakeAll)
                    take = int.MaxValue;
                take = Math.Min(take, values.Count);

                Span<byte> auxTerms = terms;

                int tokensCount = 0;
                int tokenTaken = 0;
                int totalTermsBytes = 0;
                for (; tokensCount < take; tokensCount++)
                {
                    ref var v = ref values.GetAsRef(tokensCount);

                    var distance = provider._distanceProvider.GetDistance(term, v.Term);
                    if (distance < provider._accuracy)
                    {
                        continue;
                    }
                    
                    int termSize = v.Term.Size;
                    if (termSize > 1 && v.Term[termSize - 1] == '\0') 
                        termSize--; //delete null char from the end
                    if (termSize >= auxTerms.Length)
                        break; // No enough space to put another one. 

                    v.Item1.CopyTo(auxTerms);
                    tokens[tokenTaken].Offset = totalTermsBytes;
                    tokens[tokenTaken].Length = (uint)termSize;
                    tokenTaken++;

                    auxTerms = auxTerms.Slice(termSize);
                    totalTermsBytes += termSize;
                }

                terms = terms.Slice(0, totalTermsBytes);
                tokens = tokens.Slice(0, tokenTaken);
            }

            return new SuggestionTermProvider<TDistanceProvider>(searcher, fieldId, term, binding, take, sortByPopularity, accuracy, distanceProvider, &Next);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Next(ref Span<byte> terms, ref Span<Token> tokens)
        {
            _nextFunc(ref this, ref terms, ref tokens);
        }
    }
}
