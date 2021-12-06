using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Corax.Pipeline;
using Corax.Utils;
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


    public unsafe struct SuggestionTermProvider : IRawTermProvider
    {

        public const int DefaultNGramSize = 4;

        private readonly IndexSearcher _searcher;
        private readonly int _fieldId;
        private readonly Slice _term;
        private readonly Analyzer _analyzer;
        private readonly int _take;
        private readonly delegate*<ref SuggestionTermProvider, ref Span<byte>, ref Span<Token>, void> _nextFunc;

        public SuggestionTermProvider(
            IndexSearcher searcher, int fieldId, 
            Slice term, Analyzer analyzer, int take,
            delegate*<ref SuggestionTermProvider, ref Span<byte>, ref Span<Token>, void> nextFunc)
        {
            _searcher = searcher;
            _fieldId = fieldId;
            _term = term;
            _analyzer = analyzer;
            _take = take;
            _nextFunc = nextFunc;
        }

        private struct ScoreComparer : IComparer<(Slice, int)>
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare((Slice, int) x, (Slice, int) y)
            {
                return x.Item2 - y.Item2;
            }
        }

        public static SuggestionTermProvider YieldFromNGram(IndexSearcher searcher, int fieldId, Slice term, Analyzer analyzer, int take = -1)
        {
            static void Next(ref SuggestionTermProvider provider, ref Span<byte> terms, ref Span<Token> tokens)
            {
                int fieldId = provider._fieldId;
                var allocator = provider._searcher.Allocator;
                var term = provider._term;                

                Slice.From(allocator, $"__Suggestion_{fieldId}", out var treeName);
                var tree = provider._searcher.Transaction.CompactTreeFor(treeName);

                var keys = SuggestionsKeys.Generate(allocator, DefaultNGramSize, term.AsSpan(), out int keysCount).ToReadOnlySpan();
                int keySizes = keys.Length / keysCount;

                var values = new FastList<(Slice, int)>();
                var dictionary = new Dictionary<Slice, int>();                
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
                        if (!dictionary.TryGetValue(actualKey, out int location))
                        {
                            location = values.Count;
                            values.Add((actualKey, 0));
                        }

                        ref var item = ref values.GetAsRef(location);
                        item.Item2++;                        
                    }
                }

                var sorter = new Sorter<(Slice, int), ScoreComparer>();
                sorter.Sort(values.AsUnsafeSpan());

                int take = provider._take;
                if (take == -1)
                    take = int.MaxValue;
                take = Math.Min(take, values.Count);

                Span<byte> auxTerms = terms;

                int tokensCount = 0;
                int totalTermsBytes = 0;
                for (; tokensCount < take; tokensCount++ )
                {
                    ref var v = ref values.GetAsRef(tokensCount);

                    int termSize = v.Item1.Size;
                    if (termSize >= auxTerms.Length)
                        break; // No enough space to put another one. 

                    v.Item1.CopyTo(auxTerms);

                    auxTerms = auxTerms.Slice(termSize);
                    totalTermsBytes += termSize;
                }

                terms = terms.Slice(0, totalTermsBytes);
                tokens = tokens.Slice(0, tokensCount);                
            }

            if (analyzer != null)
                throw new NotSupportedException("Non null analyzer is not supported yet.");

            return new SuggestionTermProvider(searcher, fieldId, term, analyzer, take, &Next);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Next(ref Span<byte> terms, ref Span<Token> tokens)
        {
            _nextFunc(ref this, ref terms, ref tokens);
        }
    }
}
