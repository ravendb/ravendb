using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Newtonsoft.Json;
using Sparrow;
using Voron;
using Voron.Data.Lookups;
using Voron.Impl;
using Container = Voron.Data.Containers.Container;

namespace Corax.Querying.Matches
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct ExactVectorSearchMatch(IndexSearcher searcher, Transaction tx, long fieldRootPage, byte[] vectorToSearch, float minimumMatch)
        : IQueryMatch
    {
        private readonly long _count = searcher.NumberOfEntries;
        private Lookup<Int64LookupKey>.ForwardIterator _entriesPagesIt;
        private bool _firstFill = true;
        private readonly PageLocator _pageLocator = new();
        private Dictionary<long, float> _scores = new();

        public SkipSortingResult AttemptToSkipSorting()
        {
            return SkipSortingResult.SortingIsRequired;
        }

        public bool IsBoosting => false;
        public long Count => _count;
        public QueryCountConfidence Confidence => QueryCountConfidence.Low;

        public int Fill(Span<long> matches)
        {
            if (_firstFill)
            {
                _entriesPagesIt = tx.LookupFor<Int64LookupKey>(Constants.IndexWriter.EntryIdToLocationSlice).Iterate();
                _entriesPagesIt.Reset();
                _firstFill = false;
            }

            int count;
            do
            {
                count = _entriesPagesIt.FillKeys(matches);
                if (count == 0)
                    break;
                count = AndWith(matches, count);
            } while (count == 0);
            return count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer, int matches)
        {
            var matchIndex = 0;
            var bufferIndex = 0;
            using var enumerator = searcher.GetEntryTermsReader(buffer[..matches], _pageLocator);
            while (enumerator.MoveNext())
            {
                var reader = enumerator.Current;
                long id = buffer[bufferIndex];
                if (ExactVectorMatch(id, reader))
                {
                    buffer[matchIndex++] = id;
                }

                bufferIndex++;
            }

            return matchIndex;
        }

        private bool ExactVectorMatch(long id, EntryTermsReader reader)
        {
            while (reader.FindNextStored(fieldRootPage))
            {
                var vector = reader.StoredField.Value.ToSpan();
                if(vector.Length != vectorToSearch.Length)
                    continue;

                var similarity = SimilarityI1(vectorToSearch, vector);
                _scores[id] = similarity; 
                if (similarity > minimumMatch)
                    return true;
            }

            return false;
        }

        public static unsafe float SimilarityI1(Span<byte> lhs, Span<byte> rhs)
        {
            // Code adapted from:
            // https://github.com/dotnet/smartcomponents/blob/main/src/SmartComponents.LocalEmbeddings/EmbeddingI1.cs#L103

            // The following approach to load the vectors is considerably faster than using a "fixed" block
            ref var lhsRef = ref lhs[0];
            var lhsPtr = (byte*)Unsafe.AsPointer(ref lhsRef);
            ref var rhsRef = ref rhs[0];
            var rhsPtr = (byte*)Unsafe.AsPointer(ref rhsRef);
            var lhsPtrEnd = lhsPtr + lhs.Length;
            var differences = 0;

            // Process as many Vector256 blocks as possible
            while (lhsPtr <= lhsPtrEnd - 32)
            {
                var lhsBlock = Vector256.Load(lhsPtr);
                var rhsBlock = Vector256.Load(rhsPtr);
                var xorBlock = Vector256.Xor(lhsBlock, rhsBlock).AsUInt64();

                // This is 10x faster than any AVX2/SSE3 vectorized approach I could find (e.g.,
                // avx2-lookup from https://stackoverflow.com/a/50082218). However I didn't try
                // AVX512 approaches (vectorized popcnt) since hardware support is less common.
                differences +=
                    BitOperations.PopCount(xorBlock.GetElement(0)) +
                    BitOperations.PopCount(xorBlock.GetElement(1)) +
                    BitOperations.PopCount(xorBlock.GetElement(2)) +
                    BitOperations.PopCount(xorBlock.GetElement(3));

                lhsPtr += 32;
                rhsPtr += 32;
            }

            // Process as many Vector128 blocks as possible
            while (lhsPtr <= lhsPtrEnd - 16)
            {
                var lhsBlock = Vector128.Load(lhsPtr);
                var rhsBlock = Vector128.Load(rhsPtr);
                var xorBlock = Vector128.Xor(lhsBlock, rhsBlock).AsUInt64();

                differences +=
                    BitOperations.PopCount(xorBlock.GetElement(0)) +
                    BitOperations.PopCount(xorBlock.GetElement(1));

                lhsPtr += 16;
                rhsPtr += 16;
            }

            // Process the remaining bytes
            while (lhsPtr < lhsPtrEnd)
            {
                var left = *lhsPtr;
                var right = *rhsPtr;
                var xor = (byte)(left ^ right);
                differences += BitOperations.PopCount(xor);
                lhsPtr++;
                rhsPtr++;
            }

            return 1 - (differences / (float)(lhs.Length * 8));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
            // TODO: Need to think about a way that doesn't allocate so much for scoring
            for (int i = 0; i < matches.Length; i++)
            {
                if (_scores.TryGetValue(matches[i], out var f))
                {
                    scores[i] = f;
                }
            }
            /*
             This is another way to achieve the same thing, but it is significantly slower
             
             
            Page lastPage = default;
            for (int i = 0; i < matches.Length; i++)
            {
                scores[i] = 0;
                var reader = searcher.GetEntryTermsReader(matches[i], ref lastPage);
                while (reader.FindNextStored(fieldRootPage))
                {
                    var vector = reader.StoredField.Value.ToSpan();
                    if (vector.Length != vectorToSearch.Length)
                        continue;
                    scores[i] = SimilarityI1(vectorToSearch, vector) * boostFactor;
                    break;
                }
            }
            */
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode(nameof(ExactVectorSearchMatch),
                parameters: new Dictionary<string, string>()
                {
                });
        }

        string DebugView => Inspect().ToString();
    }
}
