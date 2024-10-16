using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow;
using Sparrow.Server;
using Voron;
using Voron.Data.Lookups;
using Voron.Impl;

namespace Corax.Querying.Matches
{
    [DebuggerDisplay("{DebugView,nq}")]
    internal unsafe struct ExactVectorSearchMatch : IQueryMatch
    {
        private readonly long _count;
        private Lookup<Int64LookupKey>.ForwardIterator _entriesPagesIt;
        private bool _firstFill = true;
        private readonly PageLocator _pageLocator = new();
        private Dictionary<long, float> _scores = new();
        
        private readonly delegate*<Span<byte>, Span<byte>, float> _similarityFunc;

        private readonly IndexSearcher _searcher;
        private readonly Transaction _tx;
        private readonly long _fieldRootPage;
        private readonly Memory<byte> _vectorToSearch;
        private readonly float _minimumMatch;
        private readonly VectorSimilarityType _similarityType;

        public ExactVectorSearchMatch(IndexSearcher searcher, Transaction tx, long fieldRootPage, Memory<byte> vectorToSearch, float minimumMatch, VectorSimilarityType similarityType)
        {
            _count = searcher.NumberOfEntries;
            _similarityFunc = similarityType switch
            {
                VectorSimilarityType.I1 => &SimilarityI1,
                VectorSimilarityType.I8 => &SimilarityI8,
                VectorSimilarityType.Cosine => &SimilarityCosine,
                _ => throw new ArgumentOutOfRangeException(nameof(similarityType), similarityType, null)
            };
            _searcher = searcher;
            _tx = tx;
            _fieldRootPage = fieldRootPage;
            _minimumMatch = minimumMatch;
            _similarityType = similarityType;
            _vectorToSearch = vectorToSearch;
        }


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
                _entriesPagesIt = _tx.LookupFor<Int64LookupKey>(Constants.IndexWriter.EntryIdToLocationSlice).Iterate();
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
            using var enumerator = _searcher.GetEntryTermsReader(buffer[..matches], _pageLocator);
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
            while (reader.FindNextStored(_fieldRootPage))
            {
                var vector = reader.StoredField.Value.ToSpan();
                if (vector.Length != _vectorToSearch.Length)
                    continue;

                var similarity = _similarityFunc(_vectorToSearch.Span, vector);
                _scores[id] = similarity;
                if (similarity > _minimumMatch)
                    return true;
            }

            return false;
        }

        private static float SimilarityI8(Span<byte> lhs, Span<byte> rhs)
        {
            PortableExceptions.Throw<NotImplementedException>($"{nameof(SimilarityI8)}: Not implemented");
            return 0;
        }
        
        private static float SimilarityCosine(Span<byte> lhs, Span<byte> rhs)
        {
            Debug.Assert(lhs.Length == rhs.Length, "lhs.Length == rhs.Length");
            var lhsAsFloat = MemoryMarshal.Cast<byte, float>(lhs);
            var rhsAsFloat = MemoryMarshal.Cast<byte, float>(rhs);
            return TensorPrimitives.CosineSimilarity(lhsAsFloat, rhsAsFloat);
        }

        private static float SimilarityI1(Span<byte> lhs, Span<byte> rhs)
        {
            Debug.Assert(lhs.Length == rhs.Length, "lhs.Length == rhs.Length");
            var differences = TensorPrimitives.HammingBitDistance<byte>(lhs, rhs);
            return 1 - (differences / ((float)lhs.Length * 8));
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
            _searcher.GetIndexedFieldNamesByRootPage().TryGetValue(_fieldRootPage, out var fieldName);
            return new QueryInspectionNode(nameof(ExactVectorSearchMatch),
                parameters: new Dictionary<string, string>()
                {
                    {Corax.Constants.QueryInspectionNode.FieldName, fieldName.ToString()},
                    {nameof(VectorSimilarityType), _similarityType.ToString()},
                    {"minimumMatch", _minimumMatch.ToString()}
                });
        }

        string DebugView => Inspect().ToString();
    }
}
