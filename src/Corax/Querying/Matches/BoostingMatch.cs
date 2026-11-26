using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Sparrow;

namespace Corax.Querying.Matches
{
    //We should set inner type via generic but since we don't do that in QueryBuilder (we use interfaces all the time) let's skip that. 
    //This should be fixed when we introduce something similar to IL ( RavenDB-19568)
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe struct BoostingMatch : IQueryMatch
    {
        internal IQueryMatch _inner;
        public float BoostFactor;
        public BoostingMatch(Querying.IndexSearcher searcher, in IQueryMatch inner, float boostFactor)
        {
            PortableExceptions.ThrowIf<NotSupportedException>(inner is VectorSearchMatch, $"Boosting the {nameof(VectorSearchMatch)} is not supported yet.");
            
            _inner = inner;
            BoostFactor = boostFactor;
        }

        public long Count => _inner.Count;

        public bool IsBoosting => true;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches) => _inner.Fill(matches);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores, float boostFactor) => _inner.Score(matches, scores, boostFactor * BoostFactor);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor) => _inner.ScoreSorted(matches, scores, boostFactor * BoostFactor);

        public QueryInspectionNode Inspect()
        {
            var inner = _inner.Inspect();
            return new QueryInspectionNode($"{nameof(BoostingMatch)}",
                children: new List<QueryInspectionNode> { inner },
                parameters: new Dictionary<string, string>()
                {
                    { Constants.QueryInspectionNode.IsBoosting, IsBoosting.ToString() },
                    { Constants.QueryInspectionNode.Count, Count.ToString()},
                    { Constants.QueryInspectionNode.BoostFactor, BoostFactor.ToString(CultureInfo.InvariantCulture) }
                })
            {
                IsPostFilter = inner.IsPostFilter
            };
        }
    }
}
