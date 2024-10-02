using System.Collections.Generic;
using Raven.Client.Documents.Queries.Timings;
using Raven.Server.Utils.Stats;
using QueryInspectionNode = Corax.Querying.Matches.Meta.QueryInspectionNode;

namespace Raven.Server.Documents.Queries.Timings
{
    public sealed class QueryTimingsScope : StatsScope<object, QueryTimingsScope>
    {
        /// <summary>
        /// Used for nameof
        /// </summary>
        public static class Names
        {
            public static string Staleness;
            public static string Query;
            public static string Lucene;
            public static string Corax;
            public static string Highlightings;
            public static string Explanations;
            public static string JavaScript;
            public static string Load;
            public static string Storage;
            public static string Projection;
            public static string Retriever;
            public static string Fill;
            public static string Gather;
            public static string Includes;
            public static string Setup;
            public static string Optimizer;
            public static string Filter;
            public static string Terms;
            public static string AggregateBy;

            public static string Execute;
            public static string Cluster;
            public static string Reduce;
            public static string Paging;
        }

        private QueryInspectionNode _queryPlan;
        public void SetQueryPlan(QueryInspectionNode plan)
        {
            _queryPlan = plan;
            if (_base != null)
            {
                _base.QueryPlan = plan;
            }
        }

        public QueryTimingsScope(bool start = true) : base(null, start)
        {
        }

        protected override QueryTimingsScope OpenNewScope(object stats, bool start)
        {
            return new QueryTimingsScope(start);
        }

        public QueryTimings ToTimings()
        {
            QueryTimings timings = _base ?? new QueryTimings();

            timings.DurationInMs = (long)Duration.TotalMilliseconds;

            if (Scopes != null)
            {
                foreach (var scope in Scopes)
                {
                    timings.Timings ??= new SortedDictionary<string, QueryTimings>();

                    timings.Timings[scope.Key] = scope.Value.ToTimings();

                    if (scope.Value._queryPlan != null)
                    {
                        timings.QueryPlan ??= scope.Value._queryPlan;
                    }
                }
            }

            timings.QueryPlan ??= _queryPlan;

            return timings;
        }

        private QueryTimings _base;

        public void WithBase(QueryTimings timings)
        {
            _base = timings;
        }
    }
}
