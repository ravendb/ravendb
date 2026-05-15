using Raven.Client.Documents.Indexes;

namespace Raven.Client.Documents.Session
{
    public enum OrderingType
    {
        String,
        Long,
        Double,
        AlphaNumeric
    }

    /// <summary>
    /// Controls where <c>null</c> values are placed in the result of an <c>ORDER BY</c> clause.
    /// </summary>
    /// <remarks>
    /// Per-query null placement (<see cref="First"/> / <see cref="Last"/>) is supported only by the Corax indexing engine.
    /// Queries that specify <see cref="First"/> or <see cref="Last"/> against a Lucene index are rejected.
    /// </remarks>
    public enum NullsOrdering
    {
        /// <summary>
        /// No per-query placement is specified; the index/server configuration decides where nulls go.
        /// </summary>
        Default,

        /// <summary>
        /// Null values appear first in the result, regardless of sort direction.
        /// Supported only by the Corax indexing engine.
        /// </summary>
        First,

        /// <summary>
        /// Null values appear last in the result, regardless of sort direction.
        /// Supported only by the Corax indexing engine.
        /// </summary>
        Last
    }

    internal static class OrderingUtil
    {
        public static OrderingType GetOrderingFromRangeType(RangeType rangeType)
        {
            var ordering = OrderingType.String;

            switch (rangeType)
            {
                case RangeType.Double:
                    ordering = OrderingType.Double;
                    break;
                case RangeType.Long:
                    ordering = OrderingType.Long;
                    break;
            }

            return ordering;
        }
    }
}
