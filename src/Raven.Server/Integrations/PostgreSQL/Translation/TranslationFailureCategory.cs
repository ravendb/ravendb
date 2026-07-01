using System;

namespace Raven.Server.Integrations.PostgreSQL.Translation
{
    // Why the SQL to RQL translator gave up on a query shape. Carried on PgTranslationException so the
    // dispatcher can later short-circuit UnhandledQueryDiagnoser instead of re-parsing.
    internal enum TranslationFailureCategory
    {
        // Catch-all for ad-hoc messages that haven't been categorized yet.
        Other = 0,
        Join = 1,
        Aggregate = 2,
        GroupBy = 3,
        Distinct = 4,
        OrderBy = 5,
        // SELECT projection shape we don't support (function calls in projection, expressions,
        // etc. that don't reduce to "column" or "aggregate(column)").
        SelectProjection = 6,
        // Mixed aggregate + non-aggregated columns without a GROUP BY anchor — SQL forbids this,
        // and our translator rejects it with a clearer message than PG would.
        MixedAggregateAndNonAggregate = 7,
    }

    // Carries the failure category to TryParse's catch. NotSupportedException base keeps the
    // existing catch working for throws not yet migrated to set a category.
    internal sealed class PgTranslationException : NotSupportedException
    {
        public TranslationFailureCategory Category { get; }

        public PgTranslationException(TranslationFailureCategory category, string message)
            : base(message)
        {
            Category = category;
        }
    }
}
