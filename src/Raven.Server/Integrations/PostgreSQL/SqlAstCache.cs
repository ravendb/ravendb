using PgSqlParser;

namespace Raven.Server.Integrations.PostgreSQL
{
    // Per-thread, single-entry cache around pgsqlparser so PgQuery.CreateInstance's dispatch arms
    // (PowerBI / interpreter / translator / diagnoser) don't each re-parse the same query text.
    // Keyed by reference-equality on queryText: dispatch is synchronous on one thread and threads
    // the same string instance through, the next query overwrites the single slot (bounded, no leak),
    // and the cached result is read-only, so sharing it across arms is safe.
    internal static class SqlAstCache
    {
        [System.ThreadStatic]
        private static string _lastText;

        [System.ThreadStatic]
        private static Result<ParseResult> _lastResult;

        [System.ThreadStatic]
        private static bool _hasCached;

        public static Result<ParseResult> GetOrParse(string queryText)
        {
            if (queryText != null && _hasCached && ReferenceEquals(_lastText, queryText))
                return _lastResult;

            var result = Parser.Parse(queryText);
            _lastText = queryText;
            _lastResult = result;
            _hasCached = true;
            return result;
        }
    }
}
