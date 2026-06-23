using System;

namespace Raven.Server.Integrations.PostgreSQL
{
    // PG-facing names for the synthetic id / json columns. RQL uses `id()` / `json()` internally
    // but those parenthesised names can't parse as bare PG identifiers, so we expose `id` / `json`
    // at the PG endpoint surface and still recognize legacy `id()` / `json()` references for
    // backward compatibility with cached PowerBI metadata.
    internal static class PgSyntheticColumns
    {
        public const string DocumentId = "id";
        public const string Json = "json";

        // Legacy names still emitted by PowerBI for tables whose metadata was cached before
        // the rename. Recognized in SQL projections and routed back to the same RQL functions.
        public const string LegacyDocumentId = "id()";
        public const string LegacyJson = "json()";

        // True for `id` (current) or `id()` (legacy).
        public static bool IsDocumentIdColumn(string name) =>
            string.Equals(name, DocumentId, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(name, LegacyDocumentId, StringComparison.OrdinalIgnoreCase);

        // True for `json` (current) or `json()` (legacy).
        public static bool IsJsonColumn(string name) =>
            string.Equals(name, Json, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(name, LegacyJson, StringComparison.OrdinalIgnoreCase);

        // True if the name is either synthetic column in either form.
        public static bool IsSyntheticColumn(string name) =>
            IsDocumentIdColumn(name) || IsJsonColumn(name);
    }
}
