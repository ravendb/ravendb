using System;
using System.Collections.Generic;
using Raven.Server.Documents;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog
{
    internal sealed class VirtualQueryContext
    {
        public DocumentDatabase Database { get; init; }

        // The PG-protocol username that authenticated this connection. Surfaced via
        // current_user() / session_user() / the single row of pg_roles. May be null for tests or
        // contexts where no real connection is involved.
        public string Username { get; init; }

        // Equality predicates pre-extracted from the current SELECT's WHERE clause, keyed by
        // column name (case-insensitive). Set by the interpreter before each TryExecuteAsRows run
        // so virtual tables can scope their enumeration without re-walking the SQL AST. Example:
        // information_schema.columns reads Predicates["table_name"] to introspect just one
        // collection instead of every one. Save/restore on recursive sub-FROM calls — the inner
        // SELECT has its own WHERE and predicate set.
        public IReadOnlyDictionary<string, object> Predicates { get; set; }
            = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        // Materialized Common Table Expressions registered by an enclosing WITH clause. The key
        // is the CTE name (e.g. "cte"); the value is its columns + rows. JoinExecutor consults
        // this map before falling back to PgVirtualDatabase when resolving a RangeVar reference,
        // so the inner SELECT can use the CTE as if it were a table. Save/restore across nested
        // WITH clauses.
        public Dictionary<string, MaterializedCte> Ctes { get; set; }
    }

    internal sealed class MaterializedCte
    {
        public string Name { get; init; }
        public IReadOnlyList<PgVirtualColumn> Columns { get; init; }
        public List<object[]> Rows { get; init; }
    }
}
