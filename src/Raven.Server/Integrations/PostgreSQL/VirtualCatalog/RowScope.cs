using System;
using System.Collections.Generic;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog
{
    // Holds the current row across one or more joined sources, keyed by alias. Lookups accept
    // either a bare column name (searches every source, first match wins) or "<alias>.<column>"
    // (resolved against the matching source). A null Row slot means the source contributed no
    // row to this join combination — its columns evaluate to null (LEFT OUTER semantics).
    internal sealed class RowScope
    {
        private readonly struct Source
        {
            public readonly string Alias;
            public readonly IReadOnlyList<PgVirtualColumn> Columns;
            public readonly object[] Row;

            public Source(string alias, IReadOnlyList<PgVirtualColumn> columns, object[] row)
            {
                Alias = alias;
                Columns = columns;
                Row = row;
            }
        }

        private readonly List<Source> _sources;
        public RowScope Parent { get; }

        private RowScope(List<Source> sources, RowScope parent = null)
        {
            _sources = sources;
            Parent = parent;
        }

        public static RowScope Single(string alias, IReadOnlyList<PgVirtualColumn> columns, object[] row)
            => new(new List<Source> { new(alias, columns, row) });

        public static RowScopeBuilder Builder() => new();

        // Returns a new scope whose local sources are the same as this one's, but with the given
        // parent attached. Used to make an outer query's row visible to a subquery's evaluator
        // (correlated subqueries) without mutating either side.
        public RowScope WithParent(RowScope parent) => new(_sources, parent);

        public bool TryLookup(IReadOnlyList<string> fieldPath, out object value)
        {
            value = null;
            if (fieldPath == null || fieldPath.Count == 0)
                return false;

            if (TryLookupLocal(fieldPath, out value))
                return true;

            // Correlated lookup: not found locally, ask the outer scope.
            return Parent != null && Parent.TryLookup(fieldPath, out value);
        }

        private bool TryLookupLocal(IReadOnlyList<string> fieldPath, out object value)
        {
            value = null;

            var columnName = fieldPath[^1];
            string qualifier = null;
            if (fieldPath.Count >= 2)
                qualifier = fieldPath[^2];

            if (string.IsNullOrWhiteSpace(qualifier) == false)
            {
                foreach (var src in _sources)
                {
                    if (string.Equals(src.Alias, qualifier, StringComparison.OrdinalIgnoreCase) == false)
                        continue;
                    return TryLookupInSource(in src, columnName, out value);
                }
                return false;
            }

            foreach (var src in _sources)
            {
                if (TryLookupInSource(in src, columnName, out value))
                    return true;
            }
            return false;
        }

        private static bool TryLookupInSource(in Source src, string columnName, out object value)
        {
            for (int i = 0; i < src.Columns.Count; i++)
            {
                if (string.Equals(src.Columns[i].Name, columnName, StringComparison.OrdinalIgnoreCase))
                {
                    value = src.Row == null ? null : src.Row[i];
                    return true;
                }
            }
            value = null;
            return false;
        }

        public sealed class RowScopeBuilder
        {
            private readonly List<Source> _sources = new();

            public RowScopeBuilder Add(string alias, IReadOnlyList<PgVirtualColumn> columns, object[] row)
            {
                _sources.Add(new Source(alias, columns, row));
                return this;
            }

            public RowScope Build() => new(_sources);
        }
    }
}
