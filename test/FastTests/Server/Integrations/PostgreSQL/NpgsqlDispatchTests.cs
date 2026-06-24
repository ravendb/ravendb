using Raven.Server.Integrations.PostgreSQL.VirtualCatalog;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL
{
    // Verifies the Npgsql type-catalog probes each client version sends on startup all reach
    // PgVirtualInterpreter and return a well-formed but empty rowset (columns present, zero rows).
    public sealed class NpgsqlDispatchTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        // Npgsql 5.0.0+ — "Load enum fields" probe.
        private const string Npgsql5EnumTypesQuery =
            "-- Load enum fields\nSELECT pg_type.oid, enumlabel\nFROM pg_enum\nJOIN pg_type ON pg_type.oid=enumtypid\nORDER BY oid, enumsortorder";

        // Npgsql 4.0.0–4.1.1 — same probe with a C-style block comment.
        private const string EnumTypesQuery =
            "/*** Load enum fields ***/\nSELECT pg_type.oid, enumlabel\nFROM pg_enum\nJOIN pg_type ON pg_type.oid=enumtypid\nORDER BY oid, enumsortorder";

        // Npgsql 5.0.0+ — "Load field definitions for composite types" probe.
        private const string Npgsql5CompositeTypesQuery =
            "-- Load field definitions for (free-standing) composite types\nSELECT typ.oid, att.attname, att.atttypid\nFROM pg_type AS typ\nJOIN pg_namespace AS ns ON (ns.oid = typ.typnamespace)\nJOIN pg_class AS cls ON (cls.oid = typ.typrelid)\nJOIN pg_attribute AS att ON (att.attrelid = typ.typrelid)\nWHERE\n  (typ.typtype = 'c' AND cls.relkind='c') AND\n  attnum > 0 AND     -- Don't load system attributes\n  NOT attisdropped\nORDER BY typ.oid, att.attnum";

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Npgsql5_enum_types_probe_returns_empty_rowset()
        {
            // pg_enum is empty in our virtual catalog → INNER JOIN yields no rows. Column metadata
            // must still appear so the Npgsql client can move past this probe cleanly.
            Assert.True(PgVirtualInterpreter.TryExecute(Npgsql5EnumTypesQuery, new VirtualQueryContext(), out var table));
            Assert.Equal(2, table.Columns.Count);
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Block_comment_enum_types_probe_returns_empty_rowset()
        {
            Assert.True(PgVirtualInterpreter.TryExecute(EnumTypesQuery, new VirtualQueryContext(), out var table));
            Assert.Equal(2, table.Columns.Count);
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Npgsql5_composite_types_probe_returns_empty_rowset()
        {
            // pg_attribute is empty → no rows. Three projected columns.
            Assert.True(PgVirtualInterpreter.TryExecute(Npgsql5CompositeTypesQuery, new VirtualQueryContext(), out var table));
            Assert.Equal(3, table.Columns.Count);
            Assert.Empty(table.Data);
        }
    }
}
