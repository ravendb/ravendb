using System;
using System.Collections.Generic;
using System.Globalization;
using Raven.Server.Integrations.PostgreSQL.Types;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog
{
    internal abstract class ScalarFunction
    {
        public abstract string Name { get; }
        public abstract string ResultColumnName { get; }
        public abstract PgType PgType { get; }

        // ctx allows context-aware functions (current_database needs ctx.Database.Name). Pre-existing
        // ones that don't need ctx just ignore it.
        public abstract bool TryEvaluate(IReadOnlyList<object> args, VirtualQueryContext ctx, out object result);
    }

    internal sealed class VersionFunction : ScalarFunction
    {
        private const string Version =
            "PostgreSQL 13.3, compiled by Visual C++ build 1914, 64-bit";

        public override string Name => "version";
        public override string ResultColumnName => "version";
        public override PgType PgType => PgText.Default;

        public override bool TryEvaluate(IReadOnlyList<object> args, VirtualQueryContext ctx, out object result)
        {
            if (args is { Count: > 0 })
            {
                result = null;
                return false;
            }

            result = Version;
            return true;
        }
    }

    internal sealed class CurrentSettingFunction : ScalarFunction
    {
        public override string Name => "current_setting";
        public override string ResultColumnName => "current_setting";
        public override PgType PgType => PgText.Default;

        public override bool TryEvaluate(IReadOnlyList<object> args, VirtualQueryContext ctx, out object result)
        {
            result = null;

            if (args is not { Count: 1 })
                return false;

            if (args[0] is not string setting)
                return false;

            if (PgSettings.TryGetValue(setting, out var value) == false)
                return false;

            result = value;
            return true;
        }
    }

    // Returns the role name for a given role oid. We only model one user, so return
    // ctx.Username for any oid — pgAdmin only uses this for cosmetic owner display.
    internal sealed class PgGetUserByIdFunction : ScalarFunction
    {
        public override string Name => "pg_get_userbyid";
        public override string ResultColumnName => "pg_get_userbyid";
        public override PgType PgType => PgName.Default;

        public override bool TryEvaluate(IReadOnlyList<object> args, VirtualQueryContext ctx, out object result)
        {
            result = ctx?.Username ?? string.Empty;
            return args is { Count: 1 };
        }
    }

    // Concatenates array elements with a delimiter (PG: `array_to_string(arr, delimiter)`).
    // Returns NULL when the array is NULL, matches PG semantics.
    internal sealed class ArrayToStringFunction : ScalarFunction
    {
        public override string Name => "array_to_string";
        public override string ResultColumnName => "array_to_string";
        public override PgType PgType => PgText.Default;

        public override bool TryEvaluate(IReadOnlyList<object> args, VirtualQueryContext ctx, out object result)
        {
            result = null;
            if (args is not { Count: >= 2 and <= 3 })
                return false;
            if (args[0] == null)
                return true; // NULL array → NULL result
            var delimiter = args[1]?.ToString() ?? string.Empty;

            if (args[0] is System.Collections.IEnumerable enumerable)
            {
                var sb = new System.Text.StringBuilder();
                var first = true;
                foreach (var item in enumerable)
                {
                    if (item == null) continue; // PG semantics: NULL elements skipped without the optional 3rd arg.
                    if (first == false) sb.Append(delimiter);
                    sb.Append(item);
                    first = false;
                }
                result = sb.ToString();
                return true;
            }
            // Single scalar: just emit it.
            result = args[0].ToString();
            return true;
        }
    }

    // Returns the authenticated PG-protocol username for the connection. pgAdmin's role probe
    // uses this in `WHERE rolname = current_user` to find the connected role in pg_roles.
    // Also covers `session_user` since we don't distinguish the two (no SET ROLE / SESSION
    // AUTHORIZATION semantics on this surface).
    internal sealed class CurrentUserFunction : ScalarFunction
    {
        private readonly string _aliasName;
        public CurrentUserFunction(string name = "current_user") { _aliasName = name; }

        public override string Name => _aliasName;
        public override string ResultColumnName => _aliasName;
        public override PgType PgType => PgName.Default;

        public override bool TryEvaluate(IReadOnlyList<object> args, VirtualQueryContext ctx, out object result)
        {
            result = null;
            if (args is { Count: > 0 })
                return false;
            if (string.IsNullOrEmpty(ctx?.Username))
                return false;
            result = ctx.Username;
            return true;
        }
    }

    // Returns the active RavenDB database name. Used by pgAdmin's
    // `WHERE db.datname = current_database()` probe against pg_database.
    internal sealed class CurrentDatabaseFunction : ScalarFunction
    {
        public override string Name => "current_database";
        public override string ResultColumnName => "current_database";
        public override PgType PgType => PgName.Default;

        public override bool TryEvaluate(IReadOnlyList<object> args, VirtualQueryContext ctx, out object result)
        {
            result = null;
            if (args is { Count: > 0 })
                return false;
            if (ctx?.Database == null)
                return false;
            result = ctx.Database.Name;
            return true;
        }
    }

    internal sealed class CurrentSchemaFunction : ScalarFunction
    {
        private const string Schema = "public";

        public override string Name => "current_schema";
        public override string ResultColumnName => "current_schema";
        public override PgType PgType => PgName.Default;

        public override bool TryEvaluate(IReadOnlyList<object> args, VirtualQueryContext ctx, out object result)
        {
            result = null;
            if (args is { Count: > 0 })
                return false;
            result = Schema;
            return true;
        }
    }

    // Tables and functions are always visible: one schema, and every pg_proc row is a pg_catalog builtin.
    internal sealed class PgIsVisibleFunction : ScalarFunction
    {
        private readonly string _name;

        public PgIsVisibleFunction(string name) { _name = name; }

        public override string Name => _name;
        public override string ResultColumnName => _name;
        public override PgType PgType => PgBool.Default;

        public override bool TryEvaluate(IReadOnlyList<object> args, VirtualQueryContext ctx, out object result)
        {
            result = null;
            if (args is not { Count: 1 })
                return false;

            if (args[0] == null)
                return true; // NULL oid -> NULL.

            result = true;
            return true;
        }
    }

    // Not constant true like the above: the information_schema domain types are off the search_path.
    internal sealed class PgTypeIsVisibleFunction : ScalarFunction
    {
        private static readonly HashSet<string> SearchedSchemas =
            new(StringComparer.OrdinalIgnoreCase) { "pg_catalog", "public" };

        // Built once: evaluated per pg_type row, so re-scanning per call would be quadratic.
        private static readonly Lazy<Dictionary<long, string>> SchemaByTypeOid = new(BuildSchemaByTypeOid);

        public override string Name => "pg_type_is_visible";
        public override string ResultColumnName => "pg_type_is_visible";
        public override PgType PgType => PgBool.Default;

        public override bool TryEvaluate(IReadOnlyList<object> args, VirtualQueryContext ctx, out object result)
        {
            result = null;
            if (args is not { Count: 1 })
                return false;

            if (args[0] == null)
                return true; // NULL oid -> NULL.

            if (CatalogOid.TryRead(args[0], out var oid) == false)
                return false;

            // Unknown oid -> NULL, not false: PG checks the syscache before calling TypeIsVisible.
            if (SchemaByTypeOid.Value.TryGetValue(oid, out var schema) == false)
                return true;

            result = SearchedSchemas.Contains(schema);
            return true;
        }

        private static Dictionary<long, string> BuildSchemaByTypeOid()
        {
            var namespaceNames = ReadCatalogColumn("pg_namespace", "oid", "nspname");
            var typeNamespaces = ReadCatalogColumn("pg_type", "oid", "typnamespace");

            var result = new Dictionary<long, string>(typeNamespaces.Count);
            foreach (var (typeOid, rawNamespace) in typeNamespaces)
            {
                // The empty string never matches a searched schema.
                var schema = string.Empty;
                if (CatalogOid.TryRead(rawNamespace, out var namespaceOid) &&
                    namespaceNames.TryGetValue(namespaceOid, out var name))
                    schema = name as string ?? string.Empty;

                result[typeOid] = schema;
            }

            return result;
        }

        // CSV-backed catalogs serve the same cached rows to every query, so ctx is unused.
        private static Dictionary<long, object> ReadCatalogColumn(string tableName, string keyColumn, string valueColumn)
        {
            var result = new Dictionary<long, object>();
            if (PgVirtualDatabase.TryGetTable("pg_catalog", tableName, out var table) == false)
                return result;

            var keyIndex = ColumnIndex(table, keyColumn);
            var valueIndex = ColumnIndex(table, valueColumn);
            if (keyIndex < 0 || valueIndex < 0)
                return result;

            foreach (var row in table.EnumerateRows(ctx: null))
            {
                if (CatalogOid.TryRead(row[keyIndex], out var key))
                    result[key] = row[valueIndex];
            }

            return result;
        }

        private static int ColumnIndex(PgVirtualTable table, string name)
        {
            for (int i = 0; i < table.Columns.Count; i++)
            {
                if (string.Equals(table.Columns[i].Name, name, StringComparison.OrdinalIgnoreCase))
                    return i;
            }

            return -1;
        }
    }

    // Oids arrive as int (CSV rows), long (integer literals) or string (quoted literals).
    internal static class CatalogOid
    {
        public static bool TryRead(object value, out long oid)
        {
            switch (value)
            {
                case long l: oid = l; return true;
                case int i: oid = i; return true;
                case short s: oid = s; return true;
                case string str when long.TryParse(str, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed): oid = parsed; return true;
                default: oid = 0; return false;
            }
        }
    }

    // Maps a PG encoding integer to its name. We always serve UTF8 (encoding id 6 in PG); for any
    // input we return "UTF8" since that's the only encoding our wire format produces.
    internal sealed class PgEncodingToCharFunction : ScalarFunction
    {
        public override string Name => "pg_encoding_to_char";
        public override string ResultColumnName => "pg_encoding_to_char";
        public override PgType PgType => PgName.Default;

        public override bool TryEvaluate(IReadOnlyList<object> args, VirtualQueryContext ctx, out object result)
        {
            result = "UTF8";
            return args is { Count: 1 };
        }
    }

    // pgAdmin uses this to decide whether to show "Create object" actions. Our PG endpoint is
    // read-only, so technically the user has no real privileges — but pgAdmin works in degraded
    // mode if we say no. Returning true keeps the UI usable; any DDL actually attempted later
    // would fail at the SQL-handling layer anyway (we don't implement DDL).
    internal sealed class HasDatabasePrivilegeFunction : ScalarFunction
    {
        public override string Name => "has_database_privilege";
        public override string ResultColumnName => "has_database_privilege";
        public override PgType PgType => PgBool.Default;

        public override bool TryEvaluate(IReadOnlyList<object> args, VirtualQueryContext ctx, out object result)
        {
            result = true;
            return args is { Count: >= 1 and <= 3 };
        }
    }

    // pgAdmin's schema-tree probe calls has_schema_privilege(oid, 'CREATE'/'USAGE') per namespace
    // row to populate the can_create / has_usage flags it shows in the UI. Same rationale as
    // has_database_privilege: returning true keeps the UI usable; any actual DDL is rejected
    // elsewhere. Signature variants: (schema, privilege) | (user, schema, privilege).
    internal sealed class HasSchemaPrivilegeFunction : ScalarFunction
    {
        public override string Name => "has_schema_privilege";
        public override string ResultColumnName => "has_schema_privilege";
        public override PgType PgType => PgBool.Default;

        public override bool TryEvaluate(IReadOnlyList<object> args, VirtualQueryContext ctx, out object result)
        {
            result = true;
            return args is { Count: >= 2 and <= 3 };
        }
    }

    // Returns the server process ID for the current backend. pgAdmin uses this to filter
    // pg_stat_*/pg_locks views to just the current connection. We don't model multiple PG
    // backends, so any stable integer is fine; the host process id is a reasonable choice.
    internal sealed class PgBackendPidFunction : ScalarFunction
    {
        private static readonly long Pid = System.Environment.ProcessId;

        public override string Name => "pg_backend_pid";
        public override string ResultColumnName => "pg_backend_pid";
        public override PgType PgType => PgInt4.Default;

        public override bool TryEvaluate(IReadOnlyList<object> args, VirtualQueryContext ctx, out object result)
        {
            result = Pid;
            return args is null or { Count: 0 };
        }
    }

    // format_type(oid, typmod): canonical PG type name (e.g. 23 → "integer", 25 → "text").
    // Used in pgAdmin's `SELECT oid, format_type(oid, NULL) AS typname FROM pg_type WHERE oid =
    // ANY($1)` probe to pretty-print result-set column types in the data grid.
    internal sealed class FormatTypeFunction : ScalarFunction
    {
        // oid → format_type display name for the builtin types the bridge surfaces / clients probe.
        private static readonly Dictionary<long, string> TypeNames = new()
        {
            [16] = "boolean",
            [17] = "bytea",
            [18] = "\"char\"",
            [19] = "name",
            [20] = "bigint",
            [21] = "smallint",
            [23] = "integer",
            [25] = "text",
            [26] = "oid",
            [114] = "json",
            [700] = "real",
            [701] = "double precision",
            [1042] = "character",
            [1043] = "character varying",
            [1082] = "date",
            [1083] = "time without time zone",
            [1114] = "timestamp without time zone",
            [1184] = "timestamp with time zone",
            [1186] = "interval",
            [1700] = "numeric",
            [2950] = "uuid",
            [3802] = "jsonb",
        };

        public override string Name => "format_type";
        public override string ResultColumnName => "format_type";
        public override PgType PgType => PgText.Default;

        public override bool TryEvaluate(IReadOnlyList<object> args, VirtualQueryContext ctx, out object result)
        {
            result = null;
            // format_type takes 1 or 2 args: (oid) or (oid, typmod). Anything else is malformed.
            if (args is not { Count: >= 1 and <= 2 })
                return false;

            // A NULL oid → NULL result (matches PG). Otherwise resolve the oid to a type name.
            if (args[0] == null)
                return true;

            if (CatalogOid.TryRead(args[0], out var oid) == false)
                return false;

            result = TypeNames.TryGetValue(oid, out var name) ? name : oid.ToString(CultureInfo.InvariantCulture);
            return true;
        }
    }
}
