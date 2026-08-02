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

            // Values come from PgSettings so `current_setting('x')` and `SHOW x` can never
            // disagree - see the remarks on that class.
            //
            // An unknown setting returns false (fall through to the next dispatch arm) rather than
            // the 42704 error `SHOW` raises. That asymmetry is deliberate: current_setting can
            // appear anywhere in a larger expression that a later arm may still handle, whereas a
            // bare SHOW is unambiguously a settings lookup with nothing left to try.
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

    // Returns the schema at the head of the effective search_path. SQLAlchemy's PGDialect calls
    // this on every connect (_get_default_schema_name) and aborts the handshake if it fails.
    // We expose a single namespace for user-visible objects - "public", oid 2200 in
    // pg_namespace.csv - so the answer is that constant. It also agrees with what
    // CurrentSettingFunction reports for 'search_path' ("$user", public): the per-user schema
    // doesn't exist here, so "public" is the first entry that resolves.
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

    // pg_table_is_visible(oid) / pg_function_is_visible(oid): does this object's namespace sit on the
    // current search_path - i.e. can the object be referenced by its bare name. SQLAlchemy's
    // get_table_oid() filters pg_class through `pg_table_is_visible(c.oid)`, and it runs BEFORE
    // column reflection, so while this was unimplemented the query was rejected and Superset could
    // not create a dataset even though pg_class already listed the table (Zoho Desk #7031). pgAdmin
    // uses the same predicate.
    //
    // TRUE is the right answer, not a workaround. RavenDB exposes a single schema for user objects -
    // "public", oid 2200 in pg_namespace.csv, the only schema CurrentSchemaFunction ever names - and
    // every pg_class row carries relnamespace 2200, so every relation really is on the search_path.
    // Likewise every pg_proc row is a pg_catalog builtin, and pg_catalog is implicitly searched
    // ahead of the rest, so those are visible too.
    //
    // pg_type_is_visible is deliberately NOT one of these. pg_type also carries the
    // information_schema domain types (cardinal_number, sql_identifier, yes_or_no, ...), whose
    // namespace is not on the search_path, so a blanket TRUE would be a wrong answer for those rows
    // rather than a conservative one.
    internal sealed class PgIsVisibleFunction : ScalarFunction
    {
        private readonly string _name;

        public PgIsVisibleFunction(string name) { _name = name; }

        public override string Name => _name;
        public override string ResultColumnName => _name;
        public override PgType PgType => PgBool.Default;

        public override bool TryEvaluate(IReadOnlyList<object> args, VirtualQueryContext ctx, out object result)
        {
            result = true;
            // Exactly one argument, the object's oid. We don't inspect it (see above), but any other
            // arity is a call we haven't modelled - fall through rather than claim we answered it.
            return args is { Count: 1 };
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
            // A TimeSpan-shaped document field reflects as interval (see CollectionCatalog), so
            // pg_attribute can hand this oid to format_type and it must resolve to a name.
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

            if (TryGetOid(args[0], out var oid) == false)
                return false;

            result = TypeNames.TryGetValue(oid, out var name) ? name : oid.ToString(CultureInfo.InvariantCulture);
            return true;
        }

        private static bool TryGetOid(object value, out long oid)
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
}
