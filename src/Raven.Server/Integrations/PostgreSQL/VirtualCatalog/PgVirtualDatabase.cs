using System;
using System.Collections.Generic;
using Raven.Server.Integrations.PostgreSQL.VirtualCatalog.Tables;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog
{
    internal static class PgVirtualDatabase
    {
        private static readonly Dictionary<(string Schema, string Table), PgVirtualTable> Tables =
            new(new SchemaTableKeyComparer());

        private static readonly Dictionary<string, ScalarFunction> Functions =
            new(StringComparer.OrdinalIgnoreCase);

        // Schema search order for unqualified table references (matches PG default search_path).
        private static readonly string[] UnqualifiedSchemaSearch = { "pg_catalog", "information_schema", "public" };

        static PgVirtualDatabase()
        {
            RegisterTable(new InformationSchemaCharacterSetsTable());
            RegisterTable(new InformationSchemaTablesTable());
            RegisterTable(new InformationSchemaColumnsTable());
            RegisterTable(new InformationSchemaTableConstraintsTable());
            RegisterTable(new InformationSchemaKeyColumnUsageTable());
            RegisterTable(EmptyCatalogTables.InformationSchemaReferentialConstraints);
            RegisterTable(EmptyCatalogTables.InformationSchemaViews);

            RegisterTable(new PgCatalogPgTypeTable());
            RegisterTable(new PgCatalogPgProcTable());
            RegisterTable(new PgCatalogPgRangeTable());
            RegisterTable(EmptyCatalogTables.PgEnum);
            RegisterTable(new PgCatalogPgClassTable());
            RegisterTable(EmptyCatalogTables.PgAttribute);
            RegisterTable(new PgCatalogPgNamespaceTable());
            RegisterTable(EmptyCatalogTables.PgExtension);
            RegisterTable(EmptyCatalogTables.PgReplicationSlots);
            RegisterTable(EmptyCatalogTables.PgStatGssapi);
            RegisterTable(new PgCatalogPgDatabaseTable());
            RegisterTable(new PgCatalogPgRolesTable());
            RegisterTable(EmptyCatalogTables.PgAuthMembers);
            RegisterTable(EmptyCatalogTables.PgTablespace);
            RegisterTable(EmptyCatalogTables.PgShdescription);
            RegisterTable(EmptyCatalogTables.PgDescription);

            RegisterFunction(new VersionFunction());
            RegisterFunction(new CurrentSettingFunction());
            RegisterFunction(new CurrentDatabaseFunction());
            RegisterFunction(new PgEncodingToCharFunction());
            RegisterFunction(new HasDatabasePrivilegeFunction());
            RegisterFunction(new HasSchemaPrivilegeFunction());
            RegisterFunction(new PgBackendPidFunction());
            RegisterFunction(new CurrentUserFunction("current_user"));
            RegisterFunction(new CurrentUserFunction("session_user"));
            RegisterFunction(new CurrentUserFunction("user"));
            RegisterFunction(new PgGetUserByIdFunction());
            RegisterFunction(new ArrayToStringFunction());
            RegisterFunction(new FormatTypeFunction());
        }

        public static bool TryGetTable(string schema, string table, out PgVirtualTable virtualTable)
        {
            if (string.IsNullOrEmpty(schema) == false)
                return Tables.TryGetValue((schema, table ?? string.Empty), out virtualTable);
            
            foreach (var s in UnqualifiedSchemaSearch)
            {
                if (Tables.TryGetValue((s, table ?? string.Empty), out virtualTable))
                    return true;
            }

            virtualTable = null;
            return false;
        }

        public static bool TryGetFunction(string name, out ScalarFunction function)
            => Functions.TryGetValue(name ?? string.Empty, out function);

        private static void RegisterTable(PgVirtualTable table)
            => Tables[(table.SchemaName, table.TableName)] = table;

        private static void RegisterFunction(ScalarFunction function)
            => Functions[function.Name] = function;

        private sealed class SchemaTableKeyComparer : IEqualityComparer<(string Schema, string Table)>
        {
            public bool Equals((string Schema, string Table) x, (string Schema, string Table) y)
                => StringComparer.OrdinalIgnoreCase.Equals(x.Schema, y.Schema) &&
                   StringComparer.OrdinalIgnoreCase.Equals(x.Table, y.Table);

            public int GetHashCode((string Schema, string Table) obj)
                => HashCode.Combine(
                    StringComparer.OrdinalIgnoreCase.GetHashCode(obj.Schema ?? string.Empty),
                    StringComparer.OrdinalIgnoreCase.GetHashCode(obj.Table ?? string.Empty));
        }
    }
}
