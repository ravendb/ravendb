using System;
using System.Collections.Generic;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog.Tables
{
    // Concrete row-less catalog table - used for catalog views where the column shape is part of
    // the contract (clients like pgAdmin and Microsoft Fabric introspect them) but RavenDB has
    // no rows to expose.
    internal sealed class EmptyCatalogTable : PgVirtualTable
    {
        public EmptyCatalogTable(string schemaName, string tableName, params PgVirtualColumn[] columns)
        {
            SchemaName = schemaName;
            TableName = tableName;
            Columns = columns;
        }

        public override string SchemaName { get; }
        public override string TableName { get; }
        public override IReadOnlyList<PgVirtualColumn> Columns { get; }
        public override bool IsAlwaysEmpty => true;
        public override IEnumerable<object[]> EnumerateRows(VirtualQueryContext ctx) => Array.Empty<object[]>();
    }

    // Everything below is a row-less catalog view.
    internal static class EmptyCatalogTables
    {
        // RavenDB has no views, so this is always empty. Microsoft Fabric's Copy Job "Choose data"
        // picker UNIONs `information_schema.tables` and `information_schema.views`, so this must be
        // registered even though it's empty. Column set matches the SQL standard's `views`.
        public static EmptyCatalogTable InformationSchemaViews => new("information_schema", "views",
            new("table_catalog",          PgName.Default,    PgFormat.Text),
            new("table_schema",           PgName.Default,    PgFormat.Text),
            new("table_name",             PgName.Default,    PgFormat.Text),
            new("view_definition",        PgText.Default,    PgFormat.Text),
            new("check_option",           PgVarchar.Default, PgFormat.Text),
            new("is_updatable",           PgVarchar.Default, PgFormat.Text),
            new("is_insertable_into",     PgVarchar.Default, PgFormat.Text),
            new("is_trivially_updatable", PgVarchar.Default, PgFormat.Text));

        // RavenDB has no foreign-key / referential constraints - cross-document links are modeled via
        // document IDs and `load`, not FKs. Clients introspect this view to discover relationships;
        // empty means "none". Column set matches the SQL standard's `referential_constraints`.
        public static EmptyCatalogTable InformationSchemaReferentialConstraints => new("information_schema", "referential_constraints",
            new("constraint_catalog",        PgName.Default, PgFormat.Text),
            new("constraint_schema",         PgName.Default, PgFormat.Text),
            new("constraint_name",           PgName.Default, PgFormat.Text),
            new("unique_constraint_catalog", PgName.Default, PgFormat.Text),
            new("unique_constraint_schema",  PgName.Default, PgFormat.Text),
            new("unique_constraint_name",    PgName.Default, PgFormat.Text),
            new("match_option",              PgText.Default, PgFormat.Text),
            new("update_rule",               PgText.Default, PgFormat.Text),
            new("delete_rule",               PgText.Default, PgFormat.Text));

        // Empty pg_catalog sources for shapes the interpreter doesn't read from real data.
        public static EmptyCatalogTable PgEnum => new("pg_catalog", "pg_enum",
            new("oid",           PgOid.Default,    PgFormat.Text),
            new("enumtypid",     PgOid.Default,    PgFormat.Text),
            new("enumlabel",     PgName.Default,   PgFormat.Text),
            new("enumsortorder", PgFloat4.Default, PgFormat.Text));

        // Column defaults. RavenDB has none - a document either carries a field or it doesn't - so
        // an empty table is the correct answer here, not a missing one.
        //
        // SQLAlchemy's get_columns() reads the default of every column through a correlated subquery
        // over this table: `SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_attrdef d WHERE
        // d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef`. Registering the table is
        // all that takes - with no rows, the projection is never evaluated, so the subquery yields
        // SQL NULL and pg_get_expr is never called. We deliberately don't implement it: rendering a
        // stored expression tree back to source text is not something we could do honestly.
        //
        // adbin is really pg_node_tree, an internal type we don't model; it's declared as text so a
        // reference to it resolves.
        public static EmptyCatalogTable PgAttrdef => new("pg_catalog", "pg_attrdef",
            new("oid",     PgOid.Default,  PgFormat.Text),
            new("adrelid", PgOid.Default,  PgFormat.Text),
            new("adnum",   PgInt2.Default, PgFormat.Text),
            new("adbin",   PgText.Default, PgFormat.Text));

        // Sequences. RavenDB has none - document ids come from the change vector / identity
        // machinery, not from a sequence relation - so an empty table is the correct answer.
        //
        // SQLAlchemy's get_columns() reads each column's identity metadata through a scalar
        // subquery over this table: `SELECT json_build_object(...) FROM pg_sequence s JOIN pg_class
        // c ON s.seqrelid = c."oid" WHERE ... AND s.seqrelid = pg_get_serial_sequence(...)`. It was
        // the last unregistered table in SQL_COLS, and one unregistered table rejects the whole
        // statement - so column reflection got nothing back even though every other element of it
        // (format_type, the pg_attrdef default lookup, the pg_description join) already resolved.
        //
        // With no rows the join is empty, so nothing inside the subquery is ever evaluated: neither
        // json_build_object() in its projection nor pg_get_serial_sequence() and the ::regclass
        // casts in its WHERE. We deliberately don't implement any of them - we'd only be inventing
        // identity metadata for columns that have none. The subquery yields SQL NULL, which is
        // precisely "this column is not an identity column".
        //
        // The columns are the ones the subquery reads; real pg_sequence also carries seqtypid,
        // which nothing asks us for.
        public static EmptyCatalogTable PgSequence => new("pg_catalog", "pg_sequence",
            new("seqrelid",     PgOid.Default,  PgFormat.Text),
            new("seqstart",     PgInt8.Default, PgFormat.Text),
            new("seqincrement", PgInt8.Default, PgFormat.Text),
            new("seqmin",       PgInt8.Default, PgFormat.Text),
            new("seqmax",       PgInt8.Default, PgFormat.Text),
            new("seqcache",     PgInt8.Default, PgFormat.Text),
            new("seqcycle",     PgBool.Default, PgFormat.Text));

        // RavenDB has no PG extensions; an empty table lets pgAdmin's `count(extname)` probe return 0.
        public static EmptyCatalogTable PgExtension => new("pg_catalog", "pg_extension",
            new("oid",        PgOid.Default,  PgFormat.Text),
            new("extname",    PgName.Default, PgFormat.Text),
            new("extversion", PgText.Default, PgFormat.Text));

        // No replication on RavenDB's PG surface; pgAdmin's `count(*)` over this returns 0.
        public static EmptyCatalogTable PgReplicationSlots => new("pg_catalog", "pg_replication_slots",
            new("slot_name", PgName.Default, PgFormat.Text),
            new("slot_type", PgText.Default, PgFormat.Text),
            new("active",    PgBool.Default, PgFormat.Text));

        // GSSAPI authentication status. We don't support GSSAPI, so the view is empty and pgAdmin's
        // `WHERE pid = pg_backend_pid()` filter yields no rows (which pgAdmin treats as "no GSSAPI").
        public static EmptyCatalogTable PgStatGssapi => new("pg_catalog", "pg_stat_gssapi",
            new("pid",               PgInt4.Default, PgFormat.Text),
            new("gss_authenticated", PgBool.Default, PgFormat.Text),
            new("encrypted",         PgBool.Default, PgFormat.Text));

        // RavenDB has no PG role hierarchy - every connected user is independent. pg_auth_members
        // (which lists role-group memberships) is therefore empty; pgAdmin's recursive role-membership
        // CTE iterates against this empty table and terminates with just the base case.
        public static EmptyCatalogTable PgAuthMembers => new("pg_catalog", "pg_auth_members",
            new("roleid",       PgOid.Default,  PgFormat.Text),
            new("member",       PgOid.Default,  PgFormat.Text),
            new("grantor",      PgOid.Default,  PgFormat.Text),
            new("admin_option", PgBool.Default, PgFormat.Text));

        // RavenDB doesn't model tablespaces, but pgAdmin LEFT-JOINs pg_database against this view to
        // get the spacename for display. Empty, so spcname stays NULL, which is fine.
        public static EmptyCatalogTable PgTablespace => new("pg_catalog", "pg_tablespace",
            new("oid",     PgOid.Default,  PgFormat.Text),
            new("spcname", PgName.Default, PgFormat.Text));

        // Shared-object comments (cluster-wide objects like databases). We don't model comments;
        // pgAdmin LEFT-JOINs to pull descriptions and accepts NULL when there's no match.
        public static EmptyCatalogTable PgShdescription => new("pg_catalog", "pg_shdescription",
            new("objoid",      PgOid.Default,  PgFormat.Text),
            new("classoid",    PgOid.Default,  PgFormat.Text),
            new("description", PgText.Default, PgFormat.Text));

        // Per-object comments (schemas, tables, columns, ...). pgAdmin's schema-tree probe LEFT-JOINs
        // pg_namespace against this to render schema descriptions. We don't model comments, so an
        // empty view returns NULL for `des.description` on every namespace row - exactly what pgAdmin
        // expects when no description is set.
        public static EmptyCatalogTable PgDescription => new("pg_catalog", "pg_description",
            new("objoid",      PgOid.Default,  PgFormat.Text),
            new("classoid",    PgOid.Default,  PgFormat.Text),
            new("objsubid",    PgInt4.Default, PgFormat.Text),
            new("description", PgText.Default, PgFormat.Text));
    }
}
