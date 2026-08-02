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

        // RavenDB has no column defaults; SQLAlchemy's get_columns() subqueries this table, so it
        // must be registered even though it's empty.
        public static EmptyCatalogTable PgAttrdef => new("pg_catalog", "pg_attrdef",
            new("oid",     PgOid.Default,  PgFormat.Text),
            new("adrelid", PgOid.Default,  PgFormat.Text),
            new("adnum",   PgInt2.Default, PgFormat.Text),
            new("adbin",   PgText.Default, PgFormat.Text));

        // RavenDB has no sequences; SQLAlchemy's get_columns() subqueries this table, so it must be
        // registered even though it's empty.
        public static EmptyCatalogTable PgSequence => new("pg_catalog", "pg_sequence",
            new("seqrelid",     PgOid.Default,  PgFormat.Text),
            new("seqstart",     PgInt8.Default, PgFormat.Text),
            new("seqincrement", PgInt8.Default, PgFormat.Text),
            new("seqmin",       PgInt8.Default, PgFormat.Text),
            new("seqmax",       PgInt8.Default, PgFormat.Text),
            new("seqcache",     PgInt8.Default, PgFormat.Text),
            new("seqcycle",     PgBool.Default, PgFormat.Text));

        // RavenDB has no indexes on collections in the PG sense - a Raven index is a separate,
        // named artifact, not a relation attached to a collection - so this stays empty.
        //
        // SQLAlchemy's get_pk_constraint() joins pg_attribute against a subquery over this table,
        // and get_indexes() joins it too. Registering it empty answers both with a zero-row rowset
        // rather than a rejected statement, which is what "this table has no primary key and no
        // indexes" looks like on the wire.
        //
        // indkey/indoption are int2vector and indexprs/indpred are pg_node_tree; there is no row to
        // put in them, so the declared types only have to let the projection resolve.
        public static EmptyCatalogTable PgIndex => new("pg_catalog", "pg_index",
            new("indrelid",     PgOid.Default,  PgFormat.Text),
            new("indexrelid",   PgOid.Default,  PgFormat.Text),
            new("indisunique",  PgBool.Default, PgFormat.Text),
            new("indisprimary", PgBool.Default, PgFormat.Text),
            new("indexprs",     PgText.Default, PgFormat.Text),
            new("indpred",      PgText.Default, PgFormat.Text),
            new("indkey",       PgText.Default, PgFormat.Text),
            new("indoption",    PgText.Default, PgFormat.Text),
            new("indnkeyatts",  PgInt2.Default, PgFormat.Text));

        // RavenDB has no constraints of any kind: no primary keys, no foreign keys (cross-document
        // links are document ids resolved with `load`, not FKs), no unique or check constraints.
        //
        // Four of SQLAlchemy's reflection methods read nothing but this table - get_pk_constraint's
        // PK_CONS_SQL, get_foreign_keys, get_unique_constraints and get_check_constraints - and
        // get_indexes LEFT-JOINs it to attach constraint-backed indexes. Registered empty, each
        // answers with a zero-row rowset, which they turn into "no primary key", "no foreign keys"
        // and empty lists rather than an error.
        //
        // With no rows, the pg_get_constraintdef() that get_foreign_keys and get_check_constraints
        // project is never evaluated - there is no constraint to render, so we don't implement it.
        //
        // conkey/confkey are int2[]; nothing reads a value out of them, so text is enough for the
        // projection to resolve.
        public static EmptyCatalogTable PgConstraint => new("pg_catalog", "pg_constraint",
            new("oid",       PgOid.Default,  PgFormat.Text),
            new("conname",   PgName.Default, PgFormat.Text),
            new("conrelid",  PgOid.Default,  PgFormat.Text),
            new("contype",   PgChar.Default, PgFormat.Text),
            new("conkey",    PgText.Default, PgFormat.Text),
            new("confrelid", PgOid.Default,  PgFormat.Text),
            new("conindid",  PgOid.Default,  PgFormat.Text));

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
