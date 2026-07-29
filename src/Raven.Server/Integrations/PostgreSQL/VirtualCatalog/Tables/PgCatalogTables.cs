using System.Collections.Generic;
using System.Globalization;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog.Tables
{
    // CSV-backed pg_catalog tables — let Npgsql startup type-loading queries run through
    // PgVirtualInterpreter.
    internal abstract class CsvBackedCatalogTable : PgVirtualTable
    {
        private readonly object _gate = new();
        // volatile: the lock-free fast path reads _rows outside the lock, so the reference must not
        // publish before the list contents are visible (matters on weak memory models like ARM64).
        private volatile List<object[]> _rows;

        protected abstract string CsvFileName { get; }

        public override bool IsAlwaysEmpty => false;

        public override IEnumerable<object[]> EnumerateRows(VirtualQueryContext ctx)
        {
            if (_rows != null)
                return _rows;

            lock (_gate)
            {
                _rows ??= CatalogCsvLoader.Load(CsvFileName, Columns);
            }
            return _rows;
        }
    }

    internal sealed class PgCatalogPgTypeTable : CsvBackedCatalogTable
    {
        public override string SchemaName => "pg_catalog";
        public override string TableName => "pg_type";
        protected override string CsvFileName => "pg_type.csv";

        public override IReadOnlyList<PgVirtualColumn> Columns { get; } = new PgVirtualColumn[]
        {
            new("oid",         PgOid.Default,  PgFormat.Text),
            new("typname",     PgName.Default, PgFormat.Text),
            new("typnamespace",PgOid.Default,  PgFormat.Text),
            new("typtype",     PgChar.Default, PgFormat.Text),
            new("typrelid",    PgOid.Default,  PgFormat.Text),
            new("typnotnull",  PgBool.Default, PgFormat.Text),
            new("typbasetype", PgOid.Default,  PgFormat.Text),
            new("typelem",     PgOid.Default,  PgFormat.Text),
            new("typreceive",  PgOid.Default,  PgFormat.Text),
            new("typcategory", PgChar.Default, PgFormat.Text),
            new("typarray",    PgOid.Default,  PgFormat.Text),
        };
    }

    internal sealed class PgCatalogPgProcTable : CsvBackedCatalogTable
    {
        public override string SchemaName => "pg_catalog";
        public override string TableName => "pg_proc";
        protected override string CsvFileName => "pg_proc.csv";

        public override IReadOnlyList<PgVirtualColumn> Columns { get; } = new PgVirtualColumn[]
        {
            new("oid",     PgOid.Default,  PgFormat.Text),
            new("proname", PgName.Default, PgFormat.Text),
        };
    }

    internal sealed class PgCatalogPgRangeTable : CsvBackedCatalogTable
    {
        public override string SchemaName => "pg_catalog";
        public override string TableName => "pg_range";
        protected override string CsvFileName => "pg_range.csv";

        public override IReadOnlyList<PgVirtualColumn> Columns { get; } = new PgVirtualColumn[]
        {
            new("rngtypid",      PgOid.Default, PgFormat.Text),
            new("rngsubtype",    PgOid.Default, PgFormat.Text),
            new("rngmultitypid", PgOid.Default, PgFormat.Text),
        };
    }

    internal sealed class PgCatalogPgNamespaceTable : CsvBackedCatalogTable
    {
        public override string SchemaName => "pg_catalog";
        public override string TableName => "pg_namespace";
        protected override string CsvFileName => "pg_namespace.csv";

        public override IReadOnlyList<PgVirtualColumn> Columns { get; } = new PgVirtualColumn[]
        {
            new("oid",     PgOid.Default,  PgFormat.Text),
            new("nspname", PgName.Default, PgFormat.Text),
        };
    }

    // pg_class: PostgreSQL's catalog of relations, and where every client that reflects through
    // pg_catalog finds the table list. SQLAlchemy's PGDialect.get_table_names() - and therefore
    // Apache Superset - joins pg_class to pg_namespace and filters on relkind; it never reads
    // information_schema.tables. While this table held no rows, Superset connected fine but
    // offered no tables to build a dataset from (Zoho Desk #7031).
    //
    // So we report one row per collection, sourced exactly like InformationSchemaTablesTable so the
    // two catalogs always agree on the same set of names, with the same casing (Orders, not orders):
    //   relkind 'r'       - ordinary table. SQLAlchemy accepts 'r' and 'p' (partitioned); RavenDB
    //                       has no partitioned collections, so every row is 'r'.
    //   relnamespace 2200 - oid of the 'public' namespace, per pg_namespace.csv. We don't model
    //                       multiple schemas, same as information_schema.tables' table_schema.
    //   typrelid 0        - a collection isn't the backing relation of a composite type. Npgsql's
    //                       type-loader LEFT-JOINs pg_class on pg_type.typrelid, and PG uses 0 for
    //                       non-composite rows.
    //
    // The oids come from CollectionCatalog, which PgCatalogPgAttributeTable keys its rows by, so
    // get_columns() finds the columns of the relation get_table_names() listed.
    internal sealed class PgCatalogPgClassTable : PgVirtualTable
    {
        private const int PublicNamespaceOid = 2200;
        private const string OrdinaryTable = "r";
        private const int NotACompositeType = 0;

        public override string SchemaName => "pg_catalog";
        public override string TableName => "pg_class";

        public override IReadOnlyList<PgVirtualColumn> Columns { get; } = new PgVirtualColumn[]
        {
            new("oid",          PgOid.Default,  PgFormat.Text),
            new("relname",      PgName.Default, PgFormat.Text),
            new("relnamespace", PgOid.Default,  PgFormat.Text),
            new("relkind",      PgChar.Default, PgFormat.Text),
            new("typrelid",     PgOid.Default,  PgFormat.Text),
        };

        public override IEnumerable<object[]> EnumerateRows(VirtualQueryContext ctx)
        {
            foreach (var relation in CollectionCatalog.Relations(ctx))
            {
                yield return new object[]
                {
                    relation.Oid, relation.Name, PublicNamespaceOid, OrdinaryTable, NotACompositeType
                };
            }
        }
    }

    // pg_attribute: PostgreSQL's catalog of columns, and how a client that reflects through
    // pg_catalog learns a relation's column shape. SQLAlchemy's PGDialect.get_columns() - and
    // therefore Apache Superset - reads columns from here keyed by attrelid; it never reads
    // information_schema.columns. While this table held no rows, Superset listed the tables but
    // every one of them reflected with zero columns, so creating a dataset failed with
    // "Unable to load columns for the selected table" (Zoho Desk #7031).
    //
    // Rows come from CollectionCatalog - the same per-collection column derivation
    // information_schema.columns uses - with attrelid being the oid pg_class reports for the
    // collection. The remaining attributes describe a plain, nullable column, which is what every
    // RavenDB document field is:
    //   atttypmod -1       - no length/precision modifier. format_type(atttypid, atttypmod) renders
    //                        the type name from these two, exactly as SQLAlchemy asks for it.
    //   attnotnull false   - a document may simply omit a field; information_schema.columns says
    //                        is_nullable = YES for the same reason.
    //   atthasdef false    - RavenDB has no column defaults (see the empty pg_attrdef).
    //   attidentity ''     - not an identity column. PG stores the empty string, not NULL, and
    //   attgenerated ''      SQLAlchemy tests `attidentity != ''` - so '' is what says "plain
    //                        column". We don't model identity or generated columns at all.
    //   attisdropped false - nothing to drop; a collection's columns are whatever its documents have.
    internal sealed class PgCatalogPgAttributeTable : PgVirtualTable
    {
        // SQLAlchemy (and Npgsql) scope every read of this table to one relation. Enumerating the
        // columns of a collection costs a document read, so honoring the predicate keeps the common
        // case at one instead of one per collection.
        private const string AttRelIdPredicate = "attrelid";

        // PG's "plain column" markers - see the class doc.
        private const string NotAnIdentityColumn = "";
        private const string NotAGeneratedColumn = "";

        public override string SchemaName => "pg_catalog";
        public override string TableName => "pg_attribute";

        public override IReadOnlyList<PgVirtualColumn> Columns { get; } = new PgVirtualColumn[]
        {
            // Real pg_attribute has no oid column. This one predates the table having rows; it stays
            // declared (always NULL) so a query that projects it still resolves instead of being
            // rejected for an unknown column, which is what it got when the table was empty.
            new("oid",          PgOid.Default,  PgFormat.Text),
            new("attrelid",     PgOid.Default,  PgFormat.Text),
            new("attname",      PgName.Default, PgFormat.Text),
            new("atttypid",     PgOid.Default,  PgFormat.Text),
            new("attnum",       PgInt2.Default, PgFormat.Text),
            new("atttypmod",    PgInt4.Default, PgFormat.Text),
            new("attnotnull",   PgBool.Default, PgFormat.Text),
            new("atthasdef",    PgBool.Default, PgFormat.Text),
            new("attidentity",  PgChar.Default, PgFormat.Text),
            new("attgenerated", PgChar.Default, PgFormat.Text),
            new("attisdropped", PgBool.Default, PgFormat.Text),
        };

        public override IEnumerable<object[]> EnumerateRows(VirtualQueryContext ctx)
        {
            if (ctx?.Database == null)
                yield break;

            long? onlyRelation = null;
            if (ctx.Predicates != null &&
                ctx.Predicates.TryGetValue(AttRelIdPredicate, out var rawOid) &&
                TryReadOid(rawOid, out var predicateOid))
                onlyRelation = predicateOid;

            using (ctx.Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var relation in CollectionCatalog.Relations(ctx))
                {
                    if (onlyRelation.HasValue && relation.Oid != onlyRelation.Value)
                        continue;

                    // attnum is 1-based and gapless: PG reserves <= 0 for system attributes, and
                    // SQLAlchemy filters those out with `attnum > 0`.
                    short attnum = 1;
                    foreach (var column in CollectionCatalog.Columns(ctx.Database, context, relation.Name))
                    {
                        yield return new object[]
                        {
                            null, relation.Oid, column.Name, column.PgType.Oid,
                            attnum++, column.PgType.TypeModifier,
                            false, false,
                            NotAnIdentityColumn, NotAGeneratedColumn,
                            false
                        };
                    }
                }
            }
        }

        private static bool TryReadOid(object value, out long oid)
        {
            switch (value)
            {
                case long l: oid = l; return true;
                case int i: oid = i; return true;
                case string s when long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed): oid = parsed; return true;
                default: oid = 0; return false;
            }
        }
    }
}
