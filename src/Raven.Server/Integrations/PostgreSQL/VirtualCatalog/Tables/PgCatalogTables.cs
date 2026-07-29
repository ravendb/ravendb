using System;
using System.Collections.Generic;
using Raven.Server.Documents;
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
    // pg_attribute is still empty, so a client that follows up with get_columns() - which joins
    // pg_attribute on these oids - gets no columns back and has to fall back to
    // information_schema.columns.
    internal sealed class PgCatalogPgClassTable : PgVirtualTable
    {
        // PG's first non-system oid: collections are user relations, and clients (pgAdmin) filter
        // system objects out with `oid > 16383`. pg_database's single row uses this same value -
        // harmless, since an oid is only ever compared against others from the same catalog.
        private const int FirstCollectionOid = 16384;
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
            if (ctx?.Database == null)
                yield break;

            var collectionNames = new List<string>();

            using (ctx.Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var collection in ctx.Database.DocumentsStorage.GetCollections(context))
                {
                    if (CollectionName.IsHiLoCollection(collection.Name))
                        continue;

                    collectionNames.Add(collection.Name);
                }
            }

            // Nothing persists these oids, so they're derived: name order from a fixed base means
            // the same set of collections always yields the same oid for the same name, which is
            // what a client that reads oids from one query and uses them in the next relies on.
            collectionNames.Sort(StringComparer.Ordinal);

            for (int i = 0; i < collectionNames.Count; i++)
            {
                yield return new object[]
                {
                    FirstCollectionOid + i, collectionNames[i], PublicNamespaceOid, OrdinaryTable, NotACompositeType
                };
            }
        }
    }
}
