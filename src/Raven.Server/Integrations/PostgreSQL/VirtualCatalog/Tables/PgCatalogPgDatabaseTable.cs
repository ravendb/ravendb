using System.Collections.Generic;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog.Tables
{
    // pg_database: PostgreSQL's catalog of databases on the server. pgAdmin's connection probe
    // looks here to discover the active database's properties (encoding, can-create, etc.).
    //
    // We expose one row representing the currently-connected RavenDB database. The PG protocol
    // surface is single-DB-per-connection (`Database=Northwind` in the connection string), so
    // there's never anything else to list here. Other databases on the same server are not
    // visible — each one gets its own PG-protocol context.
    internal sealed class PgCatalogPgDatabaseTable : PgVirtualTable
    {
        public override string SchemaName => "pg_catalog";
        public override string TableName => "pg_database";

        public override IReadOnlyList<PgVirtualColumn> Columns { get; } = new PgVirtualColumn[]
        {
            new("oid",            PgOid.Default,  PgFormat.Text),
            new("datname",        PgName.Default, PgFormat.Text),
            new("datallowconn",   PgBool.Default, PgFormat.Text),
            new("encoding",       PgInt4.Default, PgFormat.Text),
            new("datistemplate",  PgBool.Default, PgFormat.Text),
            new("dattablespace",  PgOid.Default,  PgFormat.Text),
            new("datdba",         PgOid.Default,  PgFormat.Text),
            new("datconnlimit",   PgInt4.Default, PgFormat.Text),
            new("datacl",         PgText.Default, PgFormat.Text),
        };

        public override IEnumerable<object[]> EnumerateRows(VirtualQueryContext ctx)
        {
            if (ctx?.Database == null)
                yield break;

            // oid = 16384: pgAdmin's tree-load filters with `oid > 16383` to skip PG's system
            // databases. We want our DB to appear there, so the row uses the first user-oid.
            // encoding = 6   : PG's well-known id for UTF8 (which is all we ever serve).
            // dattablespace 1663 : PG's pg_default tablespace oid (referenced by pgAdmin's join).
            // datdba 10     : PG's bootstrap-superuser oid (cosmetic — pgAdmin reads it as owner).
            // datconnlimit -1   : PG's "no limit" sentinel.
            // datacl null       : no per-database ACL — pgAdmin tolerates NULL.
            yield return new object[] { 16384, ctx.Database.Name, true, 6, false, 1663, 10, -1, null };
        }
    }
}
