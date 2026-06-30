using System.Collections.Generic;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog.Tables
{
    // pg_roles: standard PG catalog of roles (users + groups). pgAdmin uses this to introspect
    // the connected user's privileges (`WHERE rolname = current_user`). We expose one row
    // representing the connected PG user; other RavenDB-side users on the same database aren't
    // visible at the PG-protocol layer.
    //
    // Privilege flags are set permissively (can-create-role, can-create-db = true) so pgAdmin
    // doesn't disable its admin UI. The PG endpoint is read-only — any DDL action attempted from
    // pgAdmin still fails at SQL-handling time.
    internal sealed class PgCatalogPgRolesTable : PgVirtualTable
    {
        public override string SchemaName => "pg_catalog";
        public override string TableName => "pg_roles";

        public override IReadOnlyList<PgVirtualColumn> Columns { get; } = new PgVirtualColumn[]
        {
            new("oid",            PgOid.Default,  PgFormat.Text),
            new("rolname",        PgName.Default, PgFormat.Text),
            new("rolsuper",       PgBool.Default, PgFormat.Text),
            new("rolcreaterole",  PgBool.Default, PgFormat.Text),
            new("rolcreatedb",    PgBool.Default, PgFormat.Text),
            new("rolcanlogin",    PgBool.Default, PgFormat.Text),
            new("rolinherit",     PgBool.Default, PgFormat.Text),
            new("rolreplication", PgBool.Default, PgFormat.Text),
            new("rolbypassrls",   PgBool.Default, PgFormat.Text),
        };

        public override IEnumerable<object[]> EnumerateRows(VirtualQueryContext ctx)
        {
            if (string.IsNullOrEmpty(ctx?.Username))
                yield break;

            // oid=10 is arbitrary but matches what real PG often emits for the bootstrap superuser.
            yield return new object[]
            {
                10,
                ctx.Username,
                false,  // rolsuper        — not a true PG superuser
                true,   // rolcreaterole   — keep pgAdmin's UI usable
                true,   // rolcreatedb     — same
                true,   // rolcanlogin     — yes (we logged in)
                true,   // rolinherit      — yes by default
                false,  // rolreplication  — no replication on this surface
                false,  // rolbypassrls    — RLS isn't a concept here
            };
        }
    }
}
