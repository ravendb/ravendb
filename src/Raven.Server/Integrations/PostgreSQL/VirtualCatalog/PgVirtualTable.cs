using System.Collections.Generic;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog
{
    internal abstract class PgVirtualTable
    {
        public abstract string SchemaName { get; }
        public abstract string TableName { get; }
        public abstract IReadOnlyList<PgVirtualColumn> Columns { get; }

        /// <summary>
        /// Lets the interpreter short-circuit joins involving this table — if any source in a join
        /// is always empty, the join result is empty regardless of the join condition. Used by the
        /// catalog tables that hold no rows in RavenDB (pg_type, pg_enum, table_constraints, ...).
        /// </summary>
        public virtual bool IsAlwaysEmpty => false;

        public abstract IEnumerable<object[]> EnumerateRows(VirtualQueryContext ctx);
    }
}
