using Raven.Server.Integrations.PostgreSQL.Types;

namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    // A single `<literal> AS <alias>` projection from PowerBI's outer wrapper, surfaced as a synthetic column.
    public sealed record ConstProjection(string ColumnName, PgType PgType, object Value);
}
