using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog
{
    internal sealed record PgVirtualColumn(string Name, PgType PgType, PgFormat FormatCode = PgFormat.Text);
}
