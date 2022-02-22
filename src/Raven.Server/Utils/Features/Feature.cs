using System.ComponentModel;

namespace Raven.Server.Utils.Features;

public enum Feature
{
    [Description("Graph API")]
    GraphApi,

    [Description("PostgreSQL")]
    PostgreSql
}
