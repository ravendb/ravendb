using System.ComponentModel;

namespace Raven.Server.Utils.Features;

public enum Feature
{
    [Description("PostgreSQL")]
    PostgreSql,
    
    [Description("Corax")]
    Corax
}
