using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common;

public sealed class RelationalDatabaseColumn
{
    public string Id;
    public object Value;
    public BlittableJsonToken Type;
    public bool IsArrayOrObject = false;
}
