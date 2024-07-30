using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Relational;

public sealed class RelationalDatabaseColumn
{
    public string Id;
    public object Value;
    public BlittableJsonToken Type;
    public bool IsArrayOrObject = false;
}
