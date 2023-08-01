using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    public sealed class SqlColumn
    {
        public string Id;
        public object Value;
        public BlittableJsonToken Type;
    }
}