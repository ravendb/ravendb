using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    public class SqlColumn
    {
        public string Key;
        public object Value;
        public BlittableJsonToken Type;
    }
}