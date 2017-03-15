using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    public class SqlReplicationColumn
    {
        public string Key;
        public object Value;
        public BlittableJsonToken Type;
    }
}