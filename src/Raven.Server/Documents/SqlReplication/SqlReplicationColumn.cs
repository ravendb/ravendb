using Sparrow.Json;

namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplicationColumn
    {
        public string Key;
        public object Value;
        public BlittableJsonToken Type;
    }
}