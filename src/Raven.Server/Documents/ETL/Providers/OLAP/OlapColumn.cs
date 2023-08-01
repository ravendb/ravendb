using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.OLAP
{
    public sealed class OlapColumn
    {
        public string Name;
        public object Value;
        public BlittableJsonToken Type;
    }
}
