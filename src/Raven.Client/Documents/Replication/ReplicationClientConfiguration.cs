using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Replication
{
    public sealed class ReplicationClientConfiguration
    {
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue();
        }
    }
}