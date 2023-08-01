using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.OngoingTasks
{
    public class ServerWideTaskResponse : IDynamicJson
    {
        public string Name { get; set; }

        public long RaftCommandIndex { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(RaftCommandIndex)] = RaftCommandIndex
            };
        }
    }

    public sealed class ServerWideExternalReplicationResponse : ServerWideTaskResponse
    {

    }
}
