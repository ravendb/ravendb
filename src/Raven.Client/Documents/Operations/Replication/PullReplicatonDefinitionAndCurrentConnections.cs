using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.OngoingTasks;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public class PullReplicationDefinitionAndCurrentConnections : IDynamicJsonValueConvertible
    {
        public PullReplicationDefinition Definition { get; set; }
        public List<OngoingTaskPullReplicationAsHub> OngoingTasks { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Definition)] = Definition.ToJson(),
                [nameof(OngoingTasks)] = new DynamicJsonArray(OngoingTasks.Select(x => x.ToJson()))
            };
        }
    }
}
