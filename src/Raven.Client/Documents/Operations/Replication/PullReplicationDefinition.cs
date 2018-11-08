using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public class PullReplicationDefinition : FeatureTaskDefinition
    {
        public TimeSpan DelayReplicationFor;
        public string MentorNode;

        public PullReplicationDefinition() { }

        public PullReplicationDefinition(string name, TimeSpan delay = default, string mentor = null) : base(name)
        {
            MentorNode = mentor;
            DelayReplicationFor = delay;
        }

        public ExternalReplication ToExternalReplication(string database, long taskId)
        {
            return new ExternalReplication
            {
                Database = database,
                DelayReplicationFor = DelayReplicationFor,
                MentorNode = MentorNode,
                TaskId = taskId
            };
        }

        public override DynamicJsonValue ToJson()
        {
            var djv = base.ToJson();
            djv[nameof(MentorNode)] = MentorNode;
            djv[nameof(DelayReplicationFor)] = DelayReplicationFor;
            return djv;
        }
    }
}
