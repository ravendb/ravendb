using System;
using Raven.Client.Documents.Replication.Messages;
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

        public ExternalReplication ToExternalReplication(ReplicationInitialRequest request, long taskId)
        {
            return new ExternalReplication
            {
                Url = request.SourceUrl,
                Database = request.Database,
                Name = request.PullReplicationSinkTaskName,
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
