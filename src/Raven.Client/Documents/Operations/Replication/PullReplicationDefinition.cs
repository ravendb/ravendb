using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Replication.Messages;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public class PullReplicationDefinition : IDynamicJsonValueConvertible
    {
        public Dictionary<string, string> Certificates; // <thumbprint, base64 cert>

        public TimeSpan DelayReplicationFor;
        public bool Disabled;

        public string MentorNode;

        public PullReplicationMode Mode = PullReplicationMode.Outgoing;

        public string Name;
        public long TaskId;

        public PullReplicationDefinition() { }

        public PullReplicationDefinition(string name, TimeSpan delay = default, string mentor = null)
        {
            Name = name;
            MentorNode = mentor;
            DelayReplicationFor = delay;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Certificates)] = DynamicJsonValue.Convert(Certificates),
                [nameof(TaskId)] = TaskId,
                [nameof(Disabled)] = Disabled,
                [nameof(MentorNode)] = MentorNode,
                [nameof(DelayReplicationFor)] = DelayReplicationFor,
                [nameof(Mode)] = Mode
            };
        }

        public void Validate(bool useSsl)
        {
            if (string.IsNullOrEmpty(Name))
                throw new ArgumentNullException(nameof(Name));

            if (useSsl == false)
            {
                if (Certificates?.Count > 0)
                {
                    throw new InvalidOperationException("Your server is unsecured and therefore you can't define pull replication with a certificate.");
                }
            }
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
    }
}
