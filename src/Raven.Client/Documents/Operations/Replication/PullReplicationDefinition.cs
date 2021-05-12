using System;
using System.Collections.Generic;
using Raven.Client.Documents.Replication.Messages;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public class PullReplicationDefinition : IDynamicJsonValueConvertible
    {
        [Obsolete("You cannot use Certificates on the PullReplicationDefinition any more, please use the dedicated commands: RegisterReplicationHubAccessOperation and UnregisterReplicationHubAccessOperation")]
        public Dictionary<string, string> Certificates; // <thumbprint, base64 cert>

        public TimeSpan DelayReplicationFor;
        public bool Disabled;

        public string MentorNode;

        public PullReplicationMode Mode = PullReplicationMode.HubToSink;

        public string Name;
        public long TaskId;

        public bool WithFiltering;
        public PreventDeletionsMode PreventDeletionsMode { get; set; }

        public PullReplicationDefinition()
        {
        }

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
#pragma warning disable CS0618 // Type or member is obsolete
                [nameof(Certificates)] = DynamicJsonValue.Convert(Certificates),
#pragma warning restore CS0618 // Type or member is obsolete
                [nameof(TaskId)] = TaskId,
                [nameof(Disabled)] = Disabled,
                [nameof(MentorNode)] = MentorNode,
                [nameof(DelayReplicationFor)] = DelayReplicationFor,
                [nameof(Mode)] = Mode,
                [nameof(WithFiltering)] = WithFiltering,
                [nameof(PreventDeletionsMode)] = PreventDeletionsMode
            };
        }

        internal void Validate(bool useSsl)
        {
            if (string.IsNullOrEmpty(Name))
                throw new ArgumentNullException(nameof(Name));

            if (useSsl == false)
            {
#pragma warning disable CS0618 // Type or member is obsolete
                if (Certificates?.Count > 0)
#pragma warning restore CS0618 // Type or member is obsolete
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

    [Flags]
    public enum PreventDeletionsMode
    {
        Disabled,
        PreventSinkToHubDeletions,
        PreventDocumentExpirationFromSink
    }
}
