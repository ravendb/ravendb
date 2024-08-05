using System;
using Raven.Client.Documents.Replication.Messages;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public sealed class PullReplicationDefinition : IDynamicJsonValueConvertible
    {
        public TimeSpan DelayReplicationFor;
        public bool Disabled;

        public string MentorNode;

        public bool PinToMentorNode;

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
                [nameof(TaskId)] = TaskId,
                [nameof(Disabled)] = Disabled,
                [nameof(MentorNode)] = MentorNode,
                [nameof(PinToMentorNode)] = PinToMentorNode,
                [nameof(DelayReplicationFor)] = DelayReplicationFor,
                [nameof(Mode)] = Mode,
                [nameof(WithFiltering)] = WithFiltering,
                [nameof(PreventDeletionsMode)] = PreventDeletionsMode
            };
        }

        public DynamicJsonValue ToAuditJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(TaskId)] = TaskId,
                [nameof(Disabled)] = Disabled,
                [nameof(MentorNode)] = MentorNode,
                [nameof(PinToMentorNode)] = PinToMentorNode,
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
                if (WithFiltering)
                {
                    throw new InvalidOperationException($"Server must be secured in order to use filtering in pull replication {Name}.");
                }

                if (Mode.HasFlag(PullReplicationMode.SinkToHub))
                {
                    throw new InvalidOperationException(
                        $"Server must be secured in order to use {nameof(Mode)} {nameof(PullReplicationMode.SinkToHub)} in pull replication {Name}");
                }
            }
        }

        internal PullReplicationAsHub ToPullReplicationAsHub(ReplicationInitialRequest request, long taskId)
        {
            return new PullReplicationAsHub
            {
                Url = request.SourceUrl,
                Database = request.Database,
                Name = request.PullReplicationSinkTaskName,
                DelayReplicationFor = DelayReplicationFor,
                MentorNode = MentorNode,
                PinToMentorNode = PinToMentorNode,
                TaskId = taskId
            };
        }
    }

    [Flags]
    public enum PreventDeletionsMode
    {
        None,
        PreventSinkToHubDeletions
    }
}
