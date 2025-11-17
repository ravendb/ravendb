using System;
using Raven.Client.Documents.Replication.Messages;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    /// <summary>
    /// The definition of a pull replication task.
    /// This defines the settings and behavior for pulling data between replication hubs and sinks.
    /// </summary>
    public sealed class PullReplicationDefinition : IDynamicJson
    {
        /// <summary>
        /// The delay duration for replication. Data will not be replicated until the specified delay has passed.
        /// </summary>
        public TimeSpan DelayReplicationFor;

        /// <summary>
        /// A value indicating whether the pull replication task is disabled.
        /// </summary>
        public bool Disabled;

        /// <summary>
        /// The mentor node responsible for the pull replication task.
        /// </summary>
        public string MentorNode;

        /// <summary>
        /// A value indicating whether the pull replication task should remain pinned to the mentor node.
        /// </summary>
        public bool PinToMentorNode;

        /// <summary>
        /// The mode of pull replication, determining the direction of data flow between hubs and sinks.
        /// Defaults to <see cref="PullReplicationMode.HubToSink"/>.
        /// </summary>
        public PullReplicationMode Mode = PullReplicationMode.HubToSink;

        /// <summary>
        /// The name of the pull replication task.
        /// </summary>
        public string Name;

        /// <summary>
        /// The unique identifier of the pull replication task.
        /// </summary>
        public long TaskId;

        /// <summary>
        /// A value indicating whether filtering is enabled for the pull replication task.
        /// </summary>
        public bool WithFiltering;

        /// <summary>
        /// The mode to prevent deletions during replication.
        /// </summary>
        public PreventDeletionsMode PreventDeletionsMode { get; set; }

        /// <inheritdoc cref="PullReplicationDefinition"/>
        public PullReplicationDefinition()
        {
        }

        /// <inheritdoc cref="PullReplicationDefinition"/>
        /// <param name="name">The name of the pull replication task.</param>
        /// <param name="delay">The delay duration for replication.</param>
        /// <param name="mentor">The mentor node for the pull replication task. Optional.</param>
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
                Name = request.PullReplicationDefinitionName,
                DelayReplicationFor = DelayReplicationFor,
                MentorNode = MentorNode,
                PinToMentorNode = PinToMentorNode,
                TaskId = taskId,
                Mode = Mode
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
