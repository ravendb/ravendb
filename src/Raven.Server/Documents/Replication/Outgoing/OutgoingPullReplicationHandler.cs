using System.IO;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;
using Sparrow.Utils;

namespace Raven.Server.Documents.Replication
{
    public enum PullReplicationChangeVectorWireMode
    {
        SendAsIs,
        SendLegacyCompatible
    }
}

namespace Raven.Server.Documents.Replication.Outgoing
{
    public abstract class OutgoingPullReplicationHandler : DatabaseOutgoingReplicationHandler
    {
        public string[] PathsToSend;
        public ReplicationLoader.PullReplicationParams OutgoingPullReplicationParams;
        private string[] _destinationAcceptablePaths;

        public string CertificateThumbprint;

        internal PullReplicationChangeVectorWireMode ChangeVectorWireMode { get; private set; } = PullReplicationChangeVectorWireMode.SendLegacyCompatible;

        protected OutgoingPullReplicationHandler(ReplicationLoader parent, DocumentDatabase database, ReplicationNode node, TcpConnectionInfo connectionInfo) :
            base(parent, database, node, connectionInfo)
        {
        }

        protected OutgoingPullReplicationHandler(ReplicationLoader parent, DocumentDatabase database, ReplicationNode node, TcpConnectionInfo connectionInfo, TcpConnectionOptions tcpConnectionOptions) :
            base(parent, database, node, connectionInfo, tcpConnectionOptions)
        {
        }

        public override ReplicationDocumentSenderBase CreateDocumentSender(Stream stream, RavenLogger logger)
        {
            return new FilteredReplicationDocumentSender(stream, this, logger, PathsToSend, _destinationAcceptablePaths);
        }

        protected override DynamicJsonValue GetInitialHandshakeRequest()
        {
            var request = base.GetInitialHandshakeRequest();

            if (_database.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors)
                request[nameof(ReplicationLatestEtagRequest.SupportsPullReplicationCompositeChangeVectors)] = true;

            return request;
        }

        protected override void ProcessHandshakeResponse((ReplicationMessageReply.ReplyType ReplyType, ReplicationMessageReply Reply) response)
        {
            base.ProcessHandshakeResponse(response);
            // this is used when the other side lets us know what paths it is going to accept from us
            // it supplements (but does not extend) what we are willing to send out 
            _destinationAcceptablePaths = response.Reply.AcceptablePaths;
            ChangeVectorWireMode = _database.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors &&
                                   response.Reply.SupportsPullReplicationCompositeChangeVectors
                ? PullReplicationChangeVectorWireMode.SendAsIs
                : PullReplicationChangeVectorWireMode.SendLegacyCompatible;
        }

        internal abstract bool ShouldSkipPreventableSinkToHubDeletion(ReplicationBatchItem item);
    }

    internal sealed class OutgoingPullReplicationHandlerAsHub : OutgoingPullReplicationHandler
    {
        // In case this is an outgoing pull replication from the hub
        // we need to associate this instance to the replication definition.
        public string PullReplicationDefinitionName;

        public OutgoingPullReplicationHandlerAsHub(ReplicationLoader parent, DocumentDatabase database, PullReplicationAsHub node, TcpConnectionInfo connectionInfo, TcpConnectionOptions tcpConnectionOptions,
            TcpConnectionHeaderMessage.SupportedFeatures supportedVersions) :
            base(parent, database, node, connectionInfo, tcpConnectionOptions)
        {
            SupportedFeatures = supportedVersions;
            _stream = tcpConnectionOptions.Stream;
            _tcpConnectionOptions = tcpConnectionOptions;

            OutgoingReplicationThreadName = $"Pull replication as hub {FromToString}";
        }

        public void StartPullReplicationAsHub()
        {
            _longRunningSendingWork =
                PoolOfThreads.GlobalRavenThreadPool.LongRunning(x => HandleReplicationErrors(PullReplication), null, ThreadNames.ForOutgoingReplication(OutgoingReplicationThreadName,
                    _database.Name, Destination.FromString(), pullReplicationAsHub: true));
        }

        private void PullReplication()
        {
            NativeMemory.EnsureRegistered();

            AddReplicationPulse(ReplicationPulseDirection.OutgoingInitiate);
            if (Logger.IsInfoEnabled)
                Logger.Info($"Start pull replication as hub {FromToString}");

            using (_tcpConnectionOptions)
            using (_stream)
            using (_interruptibleRead = new InterruptibleRead<DocumentsContextPool, DocumentsOperationContext>(_parent.ContextPool, _stream))
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (context.GetMemoryBuffer(out _buffer))
            {
                InitialHandshake();
                Replicate();
            }
        }

        protected override void ProcessHandshakeResponse((ReplicationMessageReply.ReplyType ReplyType, ReplicationMessageReply Reply) response)
        {
            base.ProcessHandshakeResponse(response);

            if (string.IsNullOrEmpty(response.Reply.LastConfirmedChangeVector))
                return;

            // we are on the hub, and we set the last sent change vector to the one that the other side has, so we won't send anything that it already has
            LastAcceptedChangeVector = ChangeVectorUtils.MergeVectors(LastAcceptedChangeVector, response.Reply.LastConfirmedChangeVector);
        }

        internal override bool ShouldSkipPreventableSinkToHubDeletion(ReplicationBatchItem item) => false;

        public override string FromToString => $"{base.FromToString} (pull definition: {PullReplicationDefinitionName})";
    }

    public sealed class OutgoingPullReplicationHandlerAsSink : OutgoingPullReplicationHandler
    {
        private readonly PullReplicationAsSink _node;

        public OutgoingPullReplicationHandlerAsSink(ReplicationLoader parent, DocumentDatabase database, PullReplicationAsSink node, TcpConnectionInfo connectionInfo) :
            base(parent, database, node, connectionInfo)
        {
            _node = node;
            PathsToSend = DetailedReplicationHubAccess.Preferred(node.AllowedSinkToHubPaths, node.AllowedHubToSinkPaths);
            CertificateThumbprint = _parent.GetCertificateForReplication(node, out _)?.Thumbprint;
        }

        protected override DynamicJsonValue GetSendPreliminaryDataRequest()
        {
            var request = base.GetSendPreliminaryDataRequest();

            request[nameof(ReplicationInitialRequest.Database)] = _parent.Database.Name; // my database
            request[nameof(ReplicationInitialRequest.DatabaseGroupId)] = _parent.Database.DatabaseGroupId; // my database id
            request[nameof(ReplicationInitialRequest.SourceUrl)] = _parent._server.GetNodeHttpServerUrl();
            request[nameof(ReplicationInitialRequest.Info)] = _parent._server.GetTcpInfoAndCertificates(null); // my connection info
            request[nameof(ReplicationInitialRequest.PullReplicationDefinitionName)] = _node.HubName;
            request[nameof(ReplicationInitialRequest.PullReplicationSinkTaskName)] = _node.GetTaskName();

            return request;
        }

        protected override void ProcessHandshakeResponse((ReplicationMessageReply.ReplyType ReplyType, ReplicationMessageReply Reply) response)
        {
            base.ProcessHandshakeResponse(response);
            OutgoingPullReplicationParams = new ReplicationLoader.PullReplicationParams
            {
                PreventDeletionsMode = response.Reply.PreventDeletionsMode,
                Type = ReplicationLoader.PullReplicationParams.ConnectionType.Outgoing
            };

            // we are on the sink and we set the change vector that we stored in order to continue sending items to the hub.
            string sinkCursor = ReplicationUtils.ReadCursorFromClusterFor(_parent.Server, _database.Name, _node.TaskId, ExternalReplicationState.ReplicationStateType.SinkCursor);
            if (string.IsNullOrEmpty(sinkCursor))
                return;

            LastAcceptedChangeVector = ChangeVectorUtils.MergeVectors(LastAcceptedChangeVector, sinkCursor);
        }

        protected override void UpdateDestinationChangeVectorHeartbeat(ReplicationMessageReply replicationBatchReply)
        {
            base.UpdateDestinationChangeVectorHeartbeat(replicationBatchReply);
            if (string.IsNullOrEmpty(replicationBatchReply.LastConfirmedChangeVector) == false)
                PersistSinkCursor(replicationBatchReply.LastConfirmedChangeVector);
        }

        private void PersistSinkCursor(string confirmedSinkCv)
        {
            var existingCv = ReplicationUtils.ReadCursorFromClusterFor(_parent.Server, _database.Name, _node.TaskId, ExternalReplicationState.ReplicationStateType.SinkCursor);
            if (existingCv == confirmedSinkCv)
                return;

            var command = new UpdateExternalReplicationStateCommand(_database.Name, RaftIdGenerator.NewId())
            {
                ExternalReplicationState = new ExternalReplicationState
                {
                    TaskId = _node.TaskId,
                    NodeTag = _parent._server.NodeTag,
                    SourceChangeVector = confirmedSinkCv,
                    Type = ExternalReplicationState.ReplicationStateType.SinkCursor,
                    FromToString = FromToString
                }
            };
            _parent.Server.SendToLeaderAsync(command).IgnoreUnobservedExceptions();
        }

        internal override bool ShouldSkipPreventableSinkToHubDeletion(ReplicationBatchItem item) =>
            OutgoingPullReplicationParams?.PreventDeletionsMode?.HasFlag(PreventDeletionsMode.PreventSinkToHubDeletions) == true &&
            item.IsPreventableSinkToHubDeletion() &&
            _database.ForTestingPurposes?.ForceSendTombstones != true;
    }
}
