using System;
using System.IO;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;
using Sparrow.Utils;

namespace Raven.Server.Documents.Replication.Outgoing
{
    public abstract class OutgoingPullReplicationHandler : DatabaseOutgoingReplicationHandler
    {
        public string[] PathsToSend;
        private string[] _destinationAcceptablePaths;
        public ReplicationLoader.PullReplicationParams OutgoingPullReplicationParams;

        public string CertificateThumbprint;

        protected OutgoingPullReplicationHandler(ReplicationLoader parent, DocumentDatabase database, ReplicationNode node, TcpConnectionInfo connectionInfo) : 
            base(parent, database, node, connectionInfo)
        {
        }

        public override ReplicationDocumentSenderBase CreateDocumentSender(Stream stream, RavenLogger logger)
        {
            return new FilteredReplicationDocumentSender(stream, this, logger, PathsToSend, _destinationAcceptablePaths);
        }

        protected override void ProcessHandshakeResponse((ReplicationMessageReply.ReplyType ReplyType, ReplicationMessageReply Reply) response)
        {
            base.ProcessHandshakeResponse(response);
            // this is used when the other side lets us know what paths it is going to accept from us
            // it supplements (but does not extend) what we are willing to send out 
            _destinationAcceptablePaths = response.Reply.AcceptablePaths;
        }
    }

    internal sealed class OutgoingPullReplicationHandlerAsHub : OutgoingPullReplicationHandler
    {
        // In case this is an outgoing pull replication from the hub
        // we need to associate this instance to the replication definition.
        public string PullReplicationDefinitionName;

        /// <summary>
        /// The replication scope that should be disposed when the replication is done.
        /// </summary>
        private IDisposable _replicationScope;

        public OutgoingPullReplicationHandlerAsHub(ReplicationLoader parent, DocumentDatabase database, PullReplicationAsHub node, TcpConnectionInfo connectionInfo) : 
            base(parent, database, node, connectionInfo)
        {
        }

        public void StartPullReplicationAsHub(IDisposable replicationScope, Stream stream, TcpConnectionHeaderMessage.SupportedFeatures supportedVersions)
        {
            SupportedFeatures = supportedVersions;
            _stream = stream;
            _replicationScope = replicationScope;
            OutgoingReplicationThreadName = $"Pull replication as hub {FromToString}";
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

            using (_replicationScope)
            using (_stream)
            using (_interruptibleRead = new InterruptibleRead<DocumentsContextPool, DocumentsOperationContext>(_parent.ContextPool, _stream))
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (context.GetMemoryBuffer(out _buffer))
            {
                InitialHandshake();
                Replicate();
            }
        }

        public override string FromToString => $"{base.FromToString} (pull definition: {PullReplicationDefinitionName})";
    }

    public sealed class OutgoingPullReplicationHandlerAsSink : OutgoingPullReplicationHandler
    {
        private readonly PullReplicationAsSink _node;
        private string _lastPersistedSinkCv;

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

            if (_node.TaskId == 0)
                return;

            string cursorCv = ReadSinkCursorFromCluster() ?? LastAcceptedChangeVector;
            long startEtag = ChangeVectorUtils.GetEtagById(cursorCv, _database.DbBase64Id);

            if (startEtag > _lastSentDocumentEtag)
                _lastSentDocumentEtag = startEtag;
        }

        protected override void UpdateDestinationChangeVectorHeartbeat(ReplicationMessageReply replicationBatchReply)
        {
            base.UpdateDestinationChangeVectorHeartbeat(replicationBatchReply);
            if (_node.TaskId != 0 && string.IsNullOrEmpty(replicationBatchReply.ConfirmedSinkCv) == false)
                PersistSinkCursor(replicationBatchReply.ConfirmedSinkCv);
        }

        private void PersistSinkCursor(string confirmedSinkCv)
        {
            if (confirmedSinkCv == _lastPersistedSinkCv)
                return;

            var command = new UpdateExternalReplicationStateCommand(_database.Name, RaftIdGenerator.NewId())
            {
                ExternalReplicationState = new ExternalReplicationState
                {
                    TaskId = _node.TaskId,
                    NodeTag = _parent._server.NodeTag,
                    SourceChangeVector = confirmedSinkCv,
                    Type = ExternalReplicationState.ReplicationStateType.SinkCursor
                }
            };
            _parent._server.SendToLeaderAsync(command).ContinueWith(x =>
            {
                if (x.IsCompletedSuccessfully)
                    _lastPersistedSinkCv = confirmedSinkCv;
            }).IgnoreUnobservedExceptions();
        }

        private string ReadSinkCursorFromCluster()
        {
            using (_parent._server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var key = ExternalReplicationState.GenerateItemName(_database.Name, _node.TaskId, ExternalReplicationState.ReplicationStateType.SinkCursor);
                var stateBlittable = _parent._server.Cluster.Read(context, key);
                if (stateBlittable == null)
                    return null;

                var state = JsonDeserializationCluster.ExternalReplicationState(stateBlittable);
                return state.SourceChangeVector;
            }
        }
    }
}
