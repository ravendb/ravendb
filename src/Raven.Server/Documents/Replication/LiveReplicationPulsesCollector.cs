using System;
using Raven.Client.Documents.Replication;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;

namespace Raven.Server.Documents.Replication
{
    public interface ILiveReplicationCollector : IDisposable
    {
        
    }

    public class LiveReplicationPulsesCollector : IDisposable
    {
        private readonly DocumentDatabase _database;

        public readonly AsyncQueue<ReplicationPulse> Pulses = new AsyncQueue<ReplicationPulse>();

        public LiveReplicationPulsesCollector(DocumentDatabase database)
        {
            _database = database;

            _database.ReplicationLoader.IncomingReplicationAdded += IncomingHandlerAdded;
            _database.ReplicationLoader.IncomingReplicationRemoved += IncomingHandlerRemoved;
            _database.ReplicationLoader.OutgoingReplicationAdded += OutgoingHandlerAdded;
            _database.ReplicationLoader.OutgoingReplicationRemoved += OutgoingHandlerRemoved;

            foreach (var handler in _database.ReplicationLoader.IncomingHandlers)
                IncomingHandlerAdded(handler);

            foreach (var handler in _database.ReplicationLoader.OutgoingHandlers)
                OutgoingHandlerAdded(handler);
        }

        private void OutgoingHandlerRemoved(OutgoingReplicationHandler handler)
        {
            handler.HandleReplicationPulse -= HandleReplicationPulse;
        }

        private void OutgoingHandlerAdded(OutgoingReplicationHandler handler)
        {
            handler.HandleReplicationPulse += HandleReplicationPulse;
        }

        private void HandleReplicationPulse(ReplicationPulse pulse)
        {
            Pulses.Enqueue(pulse);
        }

        private void IncomingHandlerRemoved(IncomingReplicationHandler handler)
        {
            handler.HandleReplicationPulse -= HandleReplicationPulse;
        }

        private void IncomingHandlerAdded(IncomingReplicationHandler handler)
        {
            handler.HandleReplicationPulse += HandleReplicationPulse;
        }

        public void Dispose()
        {
            _database.ReplicationLoader.OutgoingReplicationRemoved -= OutgoingHandlerRemoved;
            _database.ReplicationLoader.OutgoingReplicationAdded -= OutgoingHandlerAdded;
            _database.ReplicationLoader.IncomingReplicationRemoved -= IncomingHandlerRemoved;
            _database.ReplicationLoader.IncomingReplicationAdded -= IncomingHandlerAdded;
        }

        public struct ReplicationPulse
        {
            public DateTime OccurredAt;
            public ReplicationPulseDirection Direction;
            public ReplicationNode To;
            public bool IsExternal;
            public IncomingConnectionInfo From;
            public string ExceptionMessage;

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(OccurredAt)] = OccurredAt,
                    [nameof(Direction)] = Direction.ToString(),
                    [nameof(To)] = To?.ToJson(),
                    [nameof(IsExternal)] = IsExternal,
                    [nameof(From)] = From?.ToJson(),
                    [nameof(ExceptionMessage)] = ExceptionMessage
                };
            }
        }
    }

    public enum ReplicationPulseDirection
    {
        OutgoingInitiate = 101,
        OutgoingInitiateError,
        OutgoingBegin,
        OutgoingError,
        OutgoingEnd,
        OutgoingHeartbeat,
        OutgoingHeartbeatError,
        OutgoingHeartbeatAcknowledge,
        OutgoingHeartbeatAcknowledgeError,

        IncomingInitiate = 201,
        IncomingInitiateError,
        IncomingBegin,
        IncomingError,
        IncomingEnd,
        IncomingHeartbeat,
        IncomingHeartbeatAcknowledge
    }
}
