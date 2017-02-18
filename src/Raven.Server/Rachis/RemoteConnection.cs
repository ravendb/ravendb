using System;
using System.Collections.Generic;
using System.IO;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Rachis
{
    public class RemoteConnection : IDisposable
    {
        private readonly Stream _stream;
        private readonly JsonOperationContext.ManagedPinnedBuffer _buffer;
        private string _debugSource;
        private Logger _log;

        public string DebugSource => _debugSource;

        public RemoteConnection(Stream stream)
        {
            _stream = stream;
            _buffer = JsonOperationContext.ManagedPinnedBuffer.LongLivedInstance();
        }

        public void Send(JsonOperationContext context, RachisHello helloMsg)
        {
            using (var writer = new BlittableJsonTextWriter(context, _stream))
            {
                context.Write(writer,
                    new DynamicJsonValue
                    {
                        ["Type"] = nameof(RachisHello),
                        [nameof(RachisHello.DebugSourceIdentifier)] = helloMsg.DebugSourceIdentifier,
                        [nameof(RachisHello.InitialMessageType)] = helloMsg.InitialMessageType,
                        [nameof(RachisHello.TopologyId)] = helloMsg.TopologyId,
                    });
            }
        }

        public void Send(JsonOperationContext context, AppendEntries ae, List<BlittableJsonReaderObject> items = null)
        {
            using (var writer = new BlittableJsonTextWriter(context, _stream))
            {
                context.Write(writer,
                    new DynamicJsonValue
                    {
                        ["Type"] = nameof(AppendEntries),
                        [nameof(AppendEntries.EntriesCount)] = ae.EntriesCount,
                        [nameof(AppendEntries.HasTopologyChange)] = ae.HasTopologyChange,
                        [nameof(AppendEntries.LeaderCommit)] = ae.LeaderCommit,
                        [nameof(AppendEntries.PrevLogIndex)] = ae.PrevLogIndex,
                        [nameof(AppendEntries.PrevLogTerm)] = ae.PrevLogTerm,
                        [nameof(AppendEntries.Term)] = ae.Term,
                    });

                if (items == null || items.Count == 0)
                    return;

                foreach (var item in items)
                {
                    context.Write(writer, item);
                }
            }
        }

        public void Send(JsonOperationContext context, InstallSnapshot installSnapshot)
        {
            using (var writer = new BlittableJsonTextWriter(context, _stream))
            {
                context.Write(writer,
                    new DynamicJsonValue
                    {
                        ["Type"] = nameof(InstallSnapshot),
                        [nameof(InstallSnapshot.LastIncludedIndex)] = installSnapshot.LastIncludedIndex,
                        [nameof(InstallSnapshot.LastIncludedTerm)] = installSnapshot.LastIncludedTerm,
                        [nameof(InstallSnapshot.SnapshotSize)] = installSnapshot.SnapshotSize,
                        [nameof(InstallSnapshot.Topology)] = installSnapshot.Topology,
                    });
            }
        }

        public void Send(JsonOperationContext context, Exception e)
        {
            if (_log?.IsInfoEnabled == true)
            {
                _log.Info("Sending an error (and aborting connection)", e);
            }

            ;
            using (var writer = new BlittableJsonTextWriter(context, _stream))
            {
                context.Write(writer,
                    new DynamicJsonValue
                    {
                        ["Type"] = "Error",
                        ["Message"] = e.Message,
                        ["Exception"] = e.ToString()
                    });
            }
        }

        public T Read<T>(JsonOperationContext context)
        {
            using (
                var json = context.ParseToMemory(_stream, "rachis-item",
                    BlittableJsonDocumentBuilder.UsageMode.None, _buffer))
            {
                json.BlittableValidation();
                ValidateMessage(typeof(T).Name, json);
                return JsonDeserializationRachis<T>.Deserialize(json);
            }
        }
        

        public void Send(JsonOperationContext context, AppendEntriesResponse aer)
        {
            if (_log?.IsInfoEnabled == true)
            {
                if (aer.Message != null)
                {
                    _log.Info($"Replying with success {aer.Success}: {aer.Message}");
                }
            }
            ;
            using (var writer = new BlittableJsonTextWriter(context, _stream))
            {
                var msg = new DynamicJsonValue
                {
                    ["Type"] = "AppendEntriesResponse",
                    [nameof(AppendEntriesResponse.Success)] = aer.Success,
                    [nameof(AppendEntriesResponse.Message)] = aer.Message,
                    [nameof(AppendEntriesResponse.CurrentTerm)] = aer.CurrentTerm,
                    [nameof(AppendEntriesResponse.LastLogIndex)] = aer.LastLogIndex,
                };
                var negotiation = aer.Negotiation;
                if (negotiation != null)
                {
                    msg[nameof(AppendEntriesResponse.Negotiation)] = new DynamicJsonValue
                    {
                        [nameof(Negotiation.MaxIndex)] = negotiation.MaxIndex,
                        [nameof(Negotiation.MinIndex)] = negotiation.MinIndex,
                        [nameof(Negotiation.MidpointIndex)] = negotiation.MidpointIndex,
                        [nameof(Negotiation.MidpointTerm)] = negotiation.MidpointTerm,
                    };
                }
                context.Write(writer, msg);
            }
        }

        public void Dispose()
        {
            _buffer?.Dispose();
        }

        public RachisHello InitFollower(JsonOperationContext context)
        {
            ;
            using (
                var json = context.ParseToMemory(_stream, "rachis-initial-msg",
                    BlittableJsonDocumentBuilder.UsageMode.None, _buffer))
            {
                json.BlittableValidation();
                ValidateMessage(nameof(RachisHello), json);
                var rachisHello = JsonDeserializationRachis<RachisHello>.Deserialize(json);
                _debugSource = rachisHello.DebugSourceIdentifier ?? "unknown";
                _log = LoggingSource.Instance.GetLogger<RemoteConnection>(_debugSource);
                return rachisHello;
            }
        }

        public override string ToString()
        {
            return _debugSource;
        }

        private static void ValidateMessage(string expectedType,BlittableJsonReaderObject json)
        {
            string type;
            if (json.TryGet("Type", out type) == false || type != expectedType)
                ThrowUnexpectedMessage(expectedType, json);
        }

        private static void ThrowUnexpectedMessage(string expectedType, BlittableJsonReaderObject json)
        {
            throw new InvalidOperationException(
                $"Expected to get type of \'{expectedType}\' message, but got unkonwn message: {json}");
        }
    }
}