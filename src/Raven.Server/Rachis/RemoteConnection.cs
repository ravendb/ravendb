using System;
using System.Collections.Generic;
using System.IO;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Rachis
{
    public class RemoteConnection : IDisposable
    {
        private readonly string _dest;
        private string _src;
        private readonly Stream _stream;
        private readonly JsonOperationContext.ManagedPinnedBuffer _buffer;
        private Logger _log;

        public RemoteConnection(string dest, Stream stream)
        {
            _dest = new Uri(dest).Fragment ?? dest;
            _stream = stream;
            _buffer = JsonOperationContext.ManagedPinnedBuffer.LongLivedInstance();
        }

        public RemoteConnection(string dest, string src, Stream stream)
        {
            _dest = new Uri(dest).Fragment ?? dest;
            _src = new Uri(src).Fragment ?? src;
            _log = LoggingSource.Instance.GetLogger<RemoteConnection>(_src + " > " + _dest);
            _stream = stream;
            _buffer = JsonOperationContext.ManagedPinnedBuffer.LongLivedInstance();
        }

        public void Send(JsonOperationContext context, RachisHello helloMsg)
        {
            Send(context, new DynamicJsonValue
            {
                ["Type"] = nameof(RachisHello),
                [nameof(RachisHello.DebugSourceIdentifier)] = helloMsg.DebugSourceIdentifier,
                [nameof(RachisHello.InitialMessageType)] = helloMsg.InitialMessageType,
                [nameof(RachisHello.TopologyId)] = helloMsg.TopologyId,
            });
        }

        private void Send(JsonOperationContext context, DynamicJsonValue msg)
        {
            using (var msgJson = context.ReadObject(msg, "msg"))
            {
                Send(context, msgJson);
            }
        }

        private void Send(JsonOperationContext context, BlittableJsonReaderObject msg)
        {
            using (var writer = new BlittableJsonTextWriter(context, _stream))
            {
                context.Write(writer, msg);
            }
        }


        public void Send(JsonOperationContext context, RequestVoteResponse rvr)
        {
            if (_log?.IsInfoEnabled == true)
            {
                _log.Info($"Voting {rvr.VoteGranted} for term {rvr.Term} because: {rvr.Message}");
            }

            Send(context, new DynamicJsonValue
            {
                ["Type"] = nameof(RequestVoteResponse),
                [nameof(RequestVoteResponse.Term)] = rvr.Term,
                [nameof(RequestVoteResponse.VoteGranted)] = rvr.VoteGranted,
                [nameof(RequestVoteResponse.Message)] = rvr.Message,
            });
        }



        public void Send(JsonOperationContext context, RequestVote rv)
        {
            Send(context, new DynamicJsonValue
            {
                ["Type"] = nameof(RequestVote),
                [nameof(RequestVote.Term)] = rv.Term,
                [nameof(RequestVote.Source)] = rv.Source,
                [nameof(RequestVote.LastLogTerm)] = rv.LastLogTerm,
                [nameof(RequestVote.LastLogIndex)] = rv.LastLogIndex,
                [nameof(RequestVote.IsTrialElection)] = rv.IsTrialElection,
                [nameof(RequestVote.IsForcedElection)] = rv.IsForcedElection,
            });
        }

        public void Send(JsonOperationContext context, AppendEntries ae, List<BlittableJsonReaderObject> items = null)
        {
            Send(context, new DynamicJsonValue
            {
                ["Type"] = nameof(AppendEntries),
                [nameof(AppendEntries.EntriesCount)] = ae.EntriesCount,
                [nameof(AppendEntries.LeaderCommit)] = ae.LeaderCommit,
                [nameof(AppendEntries.PrevLogIndex)] = ae.PrevLogIndex,
                [nameof(AppendEntries.PrevLogTerm)] = ae.PrevLogTerm,
                [nameof(AppendEntries.Term)] = ae.Term,
                [nameof(AppendEntries.TruncateLogBefore)] = ae.TruncateLogBefore,
            });

            if (items == null || items.Count == 0)
                return;

            foreach (var item in items)
            {
                Send(context, item);
            }
        }

        public void Send(JsonOperationContext context, InstallSnapshot installSnapshot)
        {
            Send(context, new DynamicJsonValue
            {
                ["Type"] = nameof(InstallSnapshot),
                [nameof(InstallSnapshot.LastIncludedIndex)] = installSnapshot.LastIncludedIndex,
                [nameof(InstallSnapshot.LastIncludedTerm)] = installSnapshot.LastIncludedTerm,
                [nameof(InstallSnapshot.Topology)] = installSnapshot.Topology,
            });
        }

        public void Send(JsonOperationContext context, InstallSnapshotResponse installSnapshotResponse)
        {
            Send(context, new DynamicJsonValue
            {
                ["Type"] = nameof(InstallSnapshotResponse),
                [nameof(InstallSnapshotResponse.CurrentTerm)] = installSnapshotResponse.CurrentTerm,
                [nameof(InstallSnapshotResponse.LastLogIndex)] = installSnapshotResponse.LastLogIndex,
                [nameof(InstallSnapshotResponse.Done)] = installSnapshotResponse.Done,
            });
        }

        public void Send(JsonOperationContext context, Exception e)
        {
            if (_log?.IsInfoEnabled == true)
            {
                _log.Info("Sending an error (and aborting connection)", e);
            }

            ;
            Send(context, new DynamicJsonValue
            {
                ["Type"] = "Error",
                ["Message"] = e.Message,
                ["Exception"] = e.ToString()
            });
        }

        public unsafe int Read(byte[] buffer, int offset, int count)
        {
            if (_buffer.Used < _buffer.Valid)
            {
                var size = Math.Min(count, _buffer.Valid - _buffer.Used);
                fixed (byte* pBuffer = buffer)
                {
                    Memory.Copy(pBuffer + offset, _buffer.Pointer + _buffer.Used, size);
                    _buffer.Used += size;
                    return size;
                }
            }
            return _stream.Read(buffer, offset, count);
        }

        public Reader CreateReader()
        {
            return new Reader(this);
        }

        public class Reader
        {
            private readonly RemoteConnection _parent;
            private byte[] _buffer = new byte[1024];

            public Reader(RemoteConnection parent)
            {
                _parent = parent;
            }

            public int ReadInt32()
            {
                ReadExactly(sizeof(int));
                return BitConverter.ToInt32(_buffer, 0);
            }

            public long ReadInt64()
            {
                ReadExactly(sizeof(long));
                return BitConverter.ToInt64(_buffer, 0);
            }

            public byte[] Buffer => _buffer;

            public void ReadExactly(int size)
            {
                if (_buffer.Length < size)
                    _buffer = new byte[Bits.NextPowerOf2(size)];
                var remaining = 0;
                while (remaining < size)
                {
                    var read = _parent.Read(_buffer, remaining, size - remaining);
                    remaining += read;
                }
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

        public InstallSnapshot ReadInstallSnapshot(JsonOperationContext context)
        {
            // we explicitly not disposing this here, because we need to access the topology
            var json = context.ParseToMemory(_stream, "rachis-snapshot",
                BlittableJsonDocumentBuilder.UsageMode.None, _buffer);
            json.BlittableValidation();
            ValidateMessage(nameof(InstallSnapshot), json);
            return JsonDeserializationRachis<InstallSnapshot>.Deserialize(json);
        }

        public RachisEntry ReadRachisEntry(JsonOperationContext context)
        {
            // we explicitly not disposing this here, because we need to access the entry
            var json = context.ParseToMemory(_stream, "rachis-entry",
                BlittableJsonDocumentBuilder.UsageMode.None, _buffer);
            json.BlittableValidation();
            ValidateMessage(nameof(RachisEntry), json);
            return JsonDeserializationRachis<RachisEntry>.Deserialize(json);
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
            Send(context, msg);
        }

        public void Dispose()
        {
            _stream?.Dispose();
            _buffer?.Dispose();
        }

        public RachisHello InitFollower(JsonOperationContext context)
        {
            using (
                var json = context.ParseToMemory(_stream, "rachis-initial-msg",
                    BlittableJsonDocumentBuilder.UsageMode.None, _buffer))
            {
                json.BlittableValidation();
                ValidateMessage(nameof(RachisHello), json);
                var rachisHello = JsonDeserializationRachis<RachisHello>.Deserialize(json);
                _src = 
                    new Uri(rachisHello.DebugSourceIdentifier).Fragment ??
                    rachisHello.DebugSourceIdentifier ?? 
                    "unknown";
                _log = LoggingSource.Instance.GetLogger<RemoteConnection>(_src + " > " + _dest);
                return rachisHello;
            }
        }

        public override string ToString()
        {
            return _src + " > " + _dest;
        }

        private static void ValidateMessage(string expectedType, BlittableJsonReaderObject json)
        {
            string type;
            if (json.TryGet("Type", out type) == false || type != expectedType)
                ThrowUnexpectedMessage(expectedType, json);
        }

        private static void ThrowUnexpectedMessage(string expectedType, BlittableJsonReaderObject json)
        {
            throw new InvalidDataException(
                $"Expected to get type of \'{expectedType}\' message, but got unkonwn message: {json}");
        }
    }
}