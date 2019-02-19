using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Utils;

namespace Raven.Server.Rachis
{
    public class RemoteConnection : IDisposable
    {
        private string _destTag;
        private string _src;
        private Stream _stream;
        private readonly JsonOperationContext.ManagedPinnedBuffer _buffer;
        private Logger _log;
        private Action _disconnect;
        private DisposeLock _disposerLock = new DisposeLock("RemoteConnection");

        public string Source => _src;
        public Stream Stream => _stream;
        public string Dest => _destTag;

        public RemoteConnection(string src, Stream stream, long term, Action disconnect, [CallerMemberName] string caller = null)
        {
            _log = LoggingSource.Instance.GetLogger<RemoteConnection>($"{src} > [? discovering... ? ]");
            _stream = stream;
            _disconnect = disconnect;
            _buffer = JsonOperationContext.ManagedPinnedBuffer.LongLivedInstance();
            RegisterConnection("?", term, caller);
        }

        public class RemoteConnectionInfo
        {
            public string Caller;
            public DateTime StartAt;
            public string Destination;
            public int Number;
            public long Term;
        }

        private RemoteConnectionInfo _info;
        private static int _connectionNumber = 0; 
        public static ConcurrentSet<RemoteConnectionInfo> RemoteConnectionsList = new ConcurrentSet<RemoteConnectionInfo>();

        public RemoteConnection(string dest, string src, long term, Stream stream, Action disconnect, [CallerMemberName] string caller = null)
        {
            _destTag = dest;
            _src = src;
            _disconnect = disconnect;
            _log = LoggingSource.Instance.GetLogger<RemoteConnection>($"{_src} > {_destTag}");
            _stream = stream;
            _buffer = JsonOperationContext.ManagedPinnedBuffer.LongLivedInstance();
            RegisterConnection(dest, term, caller);
        }

        public void Send(JsonOperationContext context, RachisHello helloMsg)
        {
            if (_log.IsInfoEnabled)
                _log.Info($"{helloMsg.DebugSourceIdentifier} says hello to {helloMsg.DebugDestinationIdentifier} with {helloMsg.InitialMessageType}");

            Send(context, new DynamicJsonValue
            {
                ["Type"] = nameof(RachisHello),
                [nameof(RachisHello.DebugSourceIdentifier)] = helloMsg.DebugSourceIdentifier,
                [nameof(RachisHello.DebugDestinationIdentifier)] = helloMsg.DebugDestinationIdentifier,
                [nameof(RachisHello.InitialMessageType)] = helloMsg.InitialMessageType,
                [nameof(RachisHello.TopologyId)] = helloMsg.TopologyId,
                [nameof(RachisHello.SendingThread)] = Thread.CurrentThread.ManagedThreadId,
                [nameof(RachisHello.ElectionTimeout)] = helloMsg.ElectionTimeout,
                [nameof(RachisHello.SourceUrl)] = helloMsg.SourceUrl,
                [nameof(RachisHello.DestinationUrl)] = helloMsg.DestinationUrl,
            });
        }

        private void Send(JsonOperationContext context, DynamicJsonValue msg)
        {
            using (var msgJson = context.ReadObject(msg, "msg"))
            {
                Send(context, msgJson);
            }
        }

     //   private static ConcurrentDictionary<string, Lazy<StreamWriter>> _writers = new ConcurrentDictionary<string, Lazy<StreamWriter>>();

        private void Send(JsonOperationContext context, BlittableJsonReaderObject msg)
        {
            using(_disposerLock.EnsureNotDisposed())
            using (var writer = new BlittableJsonTextWriter(context, _stream))
            {
       /*         var streamWriter = _writers.GetOrAdd(_dest, d => new Lazy<StreamWriter>(() => File.CreateText(d + ".log")))
                    .Value;
                lock (streamWriter)
                {
                    streamWriter.WriteLine($"{DateTime.UtcNow:O} {_src} > {_dest}: - {msg}");
                    streamWriter.Flush();
                }
                //Console.WriteLine($"{DateTime.UtcNow:O} {_src} > {_dest}: - {msg}");
                */
                context.Write(writer, msg);
            }
        }


        public void Send(JsonOperationContext context, RequestVoteResponse rvr)
        {
            if (_log.IsInfoEnabled)
            {
                _log.Info($"Voting {rvr.VoteGranted} for term {rvr.Term:#,#;;0} because: {rvr.Message}");
            }

            Send(context, new DynamicJsonValue
            {
                ["Type"] = nameof(RequestVoteResponse),
                [nameof(RequestVoteResponse.NotInTopology)] = rvr.NotInTopology,
                [nameof(RequestVoteResponse.Term)] = rvr.Term,
                [nameof(RequestVoteResponse.VoteGranted)] = rvr.VoteGranted,
                [nameof(RequestVoteResponse.ClusterCommandsVersion)] = ClusterCommandsVersionManager.MyCommandsVersion,
                [nameof(RequestVoteResponse.Message)] = rvr.Message
            });
        }

        public void Send(JsonOperationContext context, LogLengthNegotiation lln)
        {
            if (_log.IsInfoEnabled)
                _log.Info($"Log length negotiation request with ({lln.PrevLogIndex:#,#;;0} / {lln.PrevLogTerm:#,#;;0}), term: {lln.Term:#,#;;0}, Truncated: {lln.Truncated}");

            Send(context, new DynamicJsonValue
            {
                ["Type"] = nameof(LogLengthNegotiation),
                [nameof(LogLengthNegotiation.Term)] = lln.Term,
                [nameof(LogLengthNegotiation.PrevLogIndex)] = lln.PrevLogIndex,
                [nameof(LogLengthNegotiation.PrevLogTerm)] = lln.PrevLogTerm,
                [nameof(LogLengthNegotiation.Truncated)] = lln.Truncated,
                [nameof(LogLengthNegotiation.SendingThread)] = Thread.CurrentThread.ManagedThreadId
            });
        }

        public void Send(JsonOperationContext context, LogLengthNegotiationResponse lln)
        {
            if (_log.IsInfoEnabled)
                _log.Info($"Log length negotiation response with ({lln.MidpointIndex:#,#;;0} / {lln.MidpointTerm:#,#;;0}), MinIndex: {lln.MinIndex:#,#;;0}, MaxIndex: {lln.MaxIndex:#,#;;0}, LastLogIndex: {lln.LastLogIndex:#,#;;0}, Status: {lln.Status}, {lln.Message}");

            Send(context, new DynamicJsonValue
            {
                ["Type"] = nameof(LogLengthNegotiationResponse),
                [nameof(LogLengthNegotiationResponse.Status)] = lln.Status,
                [nameof(LogLengthNegotiationResponse.Message)] = lln.Message,
                [nameof(LogLengthNegotiationResponse.CurrentTerm)] = lln.CurrentTerm,
                [nameof(LogLengthNegotiationResponse.LastLogIndex)] = lln.LastLogIndex,
                [nameof(LogLengthNegotiationResponse.MaxIndex)] = lln.MaxIndex,
                [nameof(LogLengthNegotiationResponse.MinIndex)] = lln.MinIndex,
                [nameof(LogLengthNegotiationResponse.MidpointIndex)] = lln.MidpointIndex,
                [nameof(LogLengthNegotiationResponse.MidpointTerm)] = lln.MidpointTerm,
                [nameof(LogLengthNegotiationResponse.CommandsVersion)] = ClusterCommandsVersionManager.MyCommandsVersion

            });
        }

        public void Send(JsonOperationContext context, RequestVote rv)
        {
            if (_log.IsInfoEnabled)
            {
                _log.Info(
                    $"{rv.Source} requests vote in {rv.Term:#,#;;0}, trial: {rv.IsTrialElection}, forced: {rv.IsForcedElection}, result: {rv.ElectionResult} with: ({rv.LastLogIndex:#,#;;0} / {rv.LastLogTerm:#,#;;0}).");
            }
            Send(context, new DynamicJsonValue
            {
                ["Type"] = nameof(RequestVote),
                [nameof(RequestVote.Term)] = rv.Term,
                [nameof(RequestVote.Source)] = rv.Source,
                [nameof(RequestVote.LastLogTerm)] = rv.LastLogTerm,
                [nameof(RequestVote.LastLogIndex)] = rv.LastLogIndex,
                [nameof(RequestVote.IsTrialElection)] = rv.IsTrialElection,
                [nameof(RequestVote.IsForcedElection)] = rv.IsForcedElection,
                [nameof(RequestVote.ElectionResult)] = rv.ElectionResult,
                [nameof(RequestVote.SendingThread)] = Thread.CurrentThread.ManagedThreadId
            });
        }

        public void Send(JsonOperationContext context, Action updateFollowerTicks, AppendEntries ae, List<BlittableJsonReaderObject> items = null)
        {
            if (_log.IsInfoEnabled)
            {
                if (ae.EntriesCount > 0)
                {
                    _log.Info(
                        $"AppendEntries ({ae.EntriesCount:#,#;;0}) in {ae.Term:#,#;;0}, commit: {ae.LeaderCommit:#,#;;0}, leader for: {ae.TimeAsLeader:#,#;;0}, ({ae.PrevLogIndex:#,#;;0} / {ae.PrevLogTerm:#,#;;0}), truncate: {ae.TruncateLogBefore:#,#;;0}, force elections: {ae.ForceElections}.");
                }
            }

            var msg = new DynamicJsonValue
            {
                ["Type"] = nameof(AppendEntries),
                [nameof(AppendEntries.EntriesCount)] = ae.EntriesCount,
                [nameof(AppendEntries.LeaderCommit)] = ae.LeaderCommit,
                [nameof(AppendEntries.PrevLogIndex)] = ae.PrevLogIndex,
                [nameof(AppendEntries.PrevLogTerm)] = ae.PrevLogTerm,
                [nameof(AppendEntries.Term)] = ae.Term,
                [nameof(AppendEntries.TruncateLogBefore)] = ae.TruncateLogBefore,
                [nameof(AppendEntries.TimeAsLeader)] = ae.TimeAsLeader,
                [nameof(AppendEntries.SendingThread)] = Thread.CurrentThread.ManagedThreadId,
                [nameof(AppendEntries.MinCommandVersion)] = ae.MinCommandVersion
            };

            if (ae.ForceElections)
                msg[nameof(AppendEntries.ForceElections)] = true;

            Send(context, msg);

            if (items == null || items.Count == 0)
                return;

            foreach (var item in items)
            {
                updateFollowerTicks();
                Send(context, item);
            }
        }

        public void Send(JsonOperationContext context, InstallSnapshot installSnapshot)
        {
            if (_log.IsInfoEnabled)
                _log.Info($"Install snapshot on: ({installSnapshot.LastIncludedIndex:#,#;;0} / {installSnapshot.LastIncludedTerm:#,#;;0})");

            Send(context, new DynamicJsonValue
            {
                ["Type"] = nameof(InstallSnapshot),
                [nameof(InstallSnapshot.LastIncludedIndex)] = installSnapshot.LastIncludedIndex,
                [nameof(InstallSnapshot.LastIncludedTerm)] = installSnapshot.LastIncludedTerm,
                [nameof(InstallSnapshot.Topology)] = installSnapshot.Topology
            });
        }

        public void Send(JsonOperationContext context, InstallSnapshotResponse installSnapshotResponse)
        {
            if (_log.IsInfoEnabled)
                _log.Info($"Install snapshot response in {installSnapshotResponse.CurrentTerm:#,#;;0}, last log index: {installSnapshotResponse.LastLogIndex:#,#;;0}, Done: {installSnapshotResponse.Done}");

            Send(context, new DynamicJsonValue
            {
                ["Type"] = nameof(InstallSnapshotResponse),
                [nameof(InstallSnapshotResponse.CurrentTerm)] = installSnapshotResponse.CurrentTerm,
                [nameof(InstallSnapshotResponse.LastLogIndex)] = installSnapshotResponse.LastLogIndex,
                [nameof(InstallSnapshotResponse.Done)] = installSnapshotResponse.Done
            });
        }

        public void Send(JsonOperationContext context, Exception e)
        {
            if (_log.IsInfoEnabled)
            {
                _log.Info("Sending an error (and aborting connection)", e);
            }

            Send(context, new DynamicJsonValue
            {
                ["Type"] = "Error",
                ["ExceptionType"] = e.GetType().Name,
                ["Message"] = e.Message,
                ["Exception"] = e.ToString()
            });
        }

        public unsafe int Read(byte[] buffer, int offset, int count)
        {
            using (_disposerLock.EnsureNotDisposed())
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
                    if (read == 0)
                        throw new EndOfStreamException();
                    remaining += read;
                }
            }
        }

        public T Read<T>(JsonOperationContext context)
            where T : class
        {
            try
            {
                using(_disposerLock.EnsureNotDisposed())
                using (
                    var json = context.ParseToMemory(_stream, "rachis-item",
                        BlittableJsonDocumentBuilder.UsageMode.None, _buffer))
                {
                    json.BlittableValidation();
                    ValidateMessage(typeof(T).Name, json);
                    return JsonDeserializationRachis<T>.Deserialize(json);
                }
            }
            catch (IOException e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info(
                        $"Failed to read from connection. If this error happens during state change of a node, it is expected. (source : [{_src}] -> destination : [{_destTag}])",
                        e);
                throw new IOException("Failed to read " + typeof(T).Name, e);
            }
        }

        public InstallSnapshot ReadInstallSnapshot(JsonOperationContext context)
        {
            // we explicitly not disposing this here, because we need to access the topology
            BlittableJsonReaderObject json = null;

            try
            {
                using (_disposerLock.EnsureNotDisposed())
                {
                    json = context.ParseToMemory(_stream, "rachis-snapshot",
                        BlittableJsonDocumentBuilder.UsageMode.None, _buffer);
                    json.BlittableValidation();
                    ValidateMessage(nameof(InstallSnapshot), json);
                    return JsonDeserializationRachis<InstallSnapshot>.Deserialize(json);
                }
            }
            catch
            {
                json?.Dispose();
                throw;
            }
        }

        public RachisEntry ReadRachisEntry(JsonOperationContext context)
        {
            // we explicitly not disposing this here, because we need to access the entry
            BlittableJsonReaderObject json = null;

            try
            {
                using (_disposerLock.EnsureNotDisposed())
                {
                    json = context.ParseToMemory(_stream, "rachis-entry",
                    BlittableJsonDocumentBuilder.UsageMode.None, _buffer);
                    json.BlittableValidation();
                    ValidateMessage(nameof(RachisEntry), json);
                    return JsonDeserializationRachis<RachisEntry>.Deserialize(json);
                }
            }
            catch
            {
                json?.Dispose();
                throw;
            }
        }
        
        
        public void Send(JsonOperationContext context, AppendEntriesResponse aer)
        {
            if (_log.IsInfoEnabled)
            {
                if (aer.Message != null)
                {
                    _log.Info($"Replying with success {aer.Success}: {aer.Message}");
                }
                else if (aer.Pending)
                {
                    _log.Info($"Replying with pending for {aer.CurrentTerm} / {aer.LastLogIndex}");
                }
            }
            var msg = new DynamicJsonValue
            {
                ["Type"] = nameof(AppendEntriesResponse),
                [nameof(AppendEntriesResponse.Success)] = aer.Success,
                [nameof(AppendEntriesResponse.Pending)] = aer.Pending,
                [nameof(AppendEntriesResponse.Message)] = aer.Message,
                [nameof(AppendEntriesResponse.CurrentTerm)] = aer.CurrentTerm,
                [nameof(AppendEntriesResponse.LastLogIndex)] = aer.LastLogIndex,
            };
            
            Send(context, msg);
        }

        public void Dispose()
        {
            try
            {
                _disconnect(); // force disconnection, to abort read/write
            }
            catch (Exception)
            {
                // even if we get an error , we must continue to the actual disposal
            }
            using (_disposerLock.StartDisposing())
            {
                RemoteConnectionsList.TryRemove(_info);
                _stream?.Dispose();
                if(_buffer is IDisposable s)
                    s.Dispose();
            }
        }

        public RachisHello InitFollower(JsonOperationContext context)
        {
            using (_disposerLock.EnsureNotDisposed())
            using (
                var json = context.ParseToMemory(_stream, "rachis-initial-msg",
                    BlittableJsonDocumentBuilder.UsageMode.None, _buffer))
            {
                json.BlittableValidation();
                ValidateMessage(nameof(RachisHello), json);
                var rachisHello = JsonDeserializationRachis<RachisHello>.Deserialize(json);
                _src = rachisHello.DebugSourceIdentifier ?? "unknown";
                _destTag = rachisHello.DebugDestinationIdentifier ?? _destTag;
                _log = LoggingSource.Instance.GetLogger<RemoteConnection>($"{_src} > {_destTag}");
                _info.Destination = _destTag;

                return rachisHello;
            }
        }

        public override string ToString()
        {
            return $"Remote connection (cluster) : {_src} > {_destTag}";
        }

        
        
        private void RegisterConnection(string dest, long term, string caller)
        {
            var number = Interlocked.Increment(ref _connectionNumber);
            _info = new RemoteConnectionInfo
            {
                Caller = caller,
                Destination = dest,
                StartAt = DateTime.UtcNow,
                Number = number,
                Term = term
            };
            RemoteConnectionsList.Add(_info);
        }

        private static void ValidateMessage(string expectedType, BlittableJsonReaderObject json)
        {
            if (json.TryGet("Type", out string type) == false || type != expectedType)
                ThrowUnexpectedMessage(type, expectedType, json);
        }

        private static void ThrowUnexpectedMessage(string type, string expectedType, BlittableJsonReaderObject json)
        {
            if (type == "Error")
            {
                if (json.TryGet("ExceptionType", out string errorType) && errorType == typeof(TopologyMismatchException).Name)
                {
                    json.TryGet("Message", out string message);
                    throw new TopologyMismatchException(message);
                }
                
            }
            throw new InvalidDataException(
                $"Expected to get type of \'{expectedType}\' message, but got \'{type}\' message.", new Exception(json.ToString()));
        }
    }
}
