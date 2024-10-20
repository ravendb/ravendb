using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Rachis.Remote;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Rachis;

public abstract partial class RachisConsensus
{
    public class RachisDebug
    {
        public readonly ConcurrentDictionary<string, RachisTimingsHolder> TimingTracking = new ConcurrentDictionary<string, RachisTimingsHolder>();
        public readonly ConcurrentQueue<string> StateChangeTracking = new ConcurrentQueue<string>();

        public class RachisTimingsHolder
        {
            public ConcurrentQueue<RachisTimings> TimingTracking;
            public DateTime Since = DateTime.UtcNow;
        }

        public bool IsInterVersionTest;

        public RachisLogRecorder GetNewRecorder(string name)
        {
            var holder = new RachisTimingsHolder
            {
                TimingTracking = new ConcurrentQueue<RachisTimings>()
            };

            if (TimingTracking.TryAdd(name, holder) == false)
            {
                throw new ArgumentException($"Recorder with the name '{name}' already exists");
            }
            return new RachisLogRecorder(holder.TimingTracking);
        }

        public void RemoveRecorderOlderThan(DateTime after)
        {
            if (TimingTracking.IsEmpty)
                return;

            foreach (var item in TimingTracking)
            {
                if (item.Value.Since > after)
                    continue;
                TimingTracking.TryRemove(item.Key, out _);
            }
        }

        public void RemoveRecorder(string name)
        {
            if (TimingTracking.Remove(name, out var q))
            {
                q.TimingTracking.Clear();
            }
        }

        public DynamicJsonValue ToJson()
        {
            var timingTracking = new DynamicJsonValue();
            foreach (var tuple in TimingTracking.ForceEnumerateInThreadSafeManner().OrderBy(x => x.Key))
            {
                var key = tuple.Key;
                DynamicJsonArray inner;
                timingTracking[key] = inner = new DynamicJsonArray();
                foreach (var queue in tuple.Value.TimingTracking)
                {
                    inner.Add(new DynamicJsonArray(queue.Timings.OrderBy(x => x.At)));
                }
            }

            var stateTracking = new DynamicJsonArray(StateChangeTracking);

            return new DynamicJsonValue
            {
                [nameof(TimingTracking)] = timingTracking,
                [nameof(StateChangeTracking)] = stateTracking
            };
        }
    }

    public readonly RachisDebug InMemoryDebug = new RachisDebug();

    public DateTime? LastCommitted;
    public DateTime? LastAppended;

    public RaftDebugView DebugView()
    {
        switch (CurrentState)
        {
            case RachisState.Passive:
                return new PassiveDebugView(this);
            case RachisState.Candidate:
                return Candidate.ToDebugView;
            case RachisState.Follower:
                return Follower.ToDebugView;
            case RachisState.LeaderElect:
            case RachisState.Leader:
                return CurrentLeader.ToDebugView;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public LogSummary GetLogDetails(ClusterOperationContext context, long? fromIndex, int take, bool detailed)
    {
        GetLastTruncated(context, out var index, out var term);
        var range = GetLogEntriesRange(context);
        var commitIndex = GetLastCommitIndex(context);

        if (fromIndex.HasValue == false)
        {
            fromIndex = range.Max > 0 ? range.Max : commitIndex;
        }

        return new LogSummary
        {
            CommitIndex = commitIndex,
            LastTruncatedIndex = index,
            LastTruncatedTerm = term,
            FirstEntryIndex = range.Min,
            LastLogEntryIndex = range.Max,
            LastAppendedTime = LastAppended,
            LastCommitedTime = LastCommitted,
            CriticalError = GetUnrecoverableClusterError(),
            Logs = GetLogEntries(context, fromIndex.Value, take, detailed),
        };
    }

    public IEnumerable<RachisDebugLogEntry> GetLogEntries(ClusterOperationContext context, long fromIndex, int take, bool detailed)
    {
        var reveredNextIndex = Bits.SwapBytes(fromIndex);
        Span<byte> span = stackalloc byte[sizeof(long)];
        if (BitConverter.TryWriteBytes(span, reveredNextIndex) == false)
            throw new InvalidOperationException($"Couldn't convert {fromIndex} to span<byte>");

        var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
        using (Slice.From(context.Allocator, span, out Slice key))
        {
            foreach (var value in table.SeekBackwardByPrimaryKey(key, 0))
            {
                if (take-- <= 0)
                    yield break;

                var entry = RachisDebugLogEntry.CreateFromLog(context, value);
                if (detailed == false)
                {
                    entry.Entry.Dispose();
                    entry.Entry = null;
                }

                yield return entry;
                fromIndex = entry.Index - 1;
            }

            foreach (var value in LogHistory.GetHistoryLogs(context, fromIndex))
            {
                if (take-- <= 0)
                    yield break;

                yield return RachisLogHistory.CreateFromHistory(value);
            }
        }
    }
    public UnrecoverableClusterError GetUnrecoverableClusterError()
    {
        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            var prefix = AlertRaised.GetKey(AlertType.UnrecoverableClusterError, key: null) + "/";
            var json = ServerStore.NotificationCenter.GetStoredMessageByPrefix(context, prefix);
            if (json == null)
                return null;
            
            var error = new UnrecoverableClusterError();
            
            json.TryGet(nameof(AlertRaised.Id), out error.Id);
            json.TryGet(nameof(AlertRaised.Title), out error.Title);
            json.TryGet(nameof(AlertRaised.Message), out error.Message);
            json.TryGet(nameof(AlertRaised.CreatedAt), out error.CreatedAt);
            json.TryGet(nameof(AlertRaised.Details), out object exception);
            error.Exception = exception.ToString();
            return error;
        }
    }

    public class UnrecoverableClusterError : IDynamicJsonValueConvertible
    {
        public string Id;
        public string Title;
        public string Message;
        public DateTime CreatedAt;
        public string Exception;
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Title)] = Title,
                [nameof(Message)] = Message,
                [nameof(CreatedAt)] = CreatedAt,
                [nameof(Exception)] = Exception
            };
        }
    }

    public class RachisDebugLogEntry : IDynamicJsonValueConvertible
    {
        public long Term { get; set; }
        public long Index { get; set; }
        public long SizeInBytes { get; set; }
        public string CommandType { get; set; }
        public DateTime? CreateAt { get; set; }
        public BlittableJsonReaderObject Entry { get; set; }
        public RachisEntryFlags Flags { get; set; }
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Term)] = Term,
                [nameof(Index)] = Index,
                [nameof(SizeInBytes)] = SizeInBytes,
                [nameof(CommandType)] = CommandType,
                [nameof(Flags)] = Flags,
                [nameof(CreateAt)] = CreateAt,
                [nameof(Entry)] = Entry,
            };
        }

        internal static unsafe RachisDebugLogEntry CreateFromLog(ClusterOperationContext context, Table.TableValueHolder value)
        {
            RachisDebugLogEntry entry = new()
            {
                Index = Bits.SwapBytes(*(long*)value.Reader.Read(0, out int size)),
                Term = *(long*)value.Reader.Read(1, out size),
                Entry = new BlittableJsonReaderObject(value.Reader.Read(2, out size), size, context),
                SizeInBytes = size,
                Flags = *(RachisEntryFlags*)value.Reader.Read(3, out size)
            };

            if (entry.Flags != RachisEntryFlags.StateMachineCommand) 
                return entry;

            entry.Entry.TryGet(nameof(CommandBase.Type), out string commandType);
            entry.CommandType = commandType;

            if (entry.Entry.TryGet(nameof(CommandBase.UniqueRequestId), out string raftUniqueId))
            {
                entry.CreateAt = RachisLogHistory.GetDateTimeByGuid(context, raftUniqueId);
            }

            return entry;
        }
    }
}


public abstract class RaftDebugView : IDynamicJsonValueConvertible
{
    private readonly RachisConsensus _engine;
    public abstract string Role { get; }
    
    public long Term;
    public RaftCommandsVersion CommandsVersion;
    public DateTime Since;
    public LogSummary Log;

    protected RaftDebugView(RachisConsensus engine)
    {
        _engine = engine;
        Term = engine.CurrentTerm;
        CommandsVersion = new RaftCommandsVersion
        {
            Local = ClusterCommandsVersionManager.MyCommandsVersion,
            Cluster = ClusterCommandsVersionManager.CurrentClusterMinimalVersion,
        };
        Since = engine.LastStateChangeTime;
    }

    public void PopulateLogs(ClusterOperationContext context, long? fromIndex, int take, bool detailed)
    {
        Log = _engine.GetLogDetails(context, fromIndex, take, detailed);
    }

    public class PeerConnection(string destination, string status, bool connected) : IDynamicJsonValueConvertible
    {
        public bool Connected = connected;
        public string Destination = destination;
        public string Status = status;
        
        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Status)] = Status,
                [nameof(Destination)] = Destination,
                [nameof(Connected)] = Connected
            };
        }
    }

    public class DetailedPeerConnection(string destination, string status, bool connected) : PeerConnection(destination, status, connected)
    {
        public int Version;
        public bool Compression;
        public TcpConnectionHeaderMessage.SupportedFeatures.ClusterFeatures Features;
        public DateTime StartAt;
        public DateTime LastSent;
        public DateTime LastReceived;

        public static PeerConnection FromRemoteConnection(string destination, string status, bool connected, RemoteConnection connection)
        {
            if (connection == null)
                return new PeerConnection(destination, status, false);

            return new DetailedPeerConnection(destination, status, connected)
            {
                Destination = connection.Dest,
                Version = connection.Features.ProtocolVersion,
                Features = connection.Features.Cluster,
                Compression = connection.Features.DataCompression,
                StartAt = connection.Info.StartAt,
                LastReceived = connection.Info.LastReceived,
                LastSent = connection.Info.LastSent,
            };
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Version)] = Version;
            json[nameof(Compression)] = Compression;
            json[nameof(Features)] = new DynamicJsonValue
            {
                [nameof(Features.BaseLine)] = Features.BaseLine, 
                [nameof(Features.MultiTree)] = Features.MultiTree
            };
            json[nameof(StartAt)] = StartAt;
            json[nameof(LastSent)] = LastSent;
            json[nameof(LastReceived)] = LastReceived;
            return json;
        }
    }

    public class RaftCommandsVersion : IDynamicJsonValueConvertible
    {
        public int Cluster;
        public int Local;
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Cluster)] = Cluster,
                [nameof(Local)] = Local,
            };
        }
    }

    public virtual DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Role)] = Role,
            [nameof(Term)] = Term,
            [nameof(CommandsVersion)] = CommandsVersion.ToJson(),
            [nameof(Since)] = Since,
            [nameof(Log)] = Log?.ToJson()
        };
    }
}

public class PassiveDebugView(RachisConsensus engine) : RaftDebugView(engine)
{
    public override string Role => "Passive";
}

public class FollowerDebugView(Follower follower) : RaftDebugView(follower.Engine)
{
    public PeerConnection ConnectionToLeader = DetailedPeerConnection.FromRemoteConnection(follower.Connection.Dest, "Connected", true, follower.Connection);
    public List<RachisDebugMessage> RecentMessages = follower.DebugRecorder.Timings.ToList();
    public FollowerPhase Phase = follower.Phase;
    public enum FollowerPhase
    {
        Initial,
        Negotiation,
        Snapshot,
        Steady
    }
    public override string Role => "Follower";

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(Phase)] = Phase;
        json[nameof(ConnectionToLeader)] = ConnectionToLeader.ToJson();
        json[nameof(RecentMessages)] = new DynamicJsonArray(RecentMessages);
        return json;
    }
}

public class LeaderDebugView(Leader leader) : RaftDebugView(leader.Engine)
{
    public override string Role => "Leader";
    public string ElectionReason = leader.Engine.LastStateChangeReason;
    public List<PeerConnection> ConnectionToPeers = leader.CurrentPeers.Select(p 
        => DetailedPeerConnection.FromRemoteConnection(p.Key, p.Value.StatusMessage, p.Value.Status == AmbassadorStatus.Connected, p.Value.Connection)).ToList();
    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(ElectionReason)] = ElectionReason;
        json[nameof(ConnectionToPeers)] = new DynamicJsonArray(ConnectionToPeers);
        return json;
    }
}

public class CandidateDebugView(Candidate candidate) : RaftDebugView(candidate.Engine)
{
    public override string Role => "Candidate";
    public string ElectionReason = candidate.Engine.LastStateChangeReason;
    public List<PeerConnection> ConnectionToPeers = candidate.Voters.Select(p 
        => DetailedPeerConnection.FromRemoteConnection(p.Tag, p.StatusMessage, p.Status == AmbassadorStatus.Connected, p.Connection)).ToList();

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(ElectionReason)] = ElectionReason;
        json[nameof(ConnectionToPeers)] = new DynamicJsonArray(ConnectionToPeers);
        return json;
    }
}


public class RachisDebugMessage : IDynamicJsonValueConvertible
{
    public DateTime At = DateTime.UtcNow;
    public string Message;
    public long MsFromCycleStart;

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(At)] = At,
            [nameof(MsFromCycleStart)] = MsFromCycleStart,
            [nameof(Message)] = Message
        };
    }
}

public class RachisTimings
{
    public readonly ConcurrentBag<RachisDebugMessage> Timings = new ConcurrentBag<RachisDebugMessage>();
}

public class RachisLogRecorder
{
    private readonly ConcurrentQueue<RachisTimings> _queue;
    private readonly Stopwatch _sp = Stopwatch.StartNew();
    private RachisTimings _current;
    public ConcurrentBag<RachisDebugMessage> Timings => _current.Timings;

    public RachisLogRecorder(ConcurrentQueue<RachisTimings> queue)
    {
        _queue = queue;
    }

    public void Start()
    {
        _current = new RachisTimings();
        _queue.LimitedSizeEnqueue(_current, 10);
        Timings.Add(new RachisDebugMessage
        {
            Message = "Start",
            MsFromCycleStart = 0
        });
        _sp.Restart();
    }

    public void Record(string message)
    {
        if (_current == null)
            return; // ignore - shutting down

        Timings.Add(new RachisDebugMessage
        {
            Message = message,
            MsFromCycleStart = _sp.ElapsedMilliseconds
        });
    }
}

