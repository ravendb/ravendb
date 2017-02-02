using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Rachis.Commands;
using Rachis.Communication;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;

namespace Rachis.Storage
{
    /// <summary>
    /// currently this is an in memory state for ease of development but this should be modified to use voron
    /// </summary>
    public class PersistentState
    {

        private int _electionTimeout;

        private Metadata _state;
        private string _name;
        private Dictionary<long,LogEntry> _log; //TODO: replace this with a persistent Voron Table
        private Topology _topology;  //TODO: replace this with a persistent Voron tree value
        private Topology _priviesTopology;
        public PersistentState(string name, int electionTimeout,int heartbeatTimeInMs)
        {
            _name = name;
            _electionTimeout = electionTimeout;
            _state = new Metadata
            {
                CurrentTerm = 0,
                VotedFor = Guid.Empty,
                VotedForTerm = 0,
                ElectionTimeInMs = electionTimeout,
                HeartbeatTimeInMs = heartbeatTimeInMs
            };
            CurrentTerm = _state.CurrentTerm;
            VotedForTerm = _state.VotedForTerm;
            VotedFor = _state.VotedFor;
            _log = new Dictionary<long, LogEntry>();
        }

        public LogEntry LastLogEntry()
        {
            if (_log.Keys.Any())
            {
                var index = _log.Values.Max(entry => entry.Index);
                return _log[index];
            }
            return new LogEntry();
        }

        public LogEntry FirstLogEntry()
        {
            if (_log.Keys.Any())
            {
                var index = _log.Values.Min(entry => entry.Index);
                return _log[index];
            }
            return new LogEntry();
        }

        public long? LastLogIndex()
        {
            return _lastLogIndex > 0 ? _lastLogIndex : (long?)null;
        }


        public long? FirstLogIndex()
        {
            LogEntry entry;
            return _log.TryGetValue(1, out entry) ? entry.Index : (long?)null;
        }

        public struct Metadata
        {
            public long CurrentTerm;
            public Guid VotedFor;//TODO:Add human readable name (host:port)
            public long VotedForTerm;
            public bool IsLeaderPotential;
            public int ElectionTimeInMs;
            public int HeartbeatTimeInMs;
            //TODO: Add snapshot fields
        }

        public Guid VotedFor { get; private set; } 
        public long VotedForTerm { get; private set; }
        public long CurrentTerm { get; private set; }

        public int HeartbeatTimeInMs => _state.HeartbeatTimeInMs;
        public int ElectionTimeInMs => _state.ElectionTimeInMs;

        private static readonly Topology EmptyTopology = new Topology();
        private int _lastLogIndex;

        public Topology GetCurrentTopology()
        {
            return _topology?? EmptyTopology;
        }

        public void UpdateTermTo(long term)
        {
            CurrentTerm = term;
        }

        public long TermFor(long prevLogIndex)
        {
            if (prevLogIndex > _lastLogIndex || prevLogIndex == 0)
                return 0;
            return _log[(int) prevLogIndex].Term;
        }

        public long AppendToLog(LogEntry[] entries)
        {
            if (entries == null || entries.Length == 0)
                return LastLogIndex() ?? 0;
            var firstEntry = entries[0].Index;
            var key = firstEntry;
            while (_log.ContainsKey(key))
            {
                _lastLogIndex--;
                _log.Remove(key++);
            }

            foreach (var entry in entries)
            {
                _lastLogIndex++;
                _log.Add(entry.Index,entry);
            }
            return _lastLogIndex;
        }

        public void SetCurrentTopology(Topology topology)
        {
            var oldTopology = _topology;
            _topology = topology;
            _priviesTopology = oldTopology;
        }

        public IEnumerable<LogEntry> LogEntriesAfter(long lastMatchedLogIndex ,long lastEntryIndex = long.MaxValue)
        {
            var nextSend = (int)lastMatchedLogIndex + 1;
            while (nextSend <= _lastLogIndex && nextSend <= lastEntryIndex)
            {
                yield return _log[nextSend++];
            }            
        }

        public long AppendToLeaderLog(Command command)
        {
            _lastLogIndex++;
            var logEntry = new LogEntry {Data = command.ToBytes(),Index = _lastLogIndex, Term = CurrentTerm,IsTopologyChange = command is TopologyChangeCommand};
            _log.Add(logEntry.Index, logEntry);
            var topologyChangeCommand = command as TopologyChangeCommand;
            if (topologyChangeCommand != null)
            {
                SetCurrentTopology(topologyChangeCommand.Requested);
            }
            return logEntry.Index;
        }

        public void Bootstrap(RaftEngineOptions options)
        {
            _topology = new Topology(new Guid(),new [] {options.SelfConnectionInfo}, Enumerable.Empty<NodeConnectionInfo>(), Enumerable.Empty<NodeConnectionInfo>());
        }

        public LogEntry GetLogEntry(long commitIndex)
        {
            LogEntry entry;
            return _log.TryGetValue(commitIndex, out entry) ? entry : null;
        }
    }
}
