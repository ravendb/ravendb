using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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
        private List<LogEntry> _log; //TODO: replace this with a persistent Voron Table
        private Topology _topology;  //TODO: replace this with a persistent Voron tree value
        private Topology _priviesTopology;
        public PersistentState(string name, int electionTimeout)
        {
            _name = name;
            _electionTimeout = electionTimeout;
            _state = new Metadata
            {
                CurrentTerm = -1L,
                VotedFor = Guid.Empty,
                VotedForTerm = -1L,
                ElectionTimeInMs = electionTimeout
            };
            CurrentTerm = _state.CurrentTerm;
            VotedForTerm = _state.VotedForTerm;
            VotedFor = _state.VotedFor;
            _log = new List<LogEntry>();
        }

        public LogEntry LastLogEntry()
        {
            return _log.Last();
        }

        public struct Metadata
        {
            public long CurrentTerm;
            public Guid VotedFor;//TODO:Add human readable name (host:port)
            public long VotedForTerm;
            public bool IsLeaderPotential;
            public int ElectionTimeInMs;
            //TODO: Add snapshot fields
        }

        public Guid VotedFor { get; private set; } 
        public long VotedForTerm { get; private set; }
        public long CurrentTerm { get; private set; }

        public Topology GetCurrentTopology()
        {
            return _topology;
        }

        public void UpdateTermTo(long term)
        {
            CurrentTerm = term;
        }

        public long TermFor(long prevLogIndex)
        {
            if (prevLogIndex >= _log.Count)
                return 0;
            return _log[(int) prevLogIndex].Term;
        }

        public void AppendToLog(LogEntry[] entries)
        {
            var firstEntryIndex = (int)entries[0].Index;
            if (_log.Count > entries[0].Index)
            {
                _log.RemoveRange(firstEntryIndex, _log.Count - firstEntryIndex);
            }
           _log.AddRange(entries);
        }

        public void SetCurrentTopology(Topology topology, long topologyChangeIndex)
        {
            var oldTopology = _topology;
            _topology = topology;
            _priviesTopology = oldTopology;
        }
    }
}
