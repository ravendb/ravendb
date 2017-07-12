using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;

using Rachis.Commands;
using Rachis.Interfaces;
using Rachis.Messages;
using Rachis.Transport;

using Raven.Abstractions.Logging;
using Raven.Imports.Newtonsoft.Json;

using Voron;
using Voron.Impl;
using Voron.Trees;
using Voron.Util.Conversion;

namespace Rachis.Storage
{
    /// <summary>
    /// Uses Voron to store the persistent state / log of the raft state machine.
    /// Structure:
    /// 
    /// * $metadata tree - db id, version, current term, voted form, Topology info (like peer lists)
    /// * logs - the actual entry logs
    /// * entry-terms - the term for each entry id
    /// * peers - the data about the peers in the cluster
    /// </summary>
    public class PersistentState : IDisposable
    {
        private const string CurrentVersion = "1.0";
        private const string LogsTreeName = "logs";
        private const string EntryTermsTreeName = "entry-terms";
        private const string MetadataTreeName = "$metadata";

        public Guid DbId { get; private set; }
        public string VotedFor { get; private set; }
        public long VotedForTerm { get; private set; }
        public long CurrentTerm { get; private set; }

        private readonly StorageEnvironment _env;

        private readonly string _name;
        private readonly CancellationToken _cancellationToken;
        private bool _isDisposed;

        public ICommandSerializer CommandSerializer { get; set; }

        public void SetCurrentTopology(Topology currentTopology, long index)
        {
            using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
            {
                SetCurrentTopologyInternal(currentTopology, index, tx);

                tx.Commit();
            }
        }

        private static void SetCurrentTopologyInternal(Topology currentTopology, long index, Transaction tx)
        {
            if (currentTopology.TopologyId == Guid.Empty)
                throw new InvalidOperationException("Cannot set topology with an empty TopologyId");

            var metadata = tx.ReadTree(MetadataTreeName);

            var current = metadata.Read("current-topology");
            metadata.Add("previous-topology", current == null ? "{}" : current.Reader.ToStringValue());
            metadata.Add("current-topology", JsonConvert.SerializeObject(currentTopology));
            metadata.Add("current-topology-index", EndianBitConverter.Little.GetBytes(index));
        }

        public Topology GetCurrentTopology()
        {
            using (var tx = _env.NewTransaction(TransactionFlags.Read))
            {
                var metadata = tx.ReadTree(MetadataTreeName);
                var current = metadata.Read("current-topology");
                var currentTopology = JsonConvert.DeserializeObject<Topology>(current == null ? "{}" : current.Reader.ToStringValue());
                return currentTopology;
            }
        }

        public PersistentState(string name, StorageEnvironmentOptions options, CancellationToken cancellationToken)
        {
            _name = name;
            _cancellationToken = cancellationToken;
            _env = new StorageEnvironment(options);
            InitializeDatabase();
        }

        public static void ClusterBootstrap(RaftEngineOptions options)
        {
            SetTopologyExplicitly(options,
                new Topology(Guid.NewGuid(), new[] { options.SelfConnection }, Enumerable.Empty<NodeConnectionInfo>(), Enumerable.Empty<NodeConnectionInfo>()),
                throwIfTopologyExists: true);
        }

        public static void SetTopologyExplicitly(RaftEngineOptions options, Topology topology, bool throwIfTopologyExists)
        {
            using (var ps = new PersistentState("ClusterBootstrap", options.StorageOptions, CancellationToken.None))
            {
                if (ps.GetCurrentTopology().HasVoters && throwIfTopologyExists)
                    throw new InvalidOperationException("Cannot set topology on a cluster that already have a topology");
                ps.SetCurrentTopology(topology, 0);
            }
        }


        private void InitializeDatabase()
        {
            using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
            {
                _env.CreateTree(tx, LogsTreeName);
                _env.CreateTree(tx, EntryTermsTreeName);

                var metadata = _env.CreateTree(tx, MetadataTreeName);
                var versionReadResult = metadata.Read("version");
                if (versionReadResult == null) // new db
                {
                    metadata.Add("version", Encoding.UTF8.GetBytes(CurrentVersion));
                    DbId = Guid.NewGuid();
                    metadata.Add("db-id", DbId.ToByteArray());
                    metadata.Add("current-term", EndianBitConverter.Little.GetBytes(0L));
                    metadata.Add("voted-for", Encoding.UTF8.GetBytes(string.Empty));
                    metadata.Add("voted-for-term", EndianBitConverter.Little.GetBytes(-1L));
                    metadata.Add("is-leader-potential", EndianBitConverter.Little.GetBytes(0));
                }
                else
                {
                    var dbVersion = versionReadResult.Reader.ToStringValue();
                    if (dbVersion != CurrentVersion)
                        throw new InvalidOperationException("Cannot open db because its version is " + dbVersion +
                                                            " but the library expects version " + CurrentVersion);

                    int used;
                    var bytes = metadata.Read("db-id").Reader.ReadBytes(16, out used).Take(16).ToArray();
                    DbId = new Guid(bytes);

                    CurrentTerm = metadata.Read("current-term").Reader.ReadLittleEndianInt64();
                    var votedFor = metadata.Read("voted-for");
                    VotedFor = votedFor.Reader.Length == 0 ? null : votedFor.Reader.ToStringValue();

                    var votedForTerm = metadata.Read("voted-for-term");
                    VotedForTerm = votedForTerm.Reader.ReadLittleEndianInt64();
                }

                tx.Commit();
            }
        }

        public long AppendToLeaderLog(Command command)
        {
            if (CurrentTerm == 0)
                throw new InvalidOperationException("Cannot append entries in term 0");
            using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var logs = tx.ReadTree(LogsTreeName);
                var terms = tx.ReadTree(EntryTermsTreeName);

                var nextEntryId = GetLastEntryNumber(logs, tx) + 1;
                var key = new Slice(EndianBitConverter.Big.GetBytes(nextEntryId));

                command.AssignedIndex = nextEntryId;
                var commandEntry = CommandSerializer.Serialize(command);
                logs.Add(key, commandEntry);
                terms.Add(key, BitConverter.GetBytes(CurrentTerm));

                var topologyChangeCommand = command as TopologyChangeCommand;
                if (topologyChangeCommand != null)
                {
                    SetCurrentTopologyInternal(topologyChangeCommand.Requested, nextEntryId, tx);
                }

                tx.Commit();

                return nextEntryId;
            }
        }

        /// <summary>
        /// this method is intended to be use for emergency fixes of the cluster state using the JS admin console
        /// </summary>
        public void Danger__InjectN00pAtPosition(long position, long term)
        {
            var noop = new NopCommand();
            PutCommandInternal(position, term, noop);
        }

        /// <summary>
        /// this method is intended to be use for emergency fixes of the cluster state using the JS admin console
        /// </summary>
        public void Danger__InjectCommandAtPosition(long position, long term,string command)
        {
            var cmd = CommandSerializer.Deserialize(Encoding.UTF8.GetBytes(command));

            PutCommandInternal(position, term, cmd);
        }

        private void PutCommandInternal(long position, long term, Command command)
        {
            using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var logs = tx.ReadTree(LogsTreeName);
                var terms = tx.ReadTree(EntryTermsTreeName);

                var key = new Slice(EndianBitConverter.Big.GetBytes(position));
                
                var commandEntry = CommandSerializer.Serialize(command);
                logs.Add(key, commandEntry);
                terms.Add(key, BitConverter.GetBytes(term));
                tx.Commit();
            }
        }

        private static long GetLastEntryNumber(Tree logs, Transaction tx)
        {
            long lastEntry;
            var lastKey = logs.LastKeyOrDefault();
            if (lastKey != null)
            {
                lastEntry = lastKey.CreateReader().ReadBigEndianInt64();
            }
            else
            {
                var metadata = tx.ReadTree(MetadataTreeName);
                // maybe there is a snapshot?
                var snapshotIndex = metadata.Read("snapshot-index");
                if (snapshotIndex != null)
                    lastEntry = snapshotIndex.Reader.ReadLittleEndianInt64();
                else
                    lastEntry = 0;
            }
            return lastEntry;
        }


        public long? TermFor(long logIndex)
        {
            using (var tx = _env.NewTransaction(TransactionFlags.Read))
            {
                var terms = tx.ReadTree(EntryTermsTreeName);
                var key = new Slice(EndianBitConverter.Big.GetBytes(logIndex));
                var result = terms.Read(key);
                if (result == null)
                {
                    var metadata = tx.ReadTree(MetadataTreeName);
                    var snapshotIndex = metadata.Read("snapshot-index");
                    if (snapshotIndex == null)
                        return null;

                    var snapshotIndexVal = snapshotIndex.Reader.ReadLittleEndianInt64();
                    if (snapshotIndexVal != logIndex)
                        return null;

                    var snapshotTerm = metadata.Read("snapshot-term");
                    if (snapshotTerm == null)
                        return null;
                    var snapshotTermVal = snapshotTerm.Reader.ReadLittleEndianInt64();
                    return snapshotTermVal;
                }
                var term = result.Reader.ReadLittleEndianInt64();

                tx.Commit();
                return term;
            }
        }

        public LogEntry LastLogEntry()
        {
            using (var tx = _env.NewTransaction(TransactionFlags.Read))
            {
                var terms = tx.ReadTree(EntryTermsTreeName);
                var logs = tx.ReadTree(LogsTreeName);

                var lastKey = logs.LastKeyOrDefault();
                if (lastKey == null)
                {
                    return GetLastLogEntryFromSnapshot(tx);
                }

                var index = lastKey.CreateReader().ReadBigEndianInt64();

                var result = terms.Read(lastKey);

                var term = result.Reader.ReadLittleEndianInt64();

                return new LogEntry
                {
                    Term = term,
                    Index = index,
                };
            }
        }

        private static LogEntry GetLastLogEntryFromSnapshot(Transaction tx)
        {
            var metadata = tx.ReadTree(MetadataTreeName);
            // maybe there is a snapshot?
            var snapshotTerm = metadata.Read("snapshot-term");
            var snapshotIndex = metadata.Read("snapshot-index");

            if (snapshotIndex == null || snapshotTerm == null)
                return new LogEntry();

            return new LogEntry
            {
                Term = snapshotTerm.Reader.ReadLittleEndianInt64(),
                Index = snapshotIndex.Reader.ReadLittleEndianInt64()
            };
        }

        public LogEntry GetLogEntry(long index)
        {
            using (var tx = _env.NewTransaction(TransactionFlags.Read))
            {
                var terms = tx.ReadTree(EntryTermsTreeName);

                var key = new Slice(EndianBitConverter.Big.GetBytes(index));
                var result = terms.Read(key);
                if (result == null)
                    return GetLastLogEntryFromSnapshot(tx);

                var term = result.Reader.ReadLittleEndianInt64();

                return new LogEntry
                {
                    Term = term,
                    Index = index,
                };
            }
        }

        public void RecordVoteFor(string candidateId, long voteTerm)
        {
            if (string.IsNullOrEmpty(candidateId))
                throw new ArgumentNullException("candidateId");

            using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
            {
                VotedFor = candidateId;
                var metadata = tx.ReadTree(MetadataTreeName);
                metadata.Add("voted-for", Encoding.UTF8.GetBytes(candidateId));
                metadata.Add("voted-for-term", EndianBitConverter.Little.GetBytes(voteTerm));
                VotedFor = candidateId;
                VotedForTerm = voteTerm;
                tx.Commit();
            }
        }


        public void UpdateTermTo(RaftEngine engine, long term)
        {
            using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var metadata = tx.ReadTree(MetadataTreeName);

                metadata.Add("current-term", EndianBitConverter.Little.GetBytes(term));
                metadata.Add("voted-for", new byte[0]); // clearing who we voted for
                metadata.Add("voted-for-term", EndianBitConverter.Little.GetBytes(-1L)); // clearing who we voted for

                VotedFor = null;
                VotedForTerm = -1;
                CurrentTerm = term;

                if (engine != null)
                    engine.OnNewTerm(term);

                tx.Commit();
            }
        }


        public IEnumerable<LogEntry> LogEntriesAfter(long index, long stopAfter = long.MaxValue)
        {
            Debug.Assert(index >= 0, "assert index >= 0, index actually is " + index);

            using (var tx = _env.NewTransaction(TransactionFlags.Read))
            {
                var logs = tx.ReadTree(LogsTreeName);
                var terms = tx.ReadTree(EntryTermsTreeName);
                var metadata = tx.ReadTree(MetadataTreeName);

                var topologyChangedIndex = ReadIsTopologyChanged(metadata);

                using (var it = logs.Iterate())
                {
                    var key = new Slice(EndianBitConverter.Big.GetBytes(index));
                    if (it.Seek(key) == false)
                        yield break;

                    while (_cancellationToken.IsCancellationRequested == false)
                    {
                        var entryIndex = it.CurrentKey.CreateReader().ReadBigEndianInt64();
                        if (entryIndex > stopAfter)
                            yield break;

                        var term = terms.Read(it.CurrentKey).Reader.ReadLittleEndianInt64();

                        var entryReader = it.CreateReaderForCurrent();
                        var buffer = new byte[entryReader.Length];
                        entryReader.Read(buffer, 0, buffer.Length);

                        yield return new LogEntry
                        {
                            Term = term,
                            Data = buffer,
                            Index = entryIndex,
                            IsTopologyChange = entryIndex == topologyChangedIndex
                        };

                        if (it.MoveNext() == false)
                            yield break;
                    }
                }

                tx.Commit();
            }
        }

        public void Dispose()
        {
            if (_env != null && _isDisposed == false)
            {
                _env.Dispose();
                _isDisposed = true;
            }
        }

        public long? GetLastSnapshotIndex()
        {
            using (var tx = _env.NewTransaction(TransactionFlags.Read))
            {
                var metadata = tx.ReadTree(MetadataTreeName);
                var lastSnapshot = metadata.Read("snapshot-index");
                if (lastSnapshot == null)
                    return null;
                return lastSnapshot.Reader.ReadLittleEndianInt64();
            }
        }

        public void MarkSnapshotFor(long lastCommittedIndex, long lastCommittedTerm, int maxNumberOfItemsToRemove)
        {
            using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var logs = tx.ReadTree(LogsTreeName);
                var terms = tx.ReadTree(EntryTermsTreeName);
                var metadata = tx.ReadTree(MetadataTreeName);
                metadata.Add("snapshot-index", EndianBitConverter.Little.GetBytes(lastCommittedIndex));
                metadata.Add("snapshot-term", EndianBitConverter.Little.GetBytes(lastCommittedTerm));

                using (var it = logs.Iterate())
                {
                    it.MaxKey = new Slice(EndianBitConverter.Big.GetBytes(lastCommittedIndex + 1));
                    if (it.Seek(Slice.BeforeAllKeys))
                    {
                        do
                        {
                            terms.Delete(it.CurrentKey);
                            maxNumberOfItemsToRemove--;
                        } while (it.DeleteCurrentAndMoveNext() && maxNumberOfItemsToRemove >= 0);
                    }
                }
                tx.Commit();
            }
        }

        public void AppendToLog(RaftEngine engine, IEnumerable<LogEntry> entries, long removeAllAfter)
        {
            using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var logs = tx.ReadTree(LogsTreeName);
                var terms = tx.ReadTree(EntryTermsTreeName);
                var metadata = tx.ReadTree(MetadataTreeName);
                var changed = ReadIsTopologyChanged(metadata);
                if (changed > removeAllAfter)
                {
                    // need to reset the topology
                    var current = metadata.Read("current-topology");
                    var prevTopologyStr = current == null ? "{}" : current.Reader.ToStringValue();
                    metadata.Add("current-topology", prevTopologyStr);
                    metadata.Add("current-topology-index", EndianBitConverter.Little.GetBytes(0L));
                    var prevTopology = JsonConvert.DeserializeObject<Topology>(prevTopologyStr);
                    engine.RevertTopologyTo(prevTopology);
                }

                using (var it = logs.Iterate())
                {
                    if (it.Seek(new Slice(EndianBitConverter.Big.GetBytes(removeAllAfter))) &&
                        it.MoveNext())
                    {
                        do
                        {
                            terms.Delete(it.CurrentKey);
                        } while (it.DeleteCurrentAndMoveNext());
                    }
                }

                foreach (var entry in entries)
                {
                    var key = new Slice(EndianBitConverter.Big.GetBytes(entry.Index));
                    logs.Add(key, entry.Data);
                    terms.Add(key, new Slice(BitConverter.GetBytes(entry.Term)));
                }

                tx.Commit();
            }
        }

        private static long ReadIsTopologyChanged(Tree metadata)
        {
            var readResult = metadata.Read("current-topology-index");
            if (readResult == null)
                return -1;

            return readResult.Reader.ReadLittleEndianInt64();
        }

        public long GetCommitedEntriesCount(long lastCommittedEntry)
        {
            using (var tx = _env.NewTransaction(TransactionFlags.Read))
            {
                var logs = tx.ReadTree(LogsTreeName);

                using (var it = logs.Iterate(false))
                {
                    var lastEntryIndex = it.Seek(Slice.AfterAllKeys) == false ?
                        0 :
                        it.CurrentKey.CreateReader().ReadBigEndianInt64();
                    return logs.State.EntriesCount - Math.Max(0, (lastEntryIndex - lastCommittedEntry));
                }
            }
        }

        //this is public so we can change this from the JS console
        public static long NumberOfEntriesToFetch = 100;

        public IEnumerable<LogEntry> LastLogsEntries()
        {
            using (var tx = _env.NewTransaction(TransactionFlags.Read))
            {
                var logs = tx.ReadTree(LogsTreeName);

                var lastKey = logs.LastKeyOrDefault();
                if (lastKey == null)
                {
                    return new List<LogEntry>();
                }
                var index = lastKey.CreateReader().ReadBigEndianInt64();
                var fetchFromIndex = Math.Max(1, index - NumberOfEntriesToFetch);
                return LogEntriesAfter(fetchFromIndex);
            }
        }
    }
}
