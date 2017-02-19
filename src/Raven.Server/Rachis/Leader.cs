using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Rachis
{
    /// <summary>
    /// This class implements the leader behavior. Note that only a single thread
    /// actually does work in here, the leader thread. All other work is requested 
    /// from it and it is done
    /// </summary>
    public class Leader : IDisposable
    {
        private Task _topologyModification;
        private readonly RachisConsensus _engine;

        private TaskCompletionSource<object> _newEntriesArrived = new TaskCompletionSource<object>();

        private readonly ConcurrentDictionary<long, TaskCompletionSource<object>> _entries =
            new ConcurrentDictionary<long, TaskCompletionSource<object>>();

        private readonly ManualResetEvent _newEntry = new ManualResetEvent(false);
        private readonly ManualResetEvent _voterResponded = new ManualResetEvent(false);
        private readonly ManualResetEvent _promotableUpdated = new ManualResetEvent(false);
        private readonly ManualResetEvent _shutdownRequested = new ManualResetEvent(false);
        private readonly ManualResetEvent _noop = new ManualResetEvent(false);

        private readonly Dictionary<string, FollowerAmbassador> _voters =
            new Dictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, FollowerAmbassador> _promotables =
            new Dictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, FollowerAmbassador> _nonVoters =
            new Dictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);

        private Thread _thread;

        public Leader(RachisConsensus engine)
        {
            _engine = engine;
        }

        public bool Running
        {
            get { return Volatile.Read(ref _running); }
            private set { Volatile.Write(ref _running, value); }
        }

        public void Start()
        {
            Running = true;
            ClusterTopology clusterTopology;
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                clusterTopology = _engine.GetTopology(context);
            }

            RefreshAmbassadors(clusterTopology);

            _thread = new Thread(Run)
            {
                Name = "Consensus Leader - " + _engine.Url,
                IsBackground = true
            };
            _thread.Start();
        }


        private void RefreshAmbassadors(ClusterTopology clusterTopology)
        {
            var old = new Dictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);
            foreach (var peers in new[] { _voters, _promotables, _nonVoters })
            {
                foreach (var peer in peers)
                {
                    old[peer.Key] = peer.Value;
                }
                peers.Clear();
            }

            foreach (var voter in clusterTopology.Voters)
            {
                if (voter == _engine.Url)
                    continue; // we obviously won't be applying to ourselves

                FollowerAmbassador existingInstance;
                if (old.TryGetValue(voter, out existingInstance))
                {
                    existingInstance.UpdateLeaderWake(_voterResponded);
                    _voters.Add(voter, existingInstance);
                    old.Remove(voter);
                    continue; // already here
                }

                var ambasaddor = new FollowerAmbassador(_engine, this, _voterResponded, voter, clusterTopology.ApiKey);
                _voters.Add(voter, ambasaddor);
                _engine.AppendStateDisposable(this, ambasaddor);
                ambasaddor.Start();
            }

            foreach (var promotable in clusterTopology.Promotables)
            {
                FollowerAmbassador existingInstance;
                if (old.TryGetValue(promotable, out existingInstance))
                {
                    existingInstance.UpdateLeaderWake(_promotableUpdated);
                    _promotables.Add(promotable, existingInstance);
                    old.Remove(promotable);
                    continue; // already here
                }

                var ambasaddor = new FollowerAmbassador(_engine, this, _promotableUpdated, promotable, clusterTopology.ApiKey);
                _promotables.Add(promotable, ambasaddor);
                _engine.AppendStateDisposable(this, ambasaddor);
                ambasaddor.Start();
            }

            foreach (var nonVoter in clusterTopology.NonVotingMembers)
            {
                FollowerAmbassador existingInstance;
                if (_nonVoters.TryGetValue(nonVoter, out existingInstance))
                {
                    existingInstance.UpdateLeaderWake(_noop);

                    _nonVoters.Add(nonVoter, existingInstance);
                    old.Remove(nonVoter);
                    continue; // already here
                }
                var ambasaddor = new FollowerAmbassador(_engine, this, _noop, nonVoter, clusterTopology.ApiKey);
                _nonVoters.Add(nonVoter, ambasaddor);
                _engine.AppendStateDisposable(this, ambasaddor);
                ambasaddor.Start();
            }

            foreach (var ambasaddor in old)
            {
                // it is not used by anything else, so we can close it
                ambasaddor.Value.Dispose();
            }
        }

        /// <summary>
        /// This is expected to run for a long time, and it cannot leak exceptions
        /// </summary>
        private void Run()
        {
            try
            {
                var handles = new WaitHandle[]
                {
                    _newEntry,
                    _voterResponded,
                    _promotableUpdated,
                    _shutdownRequested
                };

                var noopCmd = new DynamicJsonValue
                {
                    ["Command"] = "noop"
                };
                TransactionOperationContext context;
                using (_engine.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenWriteTransaction())
                using (var cmd = context.ReadObject(noopCmd, "noop-cmd"))
                {
                    _engine.InsertToLeaderLog(context, cmd, RachisEntryFlags.Noop);
                    tx.Commit();
                }

                while (true)
                {
                    switch (WaitHandle.WaitAny(handles, _engine.ElectionTimeoutMs))
                    {
                        case 0: // new entry
                            _newEntry.Reset();
                            // release any waiting ambasaddors to send immediately
                            var old = Interlocked.Exchange(ref _newEntriesArrived, new TaskCompletionSource<object>());
                            old.TrySetResult(null);
                            if (_voters.Count == 0)
                                goto case 1;
                            break;
                        case 1: // voter responded
                            _voterResponded.Reset();
                            OnVoterConfirmation();
                            break;
                        case 2: // promotable updated
                            _promotableUpdated.Reset();
                            CheckPromotables();
                            break;
                        case WaitHandle.WaitTimeout:
                            if (_voters.Count != 0)
                                VoteOfNoConfidence();
                            break;
                        case 3: // shutdown requested
                            return;
                    }

                    EnsureThatWeHaveLeadership(((_voters.Count + 1) / 2) + 1);
                }
            }
            catch (Exception e)
            {
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info("Error when running leader behavior", e);
                }
                try
                {
                    _engine.SwitchToCandidateState();
                }
                catch (Exception e2)
                {
                    if (_engine.Log.IsOperationsEnabled)
                    {
                        _engine.Log.Operations("After leadership failure, could not setup switch to candidate state", e2);
                    }
                }
            }
        }

        private void VoteOfNoConfidence()
        {
            Console.WriteLine("Nobody is talking to me?!");
            //TODO: List all the voters and their times
            throw new TimeoutException("Too long has passed since we got a confirmation from the majority of the cluster that this node is still the leader." +
                                       "Assuming that I'm not the leader and stepping down");
        }

        private long _lastCommit;
        private void OnVoterConfirmation()
        {
            var maxIndexOnQuorum = GetMaxIndexOnQuorum((_voters.Count / 2) + 1);

            if (_lastCommit == maxIndexOnQuorum)
                return; // nothing to do here

            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (context.OpenWriteTransaction())
            {
                _lastCommit = _engine.GetLastCommitIndex(context);

                if (_lastCommit == maxIndexOnQuorum)
                    return; // nothing to do here

                if (_engine.GetTermFor(maxIndexOnQuorum) < _engine.CurrentTerm)
                    return;// can't commit until at least one entry from our term has been published

                _lastCommit = maxIndexOnQuorum;

                _engine.Apply(context, maxIndexOnQuorum);

                _lastCommit = maxIndexOnQuorum;

                context.Transaction.Commit();
            }

            foreach (var kvp in _entries)
            {
                if (kvp.Key > _lastCommit)
                    continue;

                TaskCompletionSource<object> value;
                if (_entries.TryRemove(kvp.Key, out value))
                {
                    value.TrySetResult(null);
                }
            }
            if (_entries.Count != 0)
            {
                // we have still items to process, run them in 1 node cluster
                // and speed up the followers ambassadors if they can
                _newEntry.Set();
            }
        }

        private void EnsureThatWeHaveLeadership(int majority)
        {
            var now = DateTime.UtcNow;
            var peersHeardFromInElectionTimeout = 1; // we count as a node :-)
            foreach (var voter in _voters.Values)
            {
                if ((now - voter.LastReplyFromFollower).TotalMilliseconds < _engine.ElectionTimeoutMs)
                    peersHeardFromInElectionTimeout++;
            }
            if (peersHeardFromInElectionTimeout < majority)
                VoteOfNoConfidence(); // we didn't get enough votes to still remain the leader
        }

        /// <summary>
        /// This method works on the match indexes, assume that we have three nodes
        /// A, B and C, and they have the following index values:
        /// 
        /// { A = 4, B = 3, C = 2 }
        /// 
        /// 
        /// In this case, the quorum agrees on 3 as the committed index.
        /// 
        /// Why? Because A has 4 (which implies that it has 3) and B has 3 as well.
        /// So we have 2 nodes that have 3, so that is the quorom.
        /// </summary>
        private readonly SortedList<long, int> _votersPerIndex = new SortedList<long, int>();

        private bool _running;

        protected long GetMaxIndexOnQuorum(int minSize)
        {
            _votersPerIndex.Clear();
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                _votersPerIndex[_engine.GetLastEntryIndex(context)] = 1;
            }

            foreach (var voter in _voters.Values)
            {
                int count;
                var voterIndex = voter.FollowerMatchIndex;
                _votersPerIndex.TryGetValue(voterIndex, out count);
                _votersPerIndex[voterIndex] = count + 1;
            }
            var votesSoFar = 0;

            for (int i = _votersPerIndex.Count - 1; i >= 0; i--)
            {
                votesSoFar += _votersPerIndex.Values[i];
                if (votesSoFar >= minSize)
                    return _votersPerIndex.Keys[i];
            }
            return -1;
        }

        private void CheckPromotables()
        {
            long lastIndex;
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                lastIndex = _engine.GetLogEntriesRange(context).Item2;
            }

            foreach (var ambasaddor in _promotables)
            {
                if (ambasaddor.Value.FollowerMatchIndex != lastIndex)
                    continue;

                Task task;
                TryModifyTopology(ambasaddor.Key, TopologyModification.Voter, out task);

                _promotableUpdated.Set();
                break;
            }

        }

        public Task PutAsync(BlittableJsonReaderObject cmd)
        {
            TaskCompletionSource<object> tcs;
            long index;

            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (context.OpenWriteTransaction())
            {
                index = _engine.InsertToLeaderLog(context, cmd, RachisEntryFlags.StateMachineCommand);
                context.Transaction.Commit();
            }
            _entries[index] = tcs = new TaskCompletionSource<object>();

            _newEntry.Set();
            return tcs.Task;
        }

        public void Dispose()
        {
            Running = false;
            _shutdownRequested.Set();
            _newEntriesArrived.TrySetCanceled();
            foreach (var entry in _entries)
            {
                entry.Value.TrySetCanceled();
            }
            var ae = new ExceptionAggregator("Could not properly dispose Leader");
            foreach (var ambasaddor in _nonVoters)
            {
                ae.Execute(ambasaddor.Value.Dispose);
            }

            foreach (var ambasaddor in _promotables)
            {
                ae.Execute(ambasaddor.Value.Dispose);
            }
            foreach (var ambasaddor in _voters)
            {
                ae.Execute(ambasaddor.Value.Dispose);
            }
            //TODO: shutdown notification of some kind?
            if (_thread != null && _thread.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
                _thread.Join();

            _newEntry.Dispose();
            _voterResponded.Dispose();
            _promotableUpdated.Dispose();
            _shutdownRequested.Dispose();
            _noop.Dispose();
        }

        public Task WaitForNewEntries()
        {
            return _newEntriesArrived.Task;
        }

        public enum TopologyModification
        {
            Voter,
            Promotable,
            NonVoter,
            Remove
        }

        public bool TryModifyTopology(string node, TopologyModification modification, out Task task)
        {
            TaskCompletionSource<object> tcs;

            ClusterTopology clusterTopology;
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (context.OpenWriteTransaction())
            {
                if (_topologyModification != null)
                {
                    task = null;
                    return false;
                }

                clusterTopology = _engine.GetTopology(context);

                switch (modification)
                {
                    case TopologyModification.Voter:
                        clusterTopology = new ClusterTopology(clusterTopology.TopologyId, clusterTopology.ApiKey,
                            clusterTopology.Voters.Concat(new[] { node }).ToArray(),
                            clusterTopology.Promotables.Except(new[] { node }).ToArray(),
                            clusterTopology.NonVotingMembers.Except(new[] { node }).ToArray()
                        );
                        break;
                    case TopologyModification.Promotable:
                        clusterTopology = new ClusterTopology(clusterTopology.TopologyId, clusterTopology.ApiKey,
                            clusterTopology.Voters.Except(new[] { node }).ToArray(),
                            clusterTopology.Promotables.Concat(new[] { node }).ToArray(),
                            clusterTopology.NonVotingMembers.Except(new[] { node }).ToArray()
                        );
                        break;
                    case TopologyModification.NonVoter:
                        clusterTopology = new ClusterTopology(clusterTopology.TopologyId, clusterTopology.ApiKey,
                            clusterTopology.Voters.Except(new[] { node }).ToArray(),
                            clusterTopology.Promotables.Except(new[] { node }).ToArray(),
                            clusterTopology.NonVotingMembers.Concat(new[] { node }).ToArray()
                        );
                        break;
                    case TopologyModification.Remove:
                        clusterTopology = new ClusterTopology(clusterTopology.TopologyId, clusterTopology.ApiKey,
                            clusterTopology.Voters.Except(new[] { node }).ToArray(),
                            clusterTopology.Promotables.Except(new[] { node }).ToArray(),
                            clusterTopology.NonVotingMembers.Except(new[] { node }).ToArray()
                        );
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(modification), modification, null);
                }

                var topologyJson = _engine.SetTopology(context, clusterTopology);

                var index = _engine.InsertToLeaderLog(context, topologyJson, RachisEntryFlags.Topology);
                _entries[index] = tcs = new TaskCompletionSource<object>();
                _topologyModification = tcs.Task.ContinueWith(_ =>
                {
                    _topologyModification = null;
                });
                context.Transaction.Commit();
            }

            _newEntry.Set();

            RefreshAmbassadors(clusterTopology);

            task = tcs.Task;
            return true;
        }
    }
}