using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Rachis
{
    /// <summary>
    /// This class implements the leader behavior. Note that only a single thread
    /// actually does work in here, the leader thread. All other work is requested 
    /// from it and it is done
    /// </summary>
    public class Leader : IDisposable
    {
        private readonly RachisConsensus _engine;

        private TaskCompletionSource<object> _newEntriesArrived = new TaskCompletionSource<object>();

        private readonly ConcurrentDictionary<long, TaskCompletionSource<object>> _entries =
            new ConcurrentDictionary<long, TaskCompletionSource<object>>();

        private readonly ManualResetEvent _newEntry = new ManualResetEvent(false);
        private readonly ManualResetEvent _voterResponded = new ManualResetEvent(false);
        private readonly ManualResetEvent _promotableUpdated = new ManualResetEvent(false);
        private readonly ManualResetEvent _shutdownRequested = new ManualResetEvent(false);
        private readonly ManualResetEvent _noop = new ManualResetEvent(false);

        private Dictionary<string, FollowerAmbassador> _voters =
            new Dictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);
        private Dictionary<string, FollowerAmbassador> _promotables =
            new Dictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);
        private Dictionary<string, FollowerAmbassador> _nonVoters =
            new Dictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);

        private Thread _thread;

        public Leader(RachisConsensus engine)
        {
            _engine = engine;
        }

        public void Start()
        {
            ClusterTopology clusterTopology;
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                clusterTopology = _engine.GetTopology(context);
            }

            RefreshAmbasaddors(clusterTopology);

            _thread = new Thread(Run)
            {
                Name = "Consensus Leader - " + _engine.Url,
                IsBackground = true
            };
            _thread.Start();
        }

        private void RefreshAmbasaddors(ClusterTopology clusterTopology)
        {
            var old = _voters;
            _voters = new Dictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);
            foreach (var voter in clusterTopology.Voters)
            {
                if (voter == _engine.Url)
                    continue; // we obviously won't be applying to ourselves

                FollowerAmbassador existingInstance;
                if (old.TryGetValue(voter, out existingInstance))
                {
                    _voters.Add(voter, existingInstance);
                    old.Remove(voter);
                    continue; // already here
                }

                var ambasaddor = new FollowerAmbassador(_engine, this, _voterResponded, voter, clusterTopology.ApiKey);
                _voters.Add(voter, ambasaddor);
                _engine.AppendStateDisposable(this, ambasaddor);
                ambasaddor.Start();
            }

            foreach (var ambasaddor in _promotables)
            {
                old.Add(ambasaddor.Key, ambasaddor.Value);
            }

            _promotables = new Dictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);
            foreach (var promotable in clusterTopology.Promotables)
            {
                FollowerAmbassador existingInstance;
                if (old.TryGetValue(promotable, out existingInstance))
                {
                    _promotables.Add(promotable, existingInstance);
                    old.Remove(promotable);
                    continue; // already here
                }

                var ambasaddor = new FollowerAmbassador(_engine, this, _promotableUpdated, promotable, clusterTopology.ApiKey);
                _promotables.Add(promotable, ambasaddor);
                _engine.AppendStateDisposable(this, ambasaddor);
                ambasaddor.Start();
            }

            foreach (var ambasaddor in _nonVoters)
            {
                old.Add(ambasaddor.Key, ambasaddor.Value);
            }

            _nonVoters = new Dictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);

            foreach (var nonVoter in clusterTopology.NonVotingMembers)
            {
                FollowerAmbassador existingInstnace;
                if (_nonVoters.TryGetValue(nonVoter, out existingInstnace))
                {
                    _nonVoters.Add(nonVoter, existingInstnace);
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
        private unsafe void Run()
        {
            using (this)
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

                    TransactionOperationContext context;
                    using (_engine.ContextPool.AllocateOperationContext(out context))
                    using (var tx = context.OpenWriteTransaction())
                    {
                        _engine.InsertToLog(context, new BlittableJsonReaderObject(null, 0, context),
                            RachisEntryFlags.Noop);
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
                                VoteOfNoConfidence();
                                break;
                            case 3: // shutdown requested
                                return;
                        }

                        EnsureThatWeHaveLeadership((_voters.Count / 2) + 1);
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
        }

        private void VoteOfNoConfidence()
        {
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

                if (_engine.GetTermFor(_lastCommit) < _engine.CurrentTerm)
                    return;// can't commit until at least one entry from our term has been published

                if (_lastCommit == maxIndexOnQuorum)
                    return; // nothing to do here

                _engine.StateMachine.Apply(context, maxIndexOnQuorum);
                _lastCommit = maxIndexOnQuorum;

                context.Transaction.Commit();
            }

            foreach (var kvp in _entries)
            {
                if(kvp.Key > _lastCommit)
                    continue;

                TaskCompletionSource<object> value;
                if (_entries.TryRemove(kvp.Key, out value))
                {
                    value.TrySetResult(null);
                }
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
        protected long GetMaxIndexOnQuorum(int minSize)
        {
            _votersPerIndex.Clear();

            foreach (var voter in _voters.Values)
            {
                int count;
                var voterIndex = voter.FollowerMatchIndex;
                _votersPerIndex.TryGetValue(voterIndex, out count);
                var indexOfKey = _votersPerIndex.IndexOfKey(voterIndex);
                if (indexOfKey == -1)
                    _votersPerIndex.Add(voterIndex, 1);
                else
                    _votersPerIndex.Values[indexOfKey]++;
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
            ClusterTopology clusterTopology;
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                clusterTopology = _engine.GetTopology(context);
                lastIndex = _engine.GetLogEntriesRange(context).Item2;
            }

            bool hasChanges = false;

            foreach (var ambasaddor in _promotables)
            {
                if (ambasaddor.Value.FollowerMatchIndex == lastIndex)
                {
                    hasChanges = true;
                    clusterTopology = new ClusterTopology(
                        clusterTopology.TopologyId,
                        clusterTopology.ApiKey,
                        clusterTopology.Voters.Concat(new[] { ambasaddor.Value.Url }).ToArray(),
                        clusterTopology.Promotables.Except(new[] { ambasaddor.Value.Url }).ToArray(),
                        clusterTopology.NonVotingMembers
                    );
                }
            }

            if (hasChanges == false)
                return;

            ChangeTopology(clusterTopology);
        }

        private void ChangeTopology(ClusterTopology clusterTopology)
        {
            RefreshAmbasaddors(clusterTopology);
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (context.OpenWriteTransaction())
            {
                var json = _engine.SetTopology(context, clusterTopology);
                _engine.InsertToLog(context, json, RachisEntryFlags.Topology);
                _newEntry.Set();
            }
        }

        public Task PutAsync(BlittableJsonReaderObject cmd)
        {
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (context.OpenWriteTransaction())
            {
                var index = _engine.InsertToLog(context, cmd, RachisEntryFlags.Topology);
                var tcs = _entries.GetOrAdd(index, _ => new TaskCompletionSource<object>());
                _newEntry.Set();
                return tcs.Task;
            }
        }

        public void Dispose()
        {
            _shutdownRequested.Set();
            _newEntriesArrived.TrySetCanceled();
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
    }
}