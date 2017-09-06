using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using Sparrow.Collections.LockFree;
using Sparrow.Threading;
using Voron.Impl;
using Voron.Impl.Extensions;

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

        private TaskCompletionSource<object> _newEntriesArrived = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly ConcurrentDictionary<long, CommandState> _entries =
            new ConcurrentDictionary<long, CommandState>();

        private class CommandState
        {
            public long CommandIndex;
            public object Result;
            public TaskCompletionSource<(long, object)> TaskCompletionSource;
            public Action<TaskCompletionSource<(long, object)>> OnNotify;
        }

        private MultipleUseFlag _hasNewTopology = new MultipleUseFlag();
        private readonly ManualResetEvent _newEntry = new ManualResetEvent(false);
        private readonly ManualResetEvent _voterResponded = new ManualResetEvent(false);
        private readonly ManualResetEvent _promotableUpdated = new ManualResetEvent(false);
        private readonly ManualResetEvent _shutdownRequested = new ManualResetEvent(false);
        private readonly ManualResetEvent _noop = new ManualResetEvent(false);
        private long _lowestIndexInEntireCluster;

        private readonly Dictionary<string, FollowerAmbassador> _voters =
            new Dictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, FollowerAmbassador> _promotables =
            new Dictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, FollowerAmbassador> _nonVoters =
            new Dictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);

        private Thread _thread;

        public long LowestIndexInEntireCluster
        {
            get { return _lowestIndexInEntireCluster; }
            set { Interlocked.Exchange(ref _lowestIndexInEntireCluster, value); }
        }

        public long Term => _engine.CurrentTerm;
        public Leader(RachisConsensus engine)
        {
            _engine = engine;
        }

        private MultipleUseFlag _running = new MultipleUseFlag();
        public bool Running => _running.IsRaised();

        public void Start()
        {
            _running.Raise();
            ClusterTopology clusterTopology;
            using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                clusterTopology = _engine.GetTopology(context);
            }

            RefreshAmbassadors(clusterTopology);

            _thread = new Thread(Run)
            {
                Name =
                    $"Consensus Leader - {_engine.Tag} in term {_engine.CurrentTerm}",
                IsBackground = true
            };
            _thread.Start();
        }

        public void StepDown()
        {
            if (_voters.Count == 0)
                throw new InvalidOperationException("Cannot step down when I'm the only voter in the cluster");
            var nextLeader = _voters.Values.OrderByDescending(x => x.FollowerMatchIndex).ThenByDescending(x => x.LastReplyFromFollower).First();
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"Stepping as down as leader and will ask {nextLeader} to become the next leader");
            }
            nextLeader.ForceElectionsNow = true;
            var old = Interlocked.Exchange(ref _newEntriesArrived, new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously));
            old.TrySetResult(null);
        }



        public Dictionary<string, NodeStatus> GetStatus()
        {
            var dict = new Dictionary<string, NodeStatus>();

            foreach (var peers in new[] { _nonVoters, _voters, _promotables })
            {
                foreach (var kvp in peers)
                {
                    var status = new NodeStatus
                    {
                        Connected = kvp.Value.Status == AmbassadorStatus.Connected,
                        LastMatchingIndex = kvp.Value.FollowerMatchIndex,
                        LastReply = kvp.Value.LastReplyFromFollower,
                        LastSent = kvp.Value.LastSendToFollower,
                        LastSentMessage = kvp.Value.LastSendMsg
                    };

                    if (status.Connected == false)
                    {
                        status.ErrorDetails = kvp.Value.StatusMessage;
                    }

                    dict[kvp.Key] = status;
                }
            }

            return dict;
        }

        private void RefreshAmbassadors(ClusterTopology clusterTopology)
        {
            bool lockTaken = false;
            Monitor.TryEnter(this, ref lockTaken);
            try
            {
                //This only means we are been disposed so we can quit now
                if (lockTaken == false)
                {
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"Leader {_engine.Tag}: Skipping refreshing ambassadors because we are been disposed of");
                    }
                    return;
                }
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info($"Leader {_engine.Tag}: Refreshing ambassadors");
                }
                var old = new Dictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);
                foreach (var peers in new[] { _voters, _promotables, _nonVoters })
                {
                    foreach (var peer in peers)
                    {
                        old[peer.Key] = peer.Value;
                    }
                    peers.Clear();
                }

                foreach (var voter in clusterTopology.Members)
                {
                    if (voter.Key == _engine.Tag)
                        continue; // we obviously won't be applying to ourselves

                    if (old.TryGetValue(voter.Key, out FollowerAmbassador existingInstance))
                    {
                        existingInstance.UpdateLeaderWake(_voterResponded);
                        _voters.Add(voter.Key, existingInstance);
                        old.Remove(voter.Key);
                        continue; // already here
                    }

                    var ambasaddor = new FollowerAmbassador(_engine, this, _voterResponded, voter.Key, voter.Value,
                        _engine.ClusterCertificate);
                    _voters.Add(voter.Key, ambasaddor);
                    _engine.AppendStateDisposable(this, ambasaddor);
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"Leader {_engine.Tag}: starting ambassador for voter {voter.Key} {voter.Value}");
                    }
                    ambasaddor.Start();
                }

                foreach (var promotable in clusterTopology.Promotables)
                {
                    if (old.TryGetValue(promotable.Key, out FollowerAmbassador existingInstance))
                    {
                        existingInstance.UpdateLeaderWake(_promotableUpdated);
                        _promotables.Add(promotable.Key, existingInstance);
                        old.Remove(promotable.Key);
                        continue; // already here
                    }

                    var ambasaddor = new FollowerAmbassador(_engine, this, _promotableUpdated, promotable.Key, promotable.Value,
                        _engine.ClusterCertificate);
                    _promotables.Add(promotable.Key, ambasaddor);
                    _engine.AppendStateDisposable(this, ambasaddor);
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"Leader {_engine.Tag}: starting ambassador for promotable {promotable.Key} {promotable.Value}");
                    }
                    ambasaddor.Start();
                }

                foreach (var nonVoter in clusterTopology.Watchers)
                {
                    if (_nonVoters.TryGetValue(nonVoter.Key, out FollowerAmbassador existingInstance))
                    {
                        existingInstance.UpdateLeaderWake(_noop);

                        _nonVoters.Add(nonVoter.Key, existingInstance);
                        old.Remove(nonVoter.Key);
                        continue; // already here
                    }
                    var ambasaddor = new FollowerAmbassador(_engine, this, _noop, nonVoter.Key, nonVoter.Value,
                        _engine.ClusterCertificate);
                    _nonVoters.Add(nonVoter.Key, ambasaddor);
                    _engine.AppendStateDisposable(this, ambasaddor);
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"Leader {_engine.Tag}: starting ambassador for watcher {nonVoter.Key} {nonVoter.Value}");
                    }
                    ambasaddor.Start();
                }
                ThreadPool.QueueUserWorkItem(_ =>
                {
                    foreach (var ambasaddor in old)
                    {
                        // it is not used by anything else, so we can close it
                        ambasaddor.Value.Dispose();
                    }
                }, null);
            }
            finally
            {
                if (lockTaken)
                    Monitor.Exit(this);
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
                using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                using (var cmd = context.ReadObject(noopCmd, "noop-cmd"))
                {
                    _engine.InsertToLeaderLog(context, cmd, RachisEntryFlags.Noop);
                    tx.Commit();
                }
                _newEntry.Set(); //This is so the noop would register right away
                while (_running)
                {
                    switch (WaitHandle.WaitAny(handles, _engine.ElectionTimeout))
                    {
                        case 0: // new entry
                            _newEntry.Reset();
                            // release any waiting ambassadors to send immediately
                            TaskExecutor.CompleteAndReplace(ref _newEntriesArrived);
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
                            break;
                        case 3: // shutdown requested
                            if (_engine.Log.IsInfoEnabled && _voters.Count != 0)
                            {
                                _engine.Log.Info($"Leader {_engine.Tag}: shutting down");
                            }
                            return;
                    }

                    EnsureThatWeHaveLeadership(VotersMajority);
                    _engine.ReportLeaderTime(LeaderShipDuration);

                    var lowestIndexInEntireCluster = GetLowestIndexInEntireCluster();
                    if (lowestIndexInEntireCluster != LowestIndexInEntireCluster)
                    {
                        using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        using (context.OpenWriteTransaction())
                        {
                            _engine.TruncateLogBefore(context, lowestIndexInEntireCluster);
                            LowestIndexInEntireCluster = lowestIndexInEntireCluster;
                            context.Transaction.Commit();
                        }
                    }
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
                    _engine.SwitchToCandidateState("An error occurred during our leadership." + Environment.NewLine + e);
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
            if (_engine.Timeout.Disable)
                return;

            var sb = new StringBuilder();
            var now = DateTime.UtcNow;
            sb.AppendLine("Triggered because of:");
            foreach (var timeoutsForVoter in _timeoutsForVoters)
            {
                sb.Append($"\t{timeoutsForVoter.voter.Tag} - {Math.Round(timeoutsForVoter.time.TotalMilliseconds, 3)} ms").AppendLine();
            }
            foreach (var ambassador in _voters)
            {
                var followerAmbassador = ambassador.Value;
                var sinceLastReply = (long)(now - followerAmbassador.LastReplyFromFollower).TotalMilliseconds;
                var sinceLastSend = (long)(now - followerAmbassador.LastSendToFollower).TotalMilliseconds;
                var lastMsg = followerAmbassador.LastSendMsg;
                sb.AppendLine(
                    $"{followerAmbassador.Tag}: Got last reply {sinceLastReply:#,#;;0} ms ago and sent {sinceLastSend:#,#;;0} ms ({lastMsg}) - {followerAmbassador.StatusMessage} - {followerAmbassador.ThreadStatus}");
            }


            if (_engine.Log.IsInfoEnabled && _voters.Count != 0)
            {
                _engine.Log.Info($"Leader {_engine.Tag}:VoteOfNoConfidence{Environment.NewLine } {sb}");
            }
            throw new TimeoutException(
                "Too long has passed since we got a confirmation from the majority of the cluster that this node is still the leader." +
                Environment.NewLine +
                "Assuming that I'm not the leader and stepping down." +
                Environment.NewLine +
                sb
                );
        }

        private long _lastCommit;
        private void OnVoterConfirmation()
        {
            TransactionOperationContext context;
            if (_hasNewTopology.Lower())
            {
                ClusterTopology clusterTopology;
                using (_engine.ContextPool.AllocateOperationContext(out context))
                using (context.OpenReadTransaction())
                {
                    clusterTopology = _engine.GetTopology(context);
                }
                if (clusterTopology.Contains(_engine.LeaderTag) == false)
                {
                    TaskExecutor.CompleteAndReplace(ref _newEntriesArrived);
                    _engine.SetNewState(RachisConsensus.State.Passive, this, _engine.CurrentTerm,
                        "I was kicked out of the cluster and moved to passive mode");
                    return;
                }
                RefreshAmbassadors(clusterTopology);
            }

            var maxIndexOnQuorum = GetMaxIndexOnQuorum(VotersMajority);

            if (_lastCommit >= maxIndexOnQuorum ||
                maxIndexOnQuorum == 0)
                return; // nothing to do here

            bool changedFromLeaderElectToLeader;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (context.OpenWriteTransaction())
            {
                _lastCommit = _engine.GetLastCommitIndex(context);

                if (_lastCommit >= maxIndexOnQuorum ||
                    maxIndexOnQuorum == 0)
                    return; // nothing to do here

                if (_engine.GetTermForKnownExisting(context, maxIndexOnQuorum) < _engine.CurrentTerm)
                    return;// can't commit until at least one entry from our term has been published

                changedFromLeaderElectToLeader = _engine.TakeOffice();

                _engine.Apply(context, maxIndexOnQuorum, this);

                context.Transaction.Commit();

                _lastCommit = maxIndexOnQuorum;
            }

            foreach (var kvp in _entries)
            {
                if (kvp.Key > _lastCommit)
                    continue;

                if (_entries.TryRemove(kvp.Key, out CommandState value))
                {
                    TaskExecutor.Execute(o =>
                    {
                        var tuple = (CommandState)o;
                        if (tuple.OnNotify != null)
                        {
                            tuple.OnNotify(tuple.TaskCompletionSource);
                            return;
                        }
                        tuple.TaskCompletionSource.TrySetResult((tuple.CommandIndex, tuple.Result));
                    }, value);
                }
            }

            if (_entries.Count != 0)
            {
                // we have still items to process, run them in 1 node cluster
                // and speed up the followers ambassadors if they can
                _newEntry.Set();
            }

            if (changedFromLeaderElectToLeader)
                _engine.LeaderElectToLeaderChanged();
        }

        private readonly List<(FollowerAmbassador voter, TimeSpan time)> _timeoutsForVoters = new List<(FollowerAmbassador, TimeSpan)>();
        private void EnsureThatWeHaveLeadership(int majority)
        {
            var now = DateTime.UtcNow;
            var peersHeardFromInElectionTimeout = 1; // we count as a node :-)
            _timeoutsForVoters.Clear();
            foreach (var voter in _voters.Values)
            {
                var time = (now - voter.LastReplyFromFollower);
                _timeoutsForVoters.Add((voter, time));
                if (time < _engine.ElectionTimeout)
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
        private readonly SortedList<long, int> _nodesPerIndex = new SortedList<long, int>();

        private readonly Stopwatch _leadership = Stopwatch.StartNew();
        private int VotersMajority => _voters.Count / 2 + 1;

        public long LeaderShipDuration => _leadership.ElapsedMilliseconds;

        protected long GetLowestIndexInEntireCluster()
        {
            long lowestIndex;
            using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                lowestIndex = _engine.GetLastEntryIndex(context);
            }

            foreach (var voter in _voters.Values)
            {
                lowestIndex = Math.Min(lowestIndex, voter.FollowerMatchIndex);
            }

            foreach (var promotable in _promotables.Values)
            {
                lowestIndex = Math.Min(lowestIndex, promotable.FollowerMatchIndex);
            }

            foreach (var nonVoter in _nonVoters.Values)
            {
                lowestIndex = Math.Min(lowestIndex, nonVoter.FollowerMatchIndex);
            }

            return lowestIndex;
        }

        protected long GetMaxIndexOnQuorum(int minSize)
        {
            _nodesPerIndex.Clear();
            using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                _nodesPerIndex[_engine.GetLastEntryIndex(context)] = 1;
            }

            foreach (var voter in _voters.Values)
            {
                var voterIndex = voter.FollowerMatchIndex;
                _nodesPerIndex.TryGetValue(voterIndex, out int count);
                _nodesPerIndex[voterIndex] = count + 1;
            }
            var votesSoFar = 0;

            for (int i = _nodesPerIndex.Count - 1; i >= 0; i--)
            {
                votesSoFar += _nodesPerIndex.Values[i];
                if (votesSoFar >= minSize)
                    return _nodesPerIndex.Keys[i];
            }
            return 0;
        }

        private void CheckPromotables()
        {
            long lastIndex;
            using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                lastIndex = _engine.GetLastEntryIndex(context);
            }

            foreach (var ambassador in _promotables)
            {
                if (ambassador.Value.FollowerMatchIndex != lastIndex)
                    continue;

                TryModifyTopology(ambassador.Key, ambassador.Value.Url, TopologyModification.Voter, out Task _);

                _promotableUpdated.Set();
                break;
            }
        }

        public Task<(long Index, object Result)> PutAsync(CommandBase cmd)
        {
            Task<(long Index, object Result)> task;
            using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenWriteTransaction()) // this line prevents concurrency issues on the PutAsync
            {
                var djv = cmd.ToJson(context);
                var cmdJson = context.ReadObject(djv, "raft/command");

                var index = _engine.InsertToLeaderLog(context, cmdJson, RachisEntryFlags.StateMachineCommand);
                context.Transaction.Commit();
                task = AddToEntries(index);
            }

            _newEntry.Set();
            return task;
        }

        public Task<(long Index, object Result)> AddToEntries(long index)
        {
            var tcs = new TaskCompletionSource<(long Index, object Result)>(TaskCreationOptions.RunContinuationsAsynchronously);
            _entries[index] =
                new
                    CommandState // we need to add entry inside write tx lock to omit a situation when command will be applied (and state set) before it is added to the entries list
                    {
                        CommandIndex = index,
                        TaskCompletionSource = tcs
                    };
            return tcs.Task;
        }

        public System.Collections.Concurrent.ConcurrentQueue<(string node, AlertRaised error)> ErrorsList = new System.Collections.Concurrent.ConcurrentQueue<(string, AlertRaised)>();

        public void NotifyAboutException(FollowerAmbassador node, Exception e)
        {
            var alert = AlertRaised.Create($"Node {node.Tag} encountered an error",
                node.StatusMessage,
                AlertType.ClusterTopologyWarning,
                NotificationSeverity.Warning,
                details: new ExceptionDetails(e));
            ErrorsList.Enqueue((node.Tag, alert));
            ErrorsList.Reduce(25);
        }

        public void NotifyAboutNodeStatusChange()
        {
            ClusterTopology clusterTopology;
            try
            {
                using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    clusterTopology = _engine.GetTopology(context);
                }
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }
            OnNodeStatusChange?.Invoke(this, clusterTopology);
        }

        public event EventHandler<ClusterTopology> OnNodeStatusChange;

        public void Dispose()
        {
            bool lockTaken = false;
            Monitor.TryEnter(this, ref lockTaken);
            try
            {
                if (lockTaken == false)
                {
                    //We need to wait that refresh ambassador finish
                    if (Monitor.Wait(this, TimeSpan.FromSeconds(15)) == false)
                    {
                        var message = $"Leader {_engine.Tag}: Refresh ambassador is taking the lock for 15 sec giving up on leader dispose";
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info(message);
                        }
                        throw new TimeoutException(message);
                    }
                }
                _running.Lower();
                _shutdownRequested.Set();
                TaskExecutor.Execute(_ =>
                {
                    _newEntriesArrived.TrySetCanceled();
                    var lastStateChangeReason = _engine.LastStateChangeReason;
                    TimeoutException te = null;
                    if (string.IsNullOrEmpty(lastStateChangeReason) == false)
                        te = new TimeoutException(lastStateChangeReason);
                    foreach (var entry in _entries)
                    {
                        if (te == null)
                        {
                            entry.Value.TaskCompletionSource.TrySetCanceled();
                        }
                        else
                        {
                            entry.Value.TaskCompletionSource.TrySetException(te);
                        }
                    }
                }, null);

                if (_thread != null && _thread.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
                    _thread.Join();

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


                _newEntry.Dispose();
                _voterResponded.Dispose();
                _promotableUpdated.Dispose();
                _shutdownRequested.Dispose();
                _noop.Dispose();
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info($"Leader {_engine.Tag}: Dispose");
                }
            }
            finally
            {
                if (lockTaken)
                    Monitor.Exit(this);
            }


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

        public bool TryModifyTopology(string nodeTag, string nodeUrl, TopologyModification modification, out Task task, bool validateNotInTopology = false, Action<TransactionOperationContext> beforeCommit = null)
        {
            using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenWriteTransaction())
            {
                var existing = Interlocked.CompareExchange(ref _topologyModification, null, null);
                if (existing != null)
                {
                    task = existing;
                    return false;
                }

                var clusterTopology = _engine.GetTopology(context);

                //We need to validate that the node doesn't exists before we generate the nodeTag
                if (validateNotInTopology && (nodeTag != null && clusterTopology.Contains(nodeTag) || clusterTopology.TryGetNodeTagByUrl(nodeUrl).HasUrl))
                {
                    throw new InvalidOperationException($"Was requested to modify the topology for node={nodeTag} " +
                                                        "with validation that it is not contained by the topology but current topology contains it.");
                }

                if (nodeTag == null)
                {
                    nodeTag = GenerateNodeTag(clusterTopology);
                }

                var newVotes = new Dictionary<string, string>(clusterTopology.Members);
                newVotes.Remove(nodeTag);
                var newPromotables = new Dictionary<string, string>(clusterTopology.Promotables);
                newPromotables.Remove(nodeTag);
                var newNonVotes = new Dictionary<string, string>(clusterTopology.Watchers);
                newNonVotes.Remove(nodeTag);

                var highestNodeId = newVotes.Keys.Concat(newPromotables.Keys).Concat(newNonVotes.Keys).Concat(new[] { nodeTag }).Max();

                switch (modification)
                {
                    case TopologyModification.Voter:
                        Debug.Assert(nodeUrl != null);
                        newVotes[nodeTag] = nodeUrl;
                        break;
                    case TopologyModification.Promotable:
                        Debug.Assert(nodeUrl != null);
                        newPromotables[nodeTag] = nodeUrl;
                        break;
                    case TopologyModification.NonVoter:
                        Debug.Assert(nodeUrl != null);
                        newNonVotes[nodeTag] = nodeUrl;
                        break;
                    case TopologyModification.Remove:
                        if (clusterTopology.Contains(nodeTag) == false)
                        {
                            throw new InvalidOperationException($"Was requested to remove node={nodeTag} from the topology " +
                                                        "but it is not contained by the topology.");
                        }
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(modification), modification, null);
                }

                clusterTopology = new ClusterTopology(
                    clusterTopology.TopologyId,
                    newVotes,
                    newPromotables,
                    newNonVotes,
                    highestNodeId
                );

                var topologyJson = _engine.SetTopology(context, clusterTopology);
                var index = _engine.InsertToLeaderLog(context, topologyJson, RachisEntryFlags.Topology);

                if (modification == TopologyModification.Remove)
                {
                    _engine.GetStateMachine().EnsureNodeRemovalOnDeletion(context,nodeTag);
                }

                context.Transaction.Commit();

                var tcs = new TaskCompletionSource<(long Index, object Result)>(TaskCreationOptions.RunContinuationsAsynchronously);
                _entries[index] = new CommandState
                {
                    TaskCompletionSource = tcs,
                    CommandIndex = index
                };

                _topologyModification = task = tcs.Task.ContinueWith(_ =>
                {
                    Interlocked.Exchange(ref _topologyModification, null);
                });
            }
            _hasNewTopology.Raise();
            _voterResponded.Set();
            _newEntry.Set();

            return true;
        }

       

        private static string GenerateNodeTag(ClusterTopology clusterTopology)
        {
            if (clusterTopology.LastNodeId.Length == 0)
            {
                return "A";
            }

            //TODO: handle properly
            if (clusterTopology.LastNodeId[clusterTopology.LastNodeId.Length - 1] + 1 > 'Z')
            {
                return clusterTopology.LastNodeId + "A";
            }

            var lastChar = (char)(clusterTopology.LastNodeId[clusterTopology.LastNodeId.Length - 1] + 1);
            return clusterTopology.LastNodeId.Substring(0, clusterTopology.LastNodeId.Length - 1) + lastChar;
        }

        public void SetStateOf(long index, Action<TaskCompletionSource<(long Index, object Result)>> onNotify)
        {
            if (_entries.TryGetValue(index, out CommandState value))
            {
                value.OnNotify = onNotify;
            }
        }

        public void SetStateOf(long index, object result)
        {
            if (_entries.TryGetValue(index, out CommandState value))
            {
                value.Result = result;
            }
        }
    }
}
