using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Extensions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using Sparrow.Json;
using Sparrow.Threading;
using Voron.Exceptions;
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
        private TaskCompletionSource<object> _topologyModification;
        private readonly RachisConsensus _engine;

        public delegate object ConvertResultFromLeader(JsonOperationContext ctx, object result);

        private TaskCompletionSource<object> _newEntriesArrived = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly ConcurrentDictionary<long, CommandState> _entries = new ConcurrentDictionary<long, CommandState>();

        private MultipleUseFlag _hasNewTopology = new MultipleUseFlag();
        private readonly ManualResetEvent _newEntry = new ManualResetEvent(false);
        private readonly ManualResetEvent _voterResponded = new ManualResetEvent(false);
        private readonly ManualResetEvent _promotableUpdated = new ManualResetEvent(false);
        private readonly ManualResetEvent _shutdownRequested = new ManualResetEvent(false);
        private readonly ManualResetEvent _noop = new ManualResetEvent(false);
        private long _lowestIndexInEntireCluster;

        private readonly ConcurrentDictionary<string, FollowerAmbassador> _voters =
            new ConcurrentDictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, FollowerAmbassador> _promotables =
            new ConcurrentDictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, FollowerAmbassador> _nonVoters =
            new ConcurrentDictionary<string, FollowerAmbassador>(StringComparer.OrdinalIgnoreCase);

        public ConcurrentDictionary<string, int> PeersVersion = new ConcurrentDictionary<string, int>();

        private PoolOfThreads.LongRunningWork _leaderLongRunningWork;

        private int _previousPeersWereDisposed;
        
        public long LowestIndexInEntireCluster
        {
            get { return _lowestIndexInEntireCluster; }
            set { Interlocked.Exchange(ref _lowestIndexInEntireCluster, value); }
        }

        public readonly long Term;
        public Leader(RachisConsensus engine, long term)
        {
            Term = term;
            _engine = engine;
        }

        private MultipleUseFlag _running = new MultipleUseFlag();
        public bool Running => _running.IsRaised();

        public void Start(Dictionary<string, RemoteConnection> connections = null)
        {
            _running.Raise();

            ClusterTopology clusterTopology;
            using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                clusterTopology = _engine.GetTopology(context);
            }

            RefreshAmbassadors(clusterTopology, connections);

            _leaderLongRunningWork =
                PoolOfThreads.GlobalRavenThreadPool.LongRunning(x => Run(), null, $"Consensus Leader - {_engine.Tag} in term {Term}");                
        }

        private int _steppedDown;

        public void StepDown()
        {
            if (_voters.Count == 0)
                throw new InvalidOperationException("Cannot step down when I'm the only voter in the cluster");

            if (Interlocked.CompareExchange(ref _steppedDown, 1, 0) == 1)
                return;

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

        private void RefreshAmbassadors(ClusterTopology clusterTopology, Dictionary<string, RemoteConnection> connections = null)
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
                        _engine.Log.Info($"{ToString()}: Skipping refreshing ambassadors because we are been disposed of");
                    }
                    return;
                }
                
                if (Term != _engine.CurrentTerm)
                {
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"{ToString()}: We are no longer the actual leader, since the current term is {_engine.CurrentTerm}");
                    }
                    return;
                }
                
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info($"{ToString()}: Refreshing ambassadors");
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
                        _voters[voter.Key] = existingInstance;
                        old.Remove(voter.Key);
                        continue; // already here
                    }
                    RemoteConnection connection = null;
                    connections?.TryGetValue(voter.Key, out connection);
                    var ambassador = new FollowerAmbassador(_engine, this, _voterResponded, voter.Key, voter.Value, connection);
                    _voters[voter.Key] = ambassador;
                    _engine.AppendStateDisposable(this, ambassador);
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"{ToString()}: starting ambassador for voter {voter.Key} {voter.Value}");
                    }
                    ambassador.Start();
                }

                foreach (var promotable in clusterTopology.Promotables)
                {
                    if (old.TryGetValue(promotable.Key, out FollowerAmbassador existingInstance))
                    {
                        existingInstance.UpdateLeaderWake(_promotableUpdated);
                        _promotables[promotable.Key] = existingInstance;
                        old.Remove(promotable.Key);
                        continue; // already here
                    }
                    RemoteConnection connection = null;
                    connections?.TryGetValue(promotable.Key, out connection);
                    var ambassador = new FollowerAmbassador(_engine, this, _promotableUpdated, promotable.Key, promotable.Value, connection);
                    _promotables[promotable.Key] = ambassador;
                    _engine.AppendStateDisposable(this, ambassador);
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"{ToString()}: starting ambassador for promotable {promotable.Key} {promotable.Value}");
                    }
                    ambassador.Start();
                }

                foreach (var nonVoter in clusterTopology.Watchers)
                {
                    if (old.TryGetValue(nonVoter.Key, out FollowerAmbassador existingInstance))
                    {
                        existingInstance.UpdateLeaderWake(_noop);

                        _nonVoters[nonVoter.Key] = existingInstance;
                        old.Remove(nonVoter.Key);
                        continue; // already here
                    }
                    RemoteConnection connection = null;
                    connections?.TryGetValue(nonVoter.Key, out connection);
                    var ambassador = new FollowerAmbassador(_engine, this, _noop, nonVoter.Key, nonVoter.Value, connection);
                    _nonVoters[nonVoter.Key] = ambassador;
                    _engine.AppendStateDisposable(this, ambassador);
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"{ToString()}: starting ambassador for watcher {nonVoter.Key} {nonVoter.Value}");
                    }
                    ambassador.Start();
                }
                
                if (old.Count > 0)
                {
                    foreach (var ambassador in old)
                    {
                        _voters.TryRemove(ambassador.Key, out _);
                        _nonVoters.TryRemove(ambassador.Key, out _);
                        _promotables.TryRemove(ambassador.Key, out _);
                    }
                    Interlocked.Increment(ref _previousPeersWereDisposed);
                    System.Threading.ThreadPool.QueueUserWorkItem(_ =>
                    {
                        foreach (var ambassador in old)
                        {
                            // it is not used by anything else, so we can close it
                            ambassador.Value.Dispose();
                        }
                        Interlocked.Decrement(ref _previousPeersWereDisposed);
                    }, null);
                }
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
                    _engine.InsertToLeaderLog(context, Term, cmd, RachisEntryFlags.Noop);
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
                                _engine.Log.Info($"{ToString()}: shutting down");
                            }
                            _running.Lower();
                            return;
                    }

                    EnsureThatWeHaveLeadership(VotersMajority);
                    _engine.ReportLeaderTime(LeaderShipDuration);

                    // don't truncate if we are disposing an old peer
                    // otherwise he would not receive notification that he was 
                    // kick out of the cluster
                    if(_previousPeersWereDisposed > 0) // Not Interlocked, because the race here is not interesting. 
                        continue;
                    
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

                if (e is VoronErrorException)
                {
                    _engine.Notify(AlertRaised.Create(
                        null,
                        "Error when running leader behavior",
                        e.Message,
                        AlertType.ClusterTopologyWarning,
                        NotificationSeverity.Error, details: new ExceptionDetails(e)));
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

            _engine.Timeout.DisableTimeout();

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
                _engine.Log.Info($"{ToString()}:VoteOfNoConfidence{Environment.NewLine } {sb}");
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
                    _engine.SetNewState(RachisState.Passive, this, Term,
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

                if (_engine.GetTermForKnownExisting(context, maxIndexOnQuorum) < Term)
                    return;// can't commit until at least one entry from our term has been published

                changedFromLeaderElectToLeader = _engine.TakeOffice();

                maxIndexOnQuorum = _engine.Apply(context, maxIndexOnQuorum, this, Stopwatch.StartNew());

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

            // we have still items to process, run them in 1 node cluster
            // and speed up the followers ambassadors if they can
            _newEntry.Set();
            
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
        /// So we have 2 nodes that have 3, so that is the quorum.
        /// </summary>
        private readonly SortedList<long, int> _nodesPerIndex = new SortedList<long, int>();

        private readonly Stopwatch _leadership = Stopwatch.StartNew();
        private int VotersMajority => (_voters.Count + 1) / 2 + 1;

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

        private class RachisMergedCommand
        {
            public CommandBase Command;
            public TaskCompletionSource<Task<(long Index, object Result)>> Tcs;
            public readonly MultipleUseFlag Consumed = new MultipleUseFlag();
        }

        private readonly ConcurrentQueue<RachisMergedCommand> _commandsQueue = new ConcurrentQueue<RachisMergedCommand>();

        private readonly AsyncManualResetEvent _waitForCommit = new AsyncManualResetEvent();

        public async Task<(long Index, object Result)> PutAsync(CommandBase command, TimeSpan timeout)
        {
            var rachisMergedCommand = new RachisMergedCommand
            {
                Command = command,
                Tcs = new TaskCompletionSource<Task<(long, object)>>(TaskCreationOptions.RunContinuationsAsynchronously)
            };
            _commandsQueue.Enqueue(rachisMergedCommand);

            while (rachisMergedCommand.Tcs.Task.IsCompleted == false)
            {
                var lockTaken = false;
                try
                {
                    Monitor.TryEnter(_commandsQueue, ref lockTaken);
                    if (lockTaken)
                    {
                        EmptyQueue();
                    }
                    else
                    {
                        if (await _waitForCommit.WaitAsync(timeout) == false)
                        {
                            if (rachisMergedCommand.Consumed.Raise())
                            {
                                GetConvertResult(command)?.AboutToTimeout();
                                throw new TimeoutException($"Waited for {timeout} but the command was not applied in this time.");
                            }

                            // if the command is already dequeued we must let it continue to keep its context valid. 
                            await rachisMergedCommand.Tcs.Task;
                        }
                    }
                }
                finally
                {
                    if (lockTaken)
                    {
                        Monitor.Exit(_commandsQueue);
                        _waitForCommit.SetAndResetAtomically();
                    }
                }
            }

            var inner = await rachisMergedCommand.Tcs.Task;
            if (await inner.WaitWithTimeout(timeout) == false)
            {
                GetConvertResult(command)?.AboutToTimeout();
                throw new TimeoutException($"Waited for {timeout} but the command was not applied in this time.");
            }

            return await inner;
        }

        private void EmptyQueue()
        {
            var list = new List<TaskCompletionSource<Task<(long, object)>>>();
            var tasks = new List<Task<(long, object)>>();
            var lostLeadershipException = new NotLeadingException("We are no longer the leader, this leader is disposed");
            try
            {
                using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var cmdsCount = 0;
                    while (cmdsCount++ < 128 && _commandsQueue.TryDequeue(out var cmd))
                    {
                        if (cmd.Consumed.Raise() == false)
                        {
                            // if the command was aborted due to timeout, we should skip it.
                            // The command is not appended, so We can and must do so, because the context of the command is no longer valid.
                            continue;
                        }

                        if (_running.IsRaised() == false)
                        {
                            cmd.Tcs.TrySetException(lostLeadershipException);
                            continue;
                        }

                        list.Add(cmd.Tcs);
                        _engine.InvokeBeforeAppendToRaftLog(context, cmd.Command);

                        var djv = cmd.Command.ToJson(context);
                        var cmdJson = context.ReadObject(djv, "raft/command");

                        var index = _engine.InsertToLeaderLog(context, Term, cmdJson, RachisEntryFlags.StateMachineCommand);

                        var tcs = new TaskCompletionSource<(long, object)>(TaskCreationOptions.RunContinuationsAsynchronously);
                        tasks.Add(tcs.Task);
                        var state = new
                            CommandState // we need to add entry inside write tx lock to avoid
                                         // a situation when command will be applied (and state set)
                                         // before it is added to the entries list
                            {
                                CommandIndex = index,
                                TaskCompletionSource = tcs,
                                ConvertResult = GetConvertResult(cmd.Command)
                        };
                        _entries[index] = state;
                    }
                    context.Transaction.Commit();
                }

                if (tasks.Count > 0)
                    _newEntry.Set();

                for (int i = 0; i < tasks.Count; i++)
                {
                    list[i].TrySetResult(tasks[i]);
                }
            }
            catch (Exception e)
            {
                if (_running.IsRaised() == false)
                {
                    e = new NotLeadingException("We are no longer the leader, this leader is disposed", e);
                }
                foreach (var tcs in list)
                {
                    tcs.TrySetException(e);
                }
            }
        }

        internal static ConvertResultAction GetConvertResult(CommandBase cmd)
        {
            switch (cmd)
            {
                case AddOrUpdateCompareExchangeBatchCommand batchCmpExchange:
                    return new ConvertResultAction(batchCmpExchange.ContextToWriteResult, CompareExchangeCommandBase.ConvertResult);
                case CompareExchangeCommandBase cmpExchange:
                    return new ConvertResultAction(cmpExchange.ContextToWriteResult, CompareExchangeCommandBase.ConvertResult);
                default:
                    return null;
            }
        }
        
        public ConcurrentQueue<(string node, AlertRaised error)> ErrorsList = new System.Collections.Concurrent.ConcurrentQueue<(string, AlertRaised)>();

        public void NotifyAboutException(string nodeTag, Exception e)
        {
            var title = $"Node {nodeTag} encountered an error";
            var alert = AlertRaised.Create(
                null,
                title,
                e.Message,
                AlertType.ClusterTopologyWarning,
                NotificationSeverity.Warning,
                key: title,
                details: new ExceptionDetails(e.InnerException));

            _engine.Notify(alert);

            if (ErrorsList.Any(err => err.error.Id == alert.Id) == false)
            {
                ErrorsList.Enqueue((nodeTag, alert));
                ErrorsList.Reduce(25);
            }
        }
        private DisposeLock _disposerLock = new DisposeLock("Leader");
        public void Dispose()
        {
            using (_disposerLock.StartDisposing())
            {
                bool lockTaken = false;
                Monitor.TryEnter(this, ref lockTaken);
                try
                {
                    if (lockTaken == false)
                    {
                        var dueTime = DateTime.UtcNow + TimeSpan.FromSeconds(15);
                        //We need to wait that refresh ambassador finish
                        while (DateTime.UtcNow < dueTime)
                        {
                            Monitor.TryEnter(this, ref lockTaken);
                            if (lockTaken)
                                break;
                            Thread.Yield();
                        }
                        if (lockTaken == false)                        
                        {
                            var message = $"{ToString()}: Refresh ambassador is taking the lock for 15 sec giving up on leader dispose";
                            if (_engine.Log.IsInfoEnabled)
                            {
                                _engine.Log.Info(message);
                            }
                            throw new TimeoutException(message);
                        }
                    }
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"Start disposing leader {_engine.Tag} of term {Term}.");
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

                    if (_leaderLongRunningWork != null && _leaderLongRunningWork.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
                        _leaderLongRunningWork.Join(int.MaxValue);

                    var ae = new ExceptionAggregator("Could not properly dispose Leader");
                    foreach (var ambassador in _nonVoters)
                    {
                        ae.Execute(ambassador.Value.Dispose);
                    }

                    foreach (var ambassador in _promotables)
                    {
                        ae.Execute(ambassador.Value.Dispose);
                    }
                    foreach (var ambassador in _voters)
                    {
                        ae.Execute(ambassador.Value.Dispose);
                    }


                    _newEntry.Dispose();
                    _voterResponded.Dispose();
                    _promotableUpdated.Dispose();
                    _shutdownRequested.Dispose();
                    _noop.Dispose();
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"Leader {_engine.Tag} of term {Term} was disposed");
                    }
                }
                finally
                {
                    if (lockTaken)
                        Monitor.Exit(this);
                }
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
            if (nodeTag != null)
            {
                ValidateNodeTag(nodeTag);
            }

            using (_disposerLock.EnsureNotDisposed())
            {
                var topologyModification = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                var existing = Interlocked.CompareExchange(ref _topologyModification, topologyModification, null);
                if (existing != null)
                {
                    task = existing.Task;
                    return false;
                }
                task = topologyModification.Task;

                using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenWriteTransaction())
                {
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

                    var highestNodeId = newVotes.Keys.Concat(newPromotables.Keys).Concat(newNonVotes.Keys).Concat(new[] {nodeTag}).Max();

                    if (nodeTag == _engine.Tag)
                        RachisTopologyChangeException.Throw("Cannot modify the topology of the leader node.");

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
                            PeersVersion.TryRemove(nodeTag, out _);
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
                    var index = _engine.InsertToLeaderLog(context, Term, topologyJson, RachisEntryFlags.Topology);

                    if (modification == TopologyModification.Remove)
                    {
                        _engine.GetStateMachine().EnsureNodeRemovalOnDeletion(context, Term, nodeTag);
                    }

                    context.Transaction.Commit();

                    var tcs = new TaskCompletionSource<(long Index, object Result)>(TaskCreationOptions.RunContinuationsAsynchronously);
                    _entries[index] = new CommandState
                    {
                        TaskCompletionSource = tcs,
                        CommandIndex = index
                    };

                    tcs.Task.ContinueWith(_ =>
                    {
                        Interlocked.Exchange(ref _topologyModification, null).TrySetResult(null);
                    });
                }
                _hasNewTopology.Raise();
                _voterResponded.Set();
                _newEntry.Set();

                return true;
            }
        }

        public static void ValidateNodeTag(string nodeTag)
        {
            if (nodeTag.Equals("RAFT"))
                ThrowInvalidNodeTag(nodeTag, "It is a reserved tag.");
            if (nodeTag.Length > 4)
                ThrowInvalidNodeTag(nodeTag, "Max node tag length is 4.");
            // Node tag must not contain ':' or '-' chars as they are in use in change vector.
            // The following check covers that as well.
            if (nodeTag.IsUpperLettersOnly() == false)
                ThrowInvalidNodeTag(nodeTag, "Node tag must contain only upper case letters.");
        }

        public static void ThrowInvalidNodeTag(string nodeTag, string reason)
        {
            throw new ArgumentException($"Can't set the node tag to '{nodeTag}'. {reason}");
        }

        public override string ToString()
        {
            return $"Leader {_engine.Tag} in term {Term}";
        }

        private static string GenerateNodeTag(ClusterTopology clusterTopology)
        {
            if (clusterTopology.LastNodeId.Length == 0)
            {
                return "A";
            }

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
            if (_entries.TryGetValue(index, out CommandState state))
            {
                if (state.ConvertResult == null)
                {
                    ValidateUsableReturnType(result);
                    state.Result = result;
                }
                else
                {
                    state.Result = state.ConvertResult.Apply(result);
                }
            }
        }

        [Conditional("DEBUG")]
        private void ValidateUsableReturnType(object result)
        {
            if (result == null)
                return;

            if (result is BlittableJsonReaderObject || result is BlittableJsonReaderArray)
                throw new RachisApplyException("You cannot return a blittable here, it is bound to the context of the state machine, and cannot leak outside");

            if (TypeConverter.IsSupportedType(result) == false)
            {
                throw new RachisApplyException("We don't support type " + result.GetType().FullName + ".");
            }
        }

        public class CommandState
        {
            public long CommandIndex;
            public object Result;
            public ConvertResultAction ConvertResult;
            public TaskCompletionSource<(long, object)> TaskCompletionSource;
            public Action<TaskCompletionSource<(long, object)>> OnNotify;
        }

        public class ConvertResultAction
        {
            private readonly JsonOperationContext _contextToWriteBlittableResult;
            private readonly ConvertResultFromLeader _action;
            private readonly SingleUseFlag _timeout = new SingleUseFlag();

            public ConvertResultAction(JsonOperationContext contextToWriteBlittableResult, ConvertResultFromLeader action)
            {
                _contextToWriteBlittableResult = contextToWriteBlittableResult ?? throw new ArgumentNullException(nameof(contextToWriteBlittableResult));
                _action = action ?? throw new ArgumentNullException(nameof(action));
            }

            public object Apply(object result)
            {
                lock (this)
                {
                    if (_timeout.IsRaised())
                        return null;

                    return _action(_contextToWriteBlittableResult, result);
                }
            }

            public void AboutToTimeout()
            {
                lock (this)
                {
                    _timeout.Raise();
                }
            }
        }
    }
}
