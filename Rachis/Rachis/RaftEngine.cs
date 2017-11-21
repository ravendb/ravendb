// -----------------------------------------------------------------------
//  <copyright file="RaftEngine.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Rachis.Behaviors;
using Rachis.Commands;
using Rachis.Interfaces;
using Rachis.Messages;
using Rachis.Storage;
using Rachis.Transport;
using Rachis.Utils;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Imports.Newtonsoft.Json;

namespace Rachis
{
    public class RaftEngine : IDisposable
    {
        private readonly RaftEngineOptions _raftEngineOptions;
        private readonly CancellationTokenSource _eventLoopCancellationTokenSource;
        private readonly ManualResetEventSlim _leaderSelectedEvent = new ManualResetEventSlim();
        private readonly ManualResetEventSlim _leaderConfirmedEvent = new ManualResetEventSlim();
        private TaskCompletionSource<object> _steppingDownCompletionSource;

        private Topology _currentTopology;

        internal ILog _log;

        public IRaftStateMachine StateMachine { get { return _raftEngineOptions.StateMachine; } }

        public ITransport Transport { get { return _raftEngineOptions.Transport; } }

        public string Name { get; set; }
        public PersistentState PersistentState { get; set; }
        public RaftEngineStatistics EngineStatistics;
        public string CurrentLeader
        {
            get
            {
                return _currentLeader;
            }
            set
            {
                if (_currentLeader == value)
                    return;
                if (_log.IsDebugEnabled)
                    _log.Debug("Setting CurrentLeader: {0} on term {1}", value, PersistentState.CurrentTerm);
                _currentLeader = value;


                if (value == null)
                {
                    _leaderSelectedEvent.Reset();
                    _leaderConfirmedEvent.Reset();
                }
                else
                {
                    _leaderSelectedEvent.Set();
                }
            }
        }

        public long CommitIndex => StateMachine.LastAppliedIndex;

        public RaftEngineState State
        {
            get
            {
                var behavior = StateBehavior;
                if (behavior == null)
                    return RaftEngineState.None;
                return behavior.State;
            }

        }

        /// <summary>
        /// This method is intended to prevent a node from becoming a leader by registering 
        /// ProposingCandidacy event and setting the veto result to false
        /// </summary>
        /// <returns> The veto result with the reasoning</returns>
        public ProposingCandidacyResult CheckIfThereIsVetoOnBecomingCandidate()
        {
            var candidacyResult = new ProposingCandidacyResult();
            var onProposingCandidacy = ProposingCandidacy;
            if (onProposingCandidacy == null)
                return candidacyResult;            
            onProposingCandidacy(this, candidacyResult);
            return candidacyResult;
        }

        private Task _eventLoopTask;

        private string _currentLeader;

        private Task _snapshottingTask;
        private Task _changingTopology;

        private AbstractRaftStateBehavior StateBehavior { get; set; }

        public CancellationToken CancellationToken { get { return _eventLoopCancellationTokenSource.Token; } }

        public event EventHandler<ProposingCandidacyResult> ProposingCandidacy;
        public event Action<RaftEngineState> StateChanged;
        public event Action<Command> CommitApplied;
        public event Action<long> NewTerm;
        public event Action ElectionStarted;
        public event Action StateTimeout;
        public event Action<LogEntry[]> EntriesAppended;
        public event Action<long, long> CommitIndexChanged;
        public event Action ElectedAsLeader;        
        public event Action<TopologyChangeCommand> TopologyChanged;
        public event Action TopologyChanging;

        public event Action CreatingSnapshot;
        public event Action CreatedSnapshot;

        public event Action InstallingSnapshot;
        public event Action SnapshotInstalled;

        public event Action<Exception> SnapshotCreationError;

        /// <summary>
        /// will fire each time event loop of the node will process events and send response messages
        /// </summary>
        public event Action EventsProcessed;

        public RaftEngine(RaftEngineOptions raftEngineOptions)
        {
            _raftEngineOptions = raftEngineOptions;
            EngineStatistics = new RaftEngineStatistics(this);
            Debug.Assert(raftEngineOptions.Stopwatch != null);

            _log = LogManager.GetLogger(raftEngineOptions.Name + "." + GetType().FullName);

            _eventLoopCancellationTokenSource = new CancellationTokenSource();

            Name = raftEngineOptions.Name;
            PersistentState = new PersistentState(raftEngineOptions.Name, raftEngineOptions.StorageOptions, _eventLoopCancellationTokenSource.Token)
            {
                CommandSerializer = new JsonCommandSerializer()
            };

            _currentTopology = PersistentState.GetCurrentTopology();

            //warm up to make sure that the serializer don't take too long and force election timeout
            PersistentState.CommandSerializer.Serialize(new NopCommand());

            var thereAreOthersInTheCluster = CurrentTopology.QuorumSize > 1;
            if (thereAreOthersInTheCluster == false && CurrentTopology.IsVoter(Name))
            {
                PersistentState.UpdateTermTo(this, PersistentState.CurrentTerm + 1);// restart means new term
                SetState(RaftEngineState.Leader);
            }
            else
            {
                SetState(RaftEngineState.Follower);
            }
        
            _eventLoopTask = Task.Factory.StartNew(EventLoop, TaskCreationOptions.LongRunning);
        }

        public RaftEngineOptions Options
        {
            get { return _raftEngineOptions; }
        }

        protected void EventLoop()
        {
            while (_eventLoopCancellationTokenSource.IsCancellationRequested == false)
            {
                try
                {
                    MessageContext message;
                    var behavior = StateBehavior;
                    var lastHeartBeat = (int) (DateTime.UtcNow - behavior.LastHeartbeatTime).TotalMilliseconds;
                    var timeout = behavior.Timeout - lastHeartBeat;
                    var hasMessage = Transport.TryReceiveMessage(timeout, _eventLoopCancellationTokenSource.Token, out message);
                    var messageBase = message?.Message as BaseMessage;
                    //Append entries is a very common messgae (it is the heartbeat)
                    //We don't want to keep empty append entries in the log so we filter them.
                    AppendEntriesRequestWithEntries appendEntriesRequest;
                    if (messageBase != null && ShouldNotFilterMessage(messageBase, out appendEntriesRequest))
                        EngineStatistics.Messages.LimitedSizeEnqueue(
                            new MessageWithTimingInformation {Message = appendEntriesRequest ?? messageBase, MessageReceiveTime = DateTime.UtcNow},
                            RaftEngineStatistics.NumberOfMessagesToTrack);
                    if (_eventLoopCancellationTokenSource.IsCancellationRequested)
                    {
                        if (_log.IsDebugEnabled)
                            _log.Debug("Cancellation request from event loop");
                        break;
                    }
                    //The behavior may have changed while we were waiting for messages (this happens when forcing leadership).
                    var hasStateChagned = behavior != StateBehavior;
                    var oldBehavior = behavior;
                    behavior = StateBehavior;
                    if (hasMessage == false)
                    {
                        if (hasStateChagned)
                        {
                            if (_log.IsDebugEnabled)
                                _log.Debug("State {0} timeout but the behavior has changed to {2} so we will skip timeout handling ({1:#,#;;0} ms).", oldBehavior.State, oldBehavior.Timeout, behavior.State);
                            continue;
                        }
                        if (State != RaftEngineState.Leader && _log.IsDebugEnabled)
                            _log.Debug("State {0} timeout ({1:#,#;;0} ms).", State, behavior.Timeout);
                        EngineStatistics.TimeOuts.LimitedSizeEnqueue(
                            new TimeoutInformation()
                                {ActualTimeout = lastHeartBeat, State = State, Timeout = behavior.Timeout, TimeOutTime = DateTime.UtcNow}
                            , RaftEngineStatistics.NumberOfTimeoutsToTrack);
                        behavior.HandleTimeout();
                        OnStateTimeout();
                        continue;
                    }

                    if (_log.IsDebugEnabled)
                        _log.Debug("{0}: {1} {2}", State,
                            message.Message.GetType().Name,
                            message.Message is BaseMessage ? JsonConvert.SerializeObject(message.Message) : string.Empty
                        );

                    behavior.HandleMessage(message);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception e)
                {
                    //This is unexpected and we can't have our event loop die on us so we re-create it 
                    _log.ErrorException($"Unexpected exception thrown from event loop",e);
                    _eventLoopTask.ContinueWith(_ =>
                    {
                        _eventLoopTask = Task.Factory.StartNew(EventLoop, TaskCreationOptions.LongRunning);
                    }, CancellationToken);
                }
            }
        }

        private bool ShouldNotFilterMessage(BaseMessage message,out AppendEntriesRequestWithEntries appendEntriesRequest)
        {            
            var appendMessage = message as AppendEntriesRequest;
            appendEntriesRequest = null;
            if (appendMessage == null)
                return true;
            if (appendMessage.EntriesCount == 0)
                return false;
            appendEntriesRequest = AppendEntriesRequestWithEntries.FromAppendEntriesRequest(appendMessage);
            return true;
        }

        internal void UpdateCurrentTerm(long term, string leader)
        {
            PersistentState.UpdateTermTo(this, term);
            SetState(RaftEngineState.Follower);
            if (_log.IsDebugEnabled)
                _log.Debug("UpdateCurrentTerm() setting new leader : {0}", leader ?? "no leader currently");
            CurrentLeader = leader;
        }

        internal void SetState(RaftEngineState state)
        {
            if (state == State)
                return;

            if (State == RaftEngineState.Leader)
                _leaderSelectedEvent.Reset();

            var oldState = State;
            if (StateBehavior != null)
                StateBehavior.Dispose();
            switch (state)
            {
                case RaftEngineState.Follower:
                case RaftEngineState.FollowerAfterStepDown:
                    StateBehavior = new FollowerStateBehavior(this, state == RaftEngineState.FollowerAfterStepDown);
                    break;
                case RaftEngineState.CandidateByRequest:
                    StateBehavior = new CandidateStateBehavior(this, true);
                    //setting state before calling election because the state may change to leader before 
                    //we set it to CandidateByRequest
                    AssertStateAndRaiseStateChanged(state, oldState);
                    StateBehavior.HandleTimeout();
                    return;
                case RaftEngineState.Candidate:
                    StateBehavior = new CandidateStateBehavior(this, false);
                    //setting state before calling election because the state may change to leader before 
                    //we set it to CandidateStateBehavior
                    AssertStateAndRaiseStateChanged(state, oldState);
                    StateBehavior.HandleTimeout();
                    return;
                case RaftEngineState.SnapshotInstallation:
                    StateBehavior = new SnapshotInstallationStateBehavior(this);
                    break;
                case RaftEngineState.Leader:
                    StateBehavior = new LeaderStateBehavior(this);
                    CurrentLeader = Name;
                    OnElectedAsLeader();
                    break;
                case RaftEngineState.SteppingDown:
                    StateBehavior = new SteppingDownStateBehavior(this);
                    CurrentLeader = Name;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(state.ToString());
            }

            AssertStateAndRaiseStateChanged(state, oldState);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AssertStateAndRaiseStateChanged(RaftEngineState state, RaftEngineState oldState)
        {
            Debug.Assert(StateBehavior != null, "StateBehavior != null");
            OnStateChanged(state);
            if (_log.IsDebugEnabled)
                _log.Debug("{2}:{0} ==> {1}", oldState, state,this.Options.SelfConnection.Uri);
        }


        public void ForceCandidateState()
        {
            if (_log.IsDebugEnabled)
                _log.Debug("Forced into candidate mode");
            SetState(RaftEngineState.CandidateByRequest);
        }

        public Task StepDownAsync()
        {
            if (State != RaftEngineState.Leader)
                throw new NotLeadingException("Not a leader, and only leaders can step down");

            if (CurrentTopology.QuorumSize == 1)
            {
                throw new InvalidOperationException("Cannot step down if I'm the only one in the cluster");
            }

            _steppingDownCompletionSource = new TaskCompletionSource<object>();
            if (_log.IsDebugEnabled)
                _log.Debug("Got a request to step down from leadership position, personal reasons, you understand.");
            SetState(RaftEngineState.SteppingDown);

            return _steppingDownCompletionSource.Task;
        }

        public Task RemoveFromClusterAsync(NodeConnectionInfo node)
        {
            if (_currentTopology.Contains(node.Name) == false)
                throw new InvalidOperationException("Node " + node + " was not found in the cluster");

            if (string.Equals(node.Name, Name, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("You cannot remove the current node from the cluster, step down this node and then remove it from the new leader");

            var requestedTopology = new Topology(
                _currentTopology.TopologyId,
                _currentTopology.AllVotingNodes.Where(x => string.Equals(x.Name, node.Name, StringComparison.OrdinalIgnoreCase) == false),
                _currentTopology.NonVotingNodes.Where(x => string.Equals(x.Name, node.Name, StringComparison.OrdinalIgnoreCase) == false),
                _currentTopology.PromotableNodes.Where(x => string.Equals(x.Name, node.Name, StringComparison.OrdinalIgnoreCase) == false)
            );
            if (_log.IsInfoEnabled)
            {
                _log.Info("RemoveFromClusterAsync, requestedTopology: {0}", requestedTopology);
            }
            return ModifyTopology(requestedTopology);
        }

        public Task AddToClusterAsync(NodeConnectionInfo node)
        {
            if (_currentTopology.Contains(node.Name))
                throw new InvalidOperationException("Node " + node.Name + " is already in the cluster");

            var requestedTopology = new Topology(
                _currentTopology.TopologyId,
                _currentTopology.AllVotingNodes,
                node.IsNoneVoter ? _currentTopology.NonVotingNodes.Union(new[] { node }) : _currentTopology.NonVotingNodes,
                node.IsNoneVoter ? _currentTopology.PromotableNodes : _currentTopology.PromotableNodes.Union(new[] { node })
                );

            if (_log.IsInfoEnabled)
            {
                _log.Info("AddToClusterClusterAsync, requestedTopology: {0}", requestedTopology);
            }
            return ModifyTopology(requestedTopology);
        }

        public Task UpdateNodeAsync(NodeConnectionInfo node)
        {
            if (CurrentLeader == node.Name && node.IsNoneVoter)
                throw new InvalidOperationException("Can't change leader voting mode to none voter");

            var currentTopology = CurrentTopology;

            var allVotingNodes = currentTopology.AllVotingNodes.Select(n => n.Uri.AbsoluteUri == node.Uri.AbsoluteUri ? node : n).ToList();
            var nonVotingNodes = currentTopology.NonVotingNodes.Select(n => n.Uri.AbsoluteUri == node.Uri.AbsoluteUri ? node : n).ToList();
            var promotableNodes = currentTopology.PromotableNodes.Select(n => n.Uri.AbsoluteUri == node.Uri.AbsoluteUri ? node : n).ToList();

            var currentNodeConfiguration = currentTopology.AllNodes.First(x => x.Uri.AbsoluteUri == node.Uri.AbsoluteUri);
            if (currentNodeConfiguration.IsNoneVoter != node.IsNoneVoter)
            {
                if (node.IsNoneVoter)
                {
                    // move node from voting -> non-voting
                    allVotingNodes.Remove(node);
                    nonVotingNodes.Add(node);
                    promotableNodes.Remove(node);
                }
                else
                {
                    // move node from non-voting -> voting
                    allVotingNodes.Add(node);
                    nonVotingNodes.Remove(node);
                    promotableNodes.Add(node);
                }
            }

            var requestedTopology = new Topology(
                currentTopology.TopologyId,
                allVotingNodes,
                nonVotingNodes,
                promotableNodes);

            return ModifyTopology(requestedTopology);
        }

        public Task ModifyNodeVotingModeAsync(NodeConnectionInfo node, bool votingMode)
        {
            if (!_currentTopology.Contains(node.Name))
                throw new InvalidOperationException("Node " + node.Name + " is not in the cluster");
            node.IsNoneVoter = !votingMode;
            var requestedTopology = new Topology(
                _currentTopology.TopologyId,
                votingMode ? _currentTopology.AllVotingNodes: _currentTopology.AllVotingNodes.Where(x => string.Equals(x.Name, node.Name, StringComparison.OrdinalIgnoreCase) == false),
                votingMode ? _currentTopology.NonVotingNodes.Where(x => string.Equals(x.Name, node.Name, StringComparison.OrdinalIgnoreCase) == false):
                _currentTopology.NonVotingNodes.Union(new[] { node}),
                votingMode ? _currentTopology.PromotableNodes.Union(new [] {node}) : _currentTopology.PromotableNodes.Where(x => string.Equals(x.Name, node.Name, StringComparison.OrdinalIgnoreCase) == false)
                );

            if (_log.IsInfoEnabled)
            {
                _log.Info("ModifyNodeVotingModeAsync, requestedTopology: {0}", requestedTopology);
            }
            return ModifyTopology(requestedTopology);
        }

        internal bool CurrentlyChangingTopology()
        {
            return Interlocked.CompareExchange(ref _changingTopology, null, null) == null;
        }

        internal Task ModifyTopology(Topology requested)
        {
            if (State != RaftEngineState.Leader)
                throw new NotLeadingException("Cannot modify topology from a non leader node, current leader is: " +
                                                    (CurrentLeader ?? "no leader"));

            var logEntry = PersistentState.GetLogEntry(CommitIndex);
            if (logEntry == null)
                throw new InvalidOperationException("No log entry for committed for index " + CommitIndex + ", this is probably a brand new cluster with no committed entries or a serious problem");

            if (logEntry.Term != PersistentState.CurrentTerm)
                throw new InvalidOperationException("Cannot modify the cluster topology when the committed index " + CommitIndex + " is in term " + logEntry.Term + " but the current term is " +
                                                    PersistentState.CurrentTerm + ". Wait until the leader finishes committing entries from the current term and try again");

            var tcc = new TopologyChangeCommand
                {
                    Completion = new TaskCompletionSource<object>(),
                    Requested = requested,
                    Previous = _currentTopology,
                    BufferCommand = false,
                };

            if (Interlocked.CompareExchange(ref _changingTopology, tcc.Completion.Task, null) != null)
                throw new InvalidOperationException("Cannot change the cluster topology while another topology change is in progress");

            try
            {
                if (_log.IsDebugEnabled)
                    _log.Debug("Topology change started on leader");
                StartTopologyChange(tcc);
                AppendCommand(tcc);
                return tcc.Completion.Task;
            }
            catch (Exception)
            {
                Interlocked.Exchange(ref _changingTopology, null);
                throw;
            }
        }

        public Topology CurrentTopology => _currentTopology;

        public Topology PersistentStateCurrentTopology => PersistentState.GetCurrentTopology();

        internal bool LogIsUpToDate(long lastLogTerm, long lastLogIndex)
        {
            // Raft paper 5.4.1
            var lastLogEntry = PersistentState.LastLogEntry();

            if (lastLogEntry.Term < lastLogTerm)
                return true;
            return lastLogEntry.Index <= lastLogIndex;
        }

        public bool WaitForLeader(int timeout = 10*1000)
        {
            var leaderFound = _leaderSelectedEvent.Wait(timeout, CancellationToken);
            if (leaderFound == false)
                CurrentLeader = null;
            return leaderFound;
        }

        public bool WaitForLeaderConfirmed(int timeout = 10 * 1000)
        {
            return _leaderConfirmedEvent.Wait(timeout, CancellationToken);
        }

        public void AppendCommand(Command command)
        {
            if (command == null) throw new ArgumentNullException("command");

            var leaderStateBehavior = StateBehavior as LeaderStateBehavior;
            if (leaderStateBehavior == null || leaderStateBehavior.State != RaftEngineState.Leader)
                throw new NotLeadingException("Command can be appended only on leader node. This node behavior type is " +
                                              StateBehavior.GetType().Name)
                {
                    CurrentLeader = CurrentLeader
                };
        

        leaderStateBehavior.AppendCommand(command);
        }

        public void ApplyCommits(long from, long to)
        {
            Debug.Assert(to >= from);
            foreach (var entry in PersistentState.LogEntriesAfter(from, to))
            {
                try
                {
                    var oldCommitIndex = CommitIndex;
                    var command = PersistentState.CommandSerializer.Deserialize(entry.Data);

                    StateMachine.Apply(entry, command);

                    Debug.Assert(entry.Index == StateMachine.LastAppliedIndex);
                    if (_log.IsDebugEnabled)
                        _log.Debug("Committing entry #{0}", entry.Index);

                    var tcc = command as TopologyChangeCommand;
                    if (tcc != null)
                    {
                        if (_log.IsInfoEnabled)
                        {
                            _log.Info("ApplyCommits for TopologyChangedCommand, tcc.Requested = {0}, Name = {1}",
                                tcc.Requested, Name);
                        }

                        // StartTopologyChange(tcc); - not sure why it was needed, see RavenDB-3808 for details
                        CommitTopologyChange(tcc);
                    }
                    var noop = command as NopCommand;
                    if (noop != null && entry.Term == PersistentState.CurrentTerm)
                    {
                        if (_log.IsInfoEnabled)
                        {
                            _log.Info($"Raising leaderConfirmedEvent, Term = {entry.Term}, Index = {entry.Index}, Name = {Name}");
                        }
                        _leaderConfirmedEvent.Set();
                    }
                    OnCommitIndexChanged(oldCommitIndex, CommitIndex);
                    OnCommitApplied(command);
                }
                catch (Exception e)
                {
                    _log.Error("Failed to apply commit", e);
                    throw;
                }
            }

            if (StateMachine.SupportSnapshots == false)
                return;

            var commitedEntriesCount = PersistentState.GetCommitedEntriesCount(to);
            if (commitedEntriesCount >= Options.MaxLogLengthBeforeCompaction)
            {
                // we need to pass this to the state machine so it can signal us
                // when it is fine to continue modifications again, otherwise
                // we might start the snapshot in a different term / index than we 
                // are actually saying we did.
                var allowFurtherModifications = new ManualResetEventSlim();
                SnapshotAndTruncateLog(to, allowFurtherModifications);
                allowFurtherModifications.Wait(CancellationToken);
            }
        }

        private void SnapshotAndTruncateLog(long to, ManualResetEventSlim allowFurtherModifications)
        {
            var task = new Task(() =>
            {
                OnSnapshotCreationStarted();
                try
                {
                    var currentTerm = PersistentState.CurrentTerm;
                    _log.Info("Starting to create snapshot up to {0} in term {1}", to, currentTerm);
                    StateMachine.CreateSnapshot(to, currentTerm, allowFurtherModifications);
                    PersistentState.MarkSnapshotFor(to, currentTerm,
                        Options.MaxLogLengthBeforeCompaction - (Options.MaxLogLengthBeforeCompaction/8));

                    _log.Info("Finished creating snapshot for {0} in term {1}", to, currentTerm);

                    OnSnapshotCreationEnded();
                }
                catch (Exception e)
                {
                    _log.Error("Failed to create snapshot", e);
                    OnSnapshotCreationError(e);
                }
                finally
                {
                    Interlocked.Exchange(ref _snapshottingTask, null);
                }
            });

            if (Interlocked.CompareExchange(ref _snapshottingTask, task, null) != null)
            {
                allowFurtherModifications.Set();
                return;
            }
            task.Start();
        }


        public void CommitTopologyChange(TopologyChangeCommand tcc)
        {
            //it is logical that _before_ OnTopologyChanged is fired the topology change task will be complete
            // - since this task is used to track progress of topoplogy changes in the interface
            Interlocked.Exchange(ref _changingTopology, null);

            //if no topology was present and TopologyChangeCommand is issued to just
            //accept new topology id - then tcc.Previous == null - it means that 
            //shouldRemainInTopology should be true - because there is no removal from topology actually
            var isRemovedFromTopology = tcc.Requested.Contains(Name) == false && 
                                         tcc.Previous != null && 
                                         tcc.Previous.Contains(Name);
            if (isRemovedFromTopology)
            {
                if (_log.IsDebugEnabled)
                    _log.Debug("This node is being removed from topology, setting its state to follower, it will be idle until a leader will join it to the cluster again");
                CurrentLeader = null;

                SetState(RaftEngineState.Follower);
            }
            else
            {
                if (_log.IsInfoEnabled)
                {
                    _log.Info("Finished applying new topology: {0}{1}", _currentTopology,
                        tcc.Previous == null ? ", Previous topology was null - perhaps it is setting topology for the first time?" : String.Empty);
                }
            }

            OnTopologyChanged(tcc);
        }

        public void Dispose()
        {
            _eventLoopCancellationTokenSource.Cancel();

            var exceptions = new List<Exception>();

            try
            {
                _eventLoopTask.Wait(1000);
            }
            catch (Exception e)
            {
                exceptions.Add(e);
            }

            try
            {
                PersistentState.Dispose();
            }
            catch (Exception e)
            {
                exceptions.Add(e);
            }

            try
            {
                _raftEngineOptions.Dispose();
            }
            catch (Exception e)
            {
                exceptions.Add(e);
            }
            
            if (exceptions.Count == 0)
                return;

            throw new AggregateException(exceptions);
        }

        internal virtual void OnCandidacyAnnounced()
        {
            var handler = ElectionStarted;
            if (handler != null)
            {
                try
                {
                    handler();
                }
                catch (Exception e)
                {
                    _log.Error("Error on raising ElectionStarted event" + e);
                }
            }
        }

        protected virtual void OnStateChanged(RaftEngineState state)
        {
            var handler = StateChanged;
            if (handler != null)
            {
                try
                {
                    handler(state);
                }
                catch (Exception e)
                {
                    _log.Error("Error on raising StateChanged event" + e);
                }
            }
        }

        protected virtual void OnStateTimeout()
        {
            var handler = StateTimeout;
            if (handler != null)
            {
                try
                {
                    handler();
                }
                catch (Exception e)
                {
                    _log.Error("Error on raising StateTimeout event" + e);
                }
            }
        }

        internal virtual void OnEntriesAppended(LogEntry[] logEntries)
        {
            var handler = EntriesAppended;
            if (handler != null)
            {
                try
                {
                    handler(logEntries);
                }
                catch (Exception e)
                {
                    _log.Error("Error on raising EntriesAppended event", e);
                }
            }
        }

        protected virtual void OnCommitIndexChanged(long oldCommitIndex, long newCommitIndex)
        {
            var handler = CommitIndexChanged;
            if (handler != null)
            {
                try
                {
                    handler(oldCommitIndex, newCommitIndex);
                }
                catch (Exception e)
                {
                    _log.Error("Error on raising CommitIndexChanged event", e);
                }
            }
        }

        protected virtual void OnElectedAsLeader()
        {
            var handler = ElectedAsLeader;
            if (handler != null)
            {
                try
                {
                    handler();
                }
                catch (Exception e)
                {
                    _log.Error("Error on raising ElectedAsLeader event", e);
                }
            }
        }

        protected virtual void OnTopologyChanged(TopologyChangeCommand cmd)
        {
            _log.Info ("OnTopologyChanged() - " + this.Name);
            var handler = TopologyChanged;
            if (handler != null)
            {
                try
                {
                    handler(cmd);
                }
                catch (Exception e)
                {
                    _log.Error("Error on raising TopologyChanged event", e);
                }
            }
        }

        protected virtual void OnCommitApplied(Command cmd)
        {
            var handler = CommitApplied;
            if (handler != null)
            {
                try
                {
                    handler(cmd);
                }
                catch (Exception e)
                {
                    _log.Error("Error on raising CommitApplied event", e);
                }
            }
        }

        private void OnTopologyChanging(TopologyChangeCommand tcc)
        {
            var handler = TopologyChanging;
            if (handler != null)
            {
                try
                {
                    handler();
                }
                catch (Exception e)
                {
                    _log.Error("Error on raising TopologyChanging event", e);
                }
            }
        }

        protected virtual void OnSnapshotCreationStarted()
        {
            var handler = CreatingSnapshot;
            if (handler != null)
            {
                try
                {
                    handler();
                }
                catch (Exception e)
                {
                    _log.Error("Error on raising event", e);
                }
            }
        }

        protected virtual void OnSnapshotCreationEnded()
        {
            var handler = CreatedSnapshot;
            if (handler != null)
            {
                try
                {
                    handler();
                }
                catch (Exception e)
                {
                    _log.Error("Error on raising event", e);
                }
            }
        }

        protected virtual void OnSnapshotCreationError(Exception e)
        {
            var handler = SnapshotCreationError;
            if (handler != null)
            {
                try
                {
                    handler(e);
                }
                catch (Exception ex)
                {
                    _log.Error("Error on raising SnapshotCreationError event", ex);
                }
            }
        }

        internal virtual void OnEventsProcessed()
        {
            var handler = EventsProcessed;
            if (handler != null)
            {
                try
                {
                    handler();
                }
                catch (Exception e)
                {
                    _log.Error("Error on raising EventsProcessed event", e);
                }
            }
        }

        internal virtual void OnSnapshotInstallationStarted()
        {
            var handler = InstallingSnapshot;
            if (handler != null)
            {
                try
                {
                    handler();
                }
                catch (Exception e)
                {
                    _log.Error("Error on raising event", e);
                }
            }
        }
        internal void OnNewTerm(long term)
        {
            var handler = NewTerm;
            if (handler != null)
            {
                try
                {
                    handler(term);
                }
                catch (Exception e)
                {
                    _log.Error("Error on raising event", e);
                }
            }
        }

        internal virtual void OnSnapshotInstallationEnded(long snapshotTerm)
        {
            var handler = SnapshotInstalled;
            if (handler != null)
            {
                try
                {
                    handler();
                }
                catch (Exception e)
                {
                    _log.Error("Error on raising event", e);
                }
            }
        }

        public override string ToString()
        {
            return string.Format("Name: {0} = {1}", Name, State);
        }

        public void StartTopologyChange(TopologyChangeCommand tcc)
        {			
            Interlocked.Exchange(ref _currentTopology, tcc.Requested);
            Interlocked.Exchange(ref _changingTopology, new TaskCompletionSource<object>().Task);
            OnTopologyChanging(tcc);
        }

        internal void RevertTopologyTo(Topology previous)
        {
            _log.Info("Reverting topology because the topology change command was reverted");

            Interlocked.Exchange(ref _changingTopology, null);
            Interlocked.Exchange(ref _currentTopology, previous);
            OnTopologyChanged(new TopologyChangeCommand
            {
                Requested = previous
            });
        }

        internal void FinishSteppingDown()
        {
            var steppingDownCompletionSource = _steppingDownCompletionSource;
            if (steppingDownCompletionSource == null)
                return;
            _steppingDownCompletionSource = null;
            steppingDownCompletionSource.TrySetResult(null);
        }

        public FollowerLastSentEntries GetFollowerStatistics()
        {
            var leader = StateBehavior as LeaderStateBehavior;
            return leader?.GetMaxIndexOnQuorumInternal();
        }

        public void Danger__CutLogAtPosition(long postion)
        {
            PersistentState.AppendToLog(this, new List<LogEntry>(), postion);
            if (postion < StateMachine.LastAppliedIndex)
            {
                StateMachine.Danger__SetLastApplied(postion);
            }
        }

        public ILog GetLogger()
        {
            return _log;
        }
    }

    public class ProposingCandidacyResult : EventArgs
    {
        public bool VetoCandidacy { get; set; }
        /// <summary>
        /// for now we support a single veto reasoning, if in the future we will have multiple 
        /// handlers we should consider chaining this property to a list
        /// </summary>
        public string Reason { get; set; }
    }
}
