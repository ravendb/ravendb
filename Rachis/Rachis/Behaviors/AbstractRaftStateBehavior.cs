using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Rachis.Commands;
using Rachis.Messages;
using Rachis.Storage;
using Rachis.Transport;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;

namespace Rachis.Behaviors
{
    public abstract class AbstractRaftStateBehavior : IDisposable
    {
        protected readonly RaftEngine Engine;
        public int Timeout { get; set; }
        public abstract RaftEngineState State { get; }

        public DateTime LastHeartbeatTime
        {
            get { return lastHeartbeatTime; }
            set
            {
                lastHeartbeatTime = value;
                Engine.EngineStatistics.HeartBeats.LimitedSizeEnqueue(value, RaftEngineStatistics.NumberOfHeartbeatsToTrack);
            }
        }
        public DateTime LastMessageTime { get; set; }

        private DateTime lastHeartbeatTime;
        private readonly Dictionary<Type, Action<MessageContext>> _actionDispatch;

        protected bool FromOurTopology(BaseMessage msg, bool acceptWhenTopologyIsEmpty = true)
        {
            if (msg.ClusterTopologyId == Engine.CurrentTopology.TopologyId)
                return true;

            // if we don't have the same topology id, maybe we have _no_ topology, if that is the case,
            // we are accepting the new topology id immediately
            if (acceptWhenTopologyIsEmpty && Engine.CurrentTopology.TopologyId == Guid.Empty && 
                Engine.CurrentTopology.HasVoters == false)
            {
                var tcc = new TopologyChangeCommand
                {
                    Requested = new Topology(msg.ClusterTopologyId)
                };

                Engine.StartTopologyChange(tcc);
                Engine.CommitTopologyChange(tcc);
                return true;
            }

            return false;
        }

        public void HandleMessage(MessageContext context)
        {
            try
            {
                Action<MessageContext> value;
                if(_actionDispatch.TryGetValue(context.Message.GetType(), out value) == false)
                       throw new InvalidOperationException("Can't find handler for " + context.Message.GetType());

                value(context);

                Engine.OnEventsProcessed();
            }
            catch (Exception e)
            {
                context.Error(e);
            }
            finally
            {
                if (context.AsyncResponse == false)
                    context.Done();
            }
        }

        public void Handle(DisconnectedFromCluster req)
        {
            if (FromOurTopology(req) == false)
            {
                _log.Info("Got a disconnection notification message outside my cluster topology (id: {0}), ignoring", req.ClusterTopologyId);
                return;
            }
            if (req.Term < Engine.PersistentState.CurrentTerm)
            {
                _log.Info("Got disconnection notification from an older term, ignoring");
                return;
            }
            if (req.From != Engine.CurrentLeader)
            {
                _log.Info("Got disconnection notification from {0}, who isn't the current leader, ignoring.",
                    req.From);
                return;
            }
            _log.Warn("Got disconnection notification  from the leader, clearing topology and moving to idle follower state");
            var tcc = new TopologyChangeCommand
            {
                Requested = new Topology(req.ClusterTopologyId)
            };
            Engine.PersistentState.SetCurrentTopology(tcc.Requested, 0L);
            Engine.StartTopologyChange(tcc);
            Engine.CommitTopologyChange(tcc);
            Engine.SetState(RaftEngineState.Follower);
        }

        public void Handle(TimeoutNowRequest req)
        {
            if (FromOurTopology(req) == false)
            {
                _log.Info("Got a timeout request message outside my cluster topology (id: {0}), ignoring", req.ClusterTopologyId);
                return;
            }
            if (req.Term < Engine.PersistentState.CurrentTerm)
            {
                _log.Info("Got timeout now request from an older term, ignoring");
                return;
            }
            if (req.From != Engine.CurrentLeader)
            {
                _log.Info("Got timeout now request from {0}, who isn't the current leader, ignoring.",
                    req.From);
                return;
            }

            _log.Info("Got timeout now request from the leader, timing out and forcing immediate election");
            Engine.SetState(RaftEngineState.CandidateByRequest);
        }

        public virtual InstallSnapshotResponse Handle(MessageContext context, InstallSnapshotRequest req, Stream stream)
        {
            if (FromOurTopology(req) == false)
            {
                _log.Info("Got an install snapshot message outside my cluster topology (id: {0}), ignoring", req.ClusterTopologyId);
                return new InstallSnapshotResponse
                {
                    Success = false,
                    Message = "Cannot accept message from outside my topology. My cluster topology id is: " + Engine.CurrentTopology.TopologyId,
                    CurrentTerm = Engine.PersistentState.CurrentTerm,
                    From = Engine.Name,
                    ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                    LastLogIndex = Engine.PersistentState.LastLogEntry().Index
                };
            }
            
            stream.Dispose();
            return new InstallSnapshotResponse
            {
                Success = false,
                Message = "Cannot install snapshot from state " + State,
                CurrentTerm = Engine.PersistentState.CurrentTerm,
                From = Engine.Name,
                ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                LastLogIndex = Engine.PersistentState.LastLogEntry().Index
            };
        }

        public virtual void Handle(RequestVoteResponse resp)
        {
            //do nothing, irrelevant here
        }

        public abstract void HandleTimeout();

        protected ILog _log;

        protected AbstractRaftStateBehavior(RaftEngine engine)
        {
            Engine = engine;
            
            _log = LogManager.GetLogger(engine.Name + "." + GetType().FullName);

            _actionDispatch = new Dictionary<Type, Action<MessageContext>>
            {
                {typeof (RequestVoteRequest), ctx => ctx.Reply(Handle((RequestVoteRequest) ctx.Message))},
                {typeof (AppendEntriesRequest), ctx => ctx.Reply(Handle((AppendEntriesRequest) ctx.Message))},
                {typeof (CanInstallSnapshotRequest), ctx => ctx.Reply(Handle((CanInstallSnapshotRequest) ctx.Message))},
                {typeof (InstallSnapshotRequest), ctx =>
                {
                    var reply = Handle(ctx, (InstallSnapshotRequest) ctx.Message, ctx.Stream);
                    if (reply != null)
                        ctx.Reply(reply);
                    else
                        ctx.AsyncResponse = true;
                }},

                {typeof(RequestVoteResponse), ctx => Handle((RequestVoteResponse)ctx.Message)},
                {typeof(AppendEntriesResponse), ctx => Handle((AppendEntriesResponse)ctx.Message)},
                {typeof(CanInstallSnapshotResponse), ctx => Handle((CanInstallSnapshotResponse)ctx.Message)},
                {typeof(InstallSnapshotResponse), ctx => Handle((InstallSnapshotResponse)ctx.Message)},

                {typeof(NothingToDo), ctx => { }},
                {typeof (TimeoutNowRequest), ctx => Handle((TimeoutNowRequest) ctx.Message)},
                {typeof (DisconnectedFromCluster), ctx => Handle((DisconnectedFromCluster) ctx.Message)},
                {typeof (Action), ctx => ((Action)ctx.Message)()},

            };
            LastHeartbeatTime = DateTime.UtcNow;            
        }

        public RequestVoteResponse Handle(RequestVoteRequest req)
        {
            //We don't vote for nodes when we have no topology!
            if (FromOurTopology(req, acceptWhenTopologyIsEmpty:false) == false)
            {
                _log.Info("Got a request vote message outside my cluster topology (id: {0}), ignoring", req.ClusterTopologyId);
                return new RequestVoteResponse
                {
                    VoteGranted = false,
                    CurrentTerm = Engine.PersistentState.CurrentTerm,
                    VoteTerm = req.Term,
                    Message = "Cannot vote for a node outside my topology, my topology id is: " + Engine.CurrentTopology.TopologyId,
                    From = Engine.Name,
                    ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                    TrialOnly = req.TrialOnly,
                    TermIncreaseMightGetMyVote = false
                };
            }

            //disregard RequestVoteRequest if this node receives regular messages and the leader is known
            // Raft paper section 6 (cluster membership changes), this apply only if we are a follower, because
            // candidate and leaders both generate their own heartbeat messages
            var timeSinceLastHeartbeat = (DateTime.UtcNow - LastMessageTime).TotalMilliseconds;

            
            if (State == RaftEngineState.Follower && req.ForcedElection == false &&
                (timeSinceLastHeartbeat < Timeout) && Engine.CurrentLeader != null)
            {
                _log.Info("Received RequestVoteRequest from a node within election timeout while leader exists, rejecting " );
                return new RequestVoteResponse
                {
                    VoteGranted = false,
                    CurrentTerm = Engine.PersistentState.CurrentTerm,
                    VoteTerm = req.Term,
                    Message = "I currently have a leader and I am receiving heartbeats within election timeout.",
                    From = Engine.Name,
                    ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                    TrialOnly = req.TrialOnly,
                    TermIncreaseMightGetMyVote = false
                };
            }

            if (Engine.CurrentTopology.IsVoter(req.From) == false 
                // if it isn't in my cluster, but we don't have any voters, than we are probably a new node, so we'll accept this 
                // and allow it to become our leader
                && Engine.CurrentTopology.HasVoters) 
            {
                _log.Info("Received RequestVoteRequest from a node that isn't a voting member in the cluster: {0}, rejecting", req.From);
                return new RequestVoteResponse
                {
                    VoteGranted = false,
                    CurrentTerm = Engine.PersistentState.CurrentTerm,
                    VoteTerm = req.Term,
                    Message = "You are not a memeber in my cluster, and cannot be a leader",
                    From = Engine.Name,
                    ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                    TrialOnly = req.TrialOnly,
                    TermIncreaseMightGetMyVote = false
                };
            }

            if (_log.IsDebugEnabled)
                _log.Debug("Received RequestVoteRequest, req.CandidateId = {0}, term = {1}", req.From, req.Term);

            if (req.Term < Engine.PersistentState.CurrentTerm)
            {
                var msg = string.Format("Rejecting request vote because term {0} is lower than current term {1}",
                    req.Term, Engine.PersistentState.CurrentTerm);
                _log.Info(msg);
                return new RequestVoteResponse
                {
                    VoteGranted = false,
                    CurrentTerm = Engine.PersistentState.CurrentTerm,
                    VoteTerm = req.Term,
                    Message = msg,
                    From = Engine.Name,
                    TrialOnly = req.TrialOnly,
                    ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                    TermIncreaseMightGetMyVote = false
                };
            }

            if (req.Term > Engine.PersistentState.CurrentTerm && req.TrialOnly == false)
            {
                Engine.UpdateCurrentTerm(req.Term, null);
            }

            if (Engine.PersistentState.VotedFor != null && Engine.PersistentState.VotedFor != req.From &&
                Engine.PersistentState.VotedForTerm >= req.Term  &&
                //This is the case where we voted for a node and right after were not able to communicate to is
                DateTime.UtcNow - LastHeartbeatTime < TimeSpan.FromMilliseconds(10 * Engine.Options.ElectionTimeout))
            {
                var msg = string.Format("Rejecting request vote because already voted for {0} in term {1}",
                    Engine.PersistentState.VotedFor, req.Term);

                _log.Info(msg);
                return new RequestVoteResponse
                {
                    VoteGranted = false,
                    CurrentTerm = Engine.PersistentState.CurrentTerm,
                    VoteTerm = req.Term,
                    Message = msg,
                    From = Engine.Name,
                    ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                    TrialOnly = req.TrialOnly,
                    TermIncreaseMightGetMyVote = true
                };
            }

            if (Engine.LogIsUpToDate(req.LastLogTerm, req.LastLogIndex) == false)
            {
                var msg = string.Format("Rejecting request vote because remote log for {0} in not up to date.", req.From);
                _log.Info(msg);
                return new RequestVoteResponse

                {
                    VoteGranted = false,
                    CurrentTerm = Engine.PersistentState.CurrentTerm,
                    VoteTerm = req.Term,
                    Message = msg,
                    From = Engine.Name,
                    ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                    TrialOnly = req.TrialOnly,
                    TermIncreaseMightGetMyVote = false
                };
            }
            
            if (req.TrialOnly == false)
            {
                // we said we would be voting for this guy, so we can give it a full election timeout, 
                // by treating this as a heart beat. This means we won't be timing out ourselves and trying
                // to become the leader
                LastHeartbeatTime = DateTime.UtcNow;
                LastMessageTime = DateTime.UtcNow;

                _log.Info("Recording vote for candidate = {0}", req.From);
                Engine.PersistentState.RecordVoteFor(req.From, req.Term);
            }
            else
            {
                _log.Info("Voted for candidate = {0} in trial election for term {1}", req.From, req.Term);
            }
            return new RequestVoteResponse
            {
                VoteGranted = true,
                CurrentTerm = Engine.PersistentState.CurrentTerm,
                VoteTerm = req.Term,
                Message = "Vote granted",
                From = Engine.Name,
                ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                TrialOnly = req.TrialOnly,
                TermIncreaseMightGetMyVote = false
            };
        }

        public virtual void Handle(CanInstallSnapshotResponse resp)
        {
            //irrelevant here, so doing nothing (used only in LeaderStateBehavior)
        }

        public virtual void Handle(InstallSnapshotResponse resp)
        {
            //irrelevant here, so doing nothing (used only in LeaderStateBehavior)
        }

        public virtual CanInstallSnapshotResponse Handle(CanInstallSnapshotRequest req)
        {
            var lastLogEntry = Engine.PersistentState.LastLogEntry();
            var index = lastLogEntry.Index;
            
            if (FromOurTopology(req) == false)
            {
                _log.Info("Got a can install snapshot message outside my cluster topology (id: {0}), ignoring", req.ClusterTopologyId);
                return new CanInstallSnapshotResponse
                {
                    From = Engine.Name,
                    ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                    IsCurrentlyInstalling = false,
                    Message = "Cannot install a snapshot from a node outside my topoloyg. My topology id is: " + req.ClusterTopologyId,
                    Success = false,
                    Index = index,
                    Term = Engine.PersistentState.CurrentTerm
                };
            }
            if (req.Term <= Engine.PersistentState.CurrentTerm && req.Index <= index)
            {
                return new CanInstallSnapshotResponse
                {
                    From = Engine.Name,
                    ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                    IsCurrentlyInstalling = false,
                    Message = String.Format("Term or Index do not match the ones on this node. Cannot install snapshot. (CurrentTerm = {0}, req.Term = {1}, LastLogEntry index = {2}, req.Index = {3}",
                        Engine.PersistentState.CurrentTerm, req.Term, index, req.Index),
                    Success = false,
                    Index = index,
                    Term = Engine.PersistentState.CurrentTerm
                };
            }

            Engine.SetState(RaftEngineState.SnapshotInstallation);

            return new CanInstallSnapshotResponse
            {
                From = Engine.Name,
                ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                IsCurrentlyInstalling = false,
                Message = "Everything ok, go ahead, install the snapshot!",
                Success = true
            };
        }

        public virtual void Handle(AppendEntriesResponse resp)
        {
            // not a leader, no idea what to do with this. Probably an old
            // message from when we were a leader, ignoring.			
        }

        public virtual AppendEntriesResponse Handle(AppendEntriesRequest req)
        {
            var lastLogIndex = Engine.PersistentState.LastLogEntry().Index;

            if (FromOurTopology(req) == false)
            {
                _log.Info("Got an append entries message outside my cluster topology (id: {0}), ignoring", req.ClusterTopologyId);
                return new AppendEntriesResponse
                {
                    Success = false,
                    CurrentTerm = Engine.PersistentState.CurrentTerm,
                    LastLogIndex = lastLogIndex,
                    LeaderId = Engine.CurrentLeader,
                    Message = "Cannot accept append entries from a node outside my cluster. My topology id is: " + Engine.CurrentTopology.TopologyId,
                    From = Engine.Name,
                    ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                };
            }

            if (req.Term < Engine.PersistentState.CurrentTerm)
            {
                var msg = string.Format(
                    "Rejecting append entries because msg term {0} is lower then current term: {1}",
                    req.Term, Engine.PersistentState.CurrentTerm);

                _log.Info(msg);

                return new AppendEntriesResponse
                {
                    Success = false,
                    CurrentTerm = Engine.PersistentState.CurrentTerm,
                    LastLogIndex = lastLogIndex,
                    LeaderId = Engine.CurrentLeader,
                    Message = msg,
                    From = Engine.Name,
                    ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                };
            }

            if (req.Term > Engine.PersistentState.CurrentTerm)
            {
                Engine.UpdateCurrentTerm(req.Term, req.From);
            }

            if (Engine.CurrentLeader == null || req.From.Equals(Engine.CurrentLeader) == false)
            {
                Engine.CurrentLeader = req.From;
                Engine.SetState(RaftEngineState.Follower);
            }

            var prevTerm = Engine.PersistentState.TermFor(req.PrevLogIndex) ?? 0;
            if (prevTerm != req.PrevLogTerm)
            {
                var midpointIndex = req.PrevLogIndex / 2;
                var midpointTerm = Engine.PersistentState.TermFor(midpointIndex) ?? 0;

                var msg = $"Rejecting append entries because msg previous term {req.PrevLogTerm} is not the same as the persisted current term {prevTerm}" +
                          $" at log index {req.PrevLogIndex}. Midpoint index {midpointIndex}, midpoint term: {midpointTerm}";
                _log.Info(msg);
                
                return new AppendEntriesResponse
                {
                    Success = false,
                    CurrentTerm = Engine.PersistentState.CurrentTerm,
                    LastLogIndex = req.PrevLogIndex,
                    Message = msg,
                    LeaderId = Engine.CurrentLeader,
                    MidpointIndex = midpointIndex,
                    MidpointTerm = midpointTerm,
                    From = Engine.Name,
                    ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                };
            }

            LastHeartbeatTime = DateTime.UtcNow;
            LastMessageTime = DateTime.UtcNow;

            var appendEntriesResponse = new AppendEntriesResponse
            {
                Success = true,
                CurrentTerm = Engine.PersistentState.CurrentTerm,
                From = Engine.Name,
                ClusterTopologyId = Engine.CurrentTopology.TopologyId,
            };

            if (req.Entries.Length > 0)
            {
                if (_log.IsDebugEnabled)
                {
                    _log.Debug("Appending log (persistant state), entries count: {0} (node state = {1})", req.Entries.Length,
                    Engine.State);

                    foreach (var logEntry in req.Entries)
                    {
                        _log.Debug("Entry {0} (term {1})", logEntry.Index, logEntry.Term);
                    }
                }

                // if is possible that we'll get the same event multiple times (for example, if we took longer than a heartbeat
                // to process a message). In this case, our log already have the entries in question, and it would be a waste to
                // truncate the log and re-add them all the time. What we are doing here is to find the next match for index/term
                // values in our log and in the entries, and then skip over the duplicates.

                var skip = 0;
                for (int i = 0; i < req.Entries.Length; i++)
                {
                    var termForEntry = Engine.PersistentState.TermFor(req.Entries[i].Index) ?? -1;
                    if (termForEntry != req.Entries[i].Term)
                        break;
                    skip++;
                }


                var topologyChange = req.Entries.Skip(skip).LastOrDefault(x => x.IsTopologyChange == true);

                if (topologyChange != null)
                {
                    var command = Engine.PersistentState.CommandSerializer.Deserialize(topologyChange.Data);
                    var topologyChangeCommand = command as TopologyChangeCommand;

                    if (topologyChangeCommand != null && topologyChangeCommand.Requested.AllNodes.Select(x=>x.Name).Contains(Engine.Options.SelfConnection.Name) == false)
                    {
                        _log.Warn("Got topology without self, disconnecting from the leader, clearing topology and moving to leader state");
                        var tcc = new TopologyChangeCommand
                        {
                            Requested = new Topology(Guid.NewGuid(), new[] { Engine.Options.SelfConnection }, new List<NodeConnectionInfo>(), new List<NodeConnectionInfo>())
                        };
                        Engine.PersistentState.SetCurrentTopology(tcc.Requested, 0L);
                        Engine.StartTopologyChange(tcc);
                        Engine.CommitTopologyChange(tcc);
                        Engine.SetState(RaftEngineState.Leader);

                        return new AppendEntriesResponse
                        {
                            Success = true,
                            CurrentTerm = Engine.PersistentState.CurrentTerm,
                            LastLogIndex = lastLogIndex,
                            Message = "Leaving cluster, because received topology from the leader that didn't contain us",
                            From = Engine.Name,
                            ClusterTopologyId = req.ClusterTopologyId, // we send this "older" ID, so the leader won't reject us
                        };
                    }
                }

                if (skip != req.Entries.Length)
                {
                    Engine.PersistentState.AppendToLog(Engine, req.Entries.Skip(skip), req.PrevLogIndex + skip);
                }
                else
                {
                    // if we skipped the whole thing, this is fine, but let us hint to the leader that we are more 
                    // up to date then it thinks
                    var lastReceivedIndex = req.Entries[req.Entries.Length - 1].Index;
                    appendEntriesResponse.MidpointIndex = lastReceivedIndex + (lastLogIndex - lastReceivedIndex) / 2;
                    appendEntriesResponse.MidpointTerm = Engine.PersistentState.TermFor(appendEntriesResponse.MidpointIndex.Value) ?? 0;

                    _log.Info($"Got {req.Entries.Length} entires from index {req.Entries[0].Index} with term {req.Entries[0].Term} skipping all. " +
                              $"Setting midpoint index to {appendEntriesResponse.MidpointIndex} with term {appendEntriesResponse.MidpointTerm}.");
                }


                
                // we consider the latest topology change to be in effect as soon as we see it, even before the 
                // it is committed, see raft spec section 6:
                //		a server always uses the latest con?guration in its log, 
                //		regardless of whether the entry is committed
                if (topologyChange != null)
                {
                    var command = Engine.PersistentState.CommandSerializer.Deserialize(topologyChange.Data);
                    var topologyChangeCommand = command as TopologyChangeCommand;
                    if (topologyChangeCommand == null) //precaution,should never be true
                        //if this is true --> it is a serious issue and should be fixed immediately!
                        throw new InvalidOperationException(@"Log entry that is marked with IsTopologyChange should be of type TopologyChangeCommand.
                                                            Instead, it is of type: " + command.GetType() + ". It is probably a bug!");
                    
                    _log.Info("Topology change started (TopologyChangeCommand committed to the log): {0}",
                        topologyChangeCommand.Requested);
                    Engine.PersistentState.SetCurrentTopology(topologyChangeCommand.Requested, topologyChange.Index);
                    Engine.StartTopologyChange(topologyChangeCommand);
                }
            }

            var lastIndex = req.Entries.Length == 0 ?
                lastLogIndex :
                req.Entries[req.Entries.Length - 1].Index;
            try
            {				
                var nextCommitIndex = Math.Min(req.LeaderCommit, lastIndex);
                if (nextCommitIndex > Engine.CommitIndex)
                {
                    CommitEntries(req.Entries, nextCommitIndex);
                }

                appendEntriesResponse.LastLogIndex = lastLogIndex;
                return appendEntriesResponse;
            }
            catch (Exception e)
            {
                return new AppendEntriesResponse
                {
                    Success = false,
                    CurrentTerm = Engine.PersistentState.CurrentTerm,
                    LastLogIndex = lastLogIndex,
                    Message = "Failed to apply new entries. Reason: " + e,
                    From = Engine.Name,
                    ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                };
            }
        }

        protected void CommitEntries(LogEntry[] entries, long lastIndex)
        {
            var oldCommitIndex = Engine.CommitIndex + 1;
            Engine.ApplyCommits(oldCommitIndex, lastIndex);			
            Engine.OnEntriesAppended(entries);
        }

        public virtual void Dispose()
        {

        }
    }
}
