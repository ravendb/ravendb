using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Rachis.Commands;
using Rachis.Communication;
using Rachis.Messages;
using Rachis.Storage;

namespace Rachis.Behaviors
{
    public class FollowerStateBehavior: AbstractRaftStateBehavior
    {
        private bool _avoidLeadership;
        private readonly long _currentTermWhenWeBecameFollowers;

        public FollowerStateBehavior(RaftEngine engine, bool avoidLeadership = false) : base(engine)
        {
            _avoidLeadership = avoidLeadership;
            _currentTermWhenWeBecameFollowers = engine.PersistentState.CurrentTerm + 1;
        }

        public override RaftEngineState State => RaftEngineState.Follower;

        public override void HandleTimeout()
        {
            TimeoutEventSlim.Set();
            if (Engine.CurrentTopology.IsVoter(Engine.Name) == false)
            {
                //TODO: log timeout, report not been a leader.
                return;
            }
            if (_avoidLeadership && _currentTermWhenWeBecameFollowers >= Engine.PersistentState.CurrentTerm)
            {
                //TODO:log the fact that we are avoiding leadership since we were leaders and stepped down.
                _avoidLeadership = false;
                return;
            }
            //TODO:Add veto on candidacy?
            //TODO:log the timeout 
            Engine.SetState(RaftEngineState.Candidate);
        }

        public override void HandleNewConnection(ITransportBus transport, CancellationToken ct)
        {
            var messageHelper = transport.GetMessageHandler();
            var lastLogIndex = Engine.PersistentState.LastLogEntry().Index;
            var message = messageHelper.ReadMessage();
            if (ct.IsCancellationRequested)
                return;
            //A leader is contacting us for the first time (could be that we had another thread that was talking to this leader but it should be close by now)
            if (message is AppendEntries)
            {
                var appendEntriesMessage = message as AppendEntries;
                if (appendEntriesMessage.ClusterTopologyId != Engine.CurrentTopology.TopologyId)
                {                        
                    ReplyAppendEntriesResponseOutsideMyTopology(messageHelper, lastLogIndex);
                    return;
                }
                if (appendEntriesMessage.Term < Engine.PersistentState.CurrentTerm)
                {
                    ReplyAppendEntriesResponseLowerTerm(messageHelper, lastLogIndex, appendEntriesMessage);
                    return;
                }
                //TODO:On the node communicating to the leader we need to check that the term has not changed and if it did, shutdown communication nicely.
                if (appendEntriesMessage.Term > Engine.PersistentState.CurrentTerm)
                {
                    Engine.PersistentState.UpdateTermTo(appendEntriesMessage.Term);
                    Engine.CurrentLeader = appendEntriesMessage.From; //TODO:this should be made thread safe, also need to handle privies leader shutdown.
                    Engine.SetState(RaftEngineState.Follower);
                }
                if (Engine.CurrentLeader == null || appendEntriesMessage.From.Equals(Engine.CurrentLeader) == false)
                {
                    Engine.CurrentLeader = appendEntriesMessage.From;
                    Engine.SetState(RaftEngineState.Follower);//TODO:this should be made thread safe, also need to handle privies leader shutdown.
                }
                HandleOnGoingCommunicationFromLeader(transport, ct, appendEntriesMessage);
                

            }
            else if (message is RequestVote)
            {
                    
            }
            else
            {
                    
            }            

            
        }

        public override void HandleOnGoingCommunicationFromLeader(ITransportBus transport, CancellationToken ct, AppendEntries appendEntriesMessage)
        {
            var messageHandler = transport.GetMessageHandler();
            var ourTerm = Engine.PersistentState.TermFor(appendEntriesMessage.PrevLogIndex);
            var messageTerm = appendEntriesMessage.PrevLogTerm;
            //since we can either get an AppendEntries or a LeanAppendEntries I'm extracting the fields needed for the first time
            LogEntry[] entries = appendEntriesMessage.Entries;
            long leaderCommit = appendEntriesMessage.LeaderCommit;
            //We need to negotiate with the leader about our last matched index
            if (ourTerm != messageTerm)
            {
                var leanAppendEntries = NegotiateMatchEntryWithLeader(appendEntriesMessage.PrevLogIndex,transport, ct);
                entries = leanAppendEntries.Entries;
                leaderCommit = leanAppendEntries.LeaderCommit;
            }

            //if we got here we are aligned with the leader, i should probably invoke a different logic here to handle this state...
            TimeoutEventSlim.Set();
            //exit if no success 
            if (HandleValidAppendEntries( entries, leaderCommit, messageHandler) == false)
                return;

            while (ct.IsCancellationRequested == false)
            {
                var leanAppendEntries = messageHandler.ReadMessage() as LeanAppendEntries;
                if(leanAppendEntries == null)
                    throw new InvalidOperationException("Expected LeanAppendEntries got something else"); //TODO: log and better error message.
                HandleValidAppendEntries(leanAppendEntries.Entries, leanAppendEntries.LeaderCommit, messageHandler);
            }
        }

        private LeanAppendEntries NegotiateMatchEntryWithLeader(long prevLogIndex, ITransportBus transport, CancellationToken ct)
        {
            bool done = false;
            var maxIndex = prevLogIndex;
            var minIndex = 0L;
            var midpointIndex = prevLogIndex / 2;
            var messageHandler = transport.GetMessageHandler();
            var midpointTerm = Engine.PersistentState.TermFor(midpointIndex);
            while (ct.IsCancellationRequested == false)
            {
                ReplyAppendEntriesResponsePriviesLogMissmatch(midpointIndex, midpointTerm, maxIndex, minIndex, messageHandler);

                var response = messageHandler.ReadMessage() as TermNegotiationResponse;
                if (response == null)
                {
                    //TODO:log error and re-establish communication and better error message
                    throw new NotSupportedException(
                        "Expected a message of type TermNegotiationResponse but got something else");
                }
                TimeoutEventSlim.Set();
                //leader knows where we at now we expect to read lean append entries 
                if (response.Done)
                {
                    var message = messageHandler.ReadMessage() as LeanAppendEntries;
                    if(message == null)
                    {
                        //TODO:log error and re-establish communication and better error message
                        throw new NotSupportedException(
                            "Expected a message of type LeanAppendEntries but got something else");
                    }
                    return message;
                }
                // the code below assumes that if we find a matching entry the leader will response with done and if we 
                // don't find a matching entry the leader will also reply with done sending all entries from index 0.

                //we need to go backward in the log
                if (midpointIndex > response.MidpointIndex)
                {
                    maxIndex = midpointIndex;                    
                }
                else // need to go forward
                {
                    minIndex = midpointIndex;
                }
                midpointIndex = (maxIndex + minIndex) / 2; 
                midpointTerm = Engine.PersistentState.TermFor(midpointIndex);
            }

            if (ct.IsCancellationRequested) 
                throw new OperationCanceledException("Cancellation was required while negotiating with leader on next match log entry");

            return null;//compiler can't see this is not a valid path...
        }


        private bool HandleValidAppendEntries(LogEntry[] entries,long leaderCommit, IMessageHandler messageHandler)
        {
            //TODO: we should not need to skip entries anymore but we might need to inform the leader that we are committing entries...
            Engine.PersistentState.AppendToLog(entries);
            var topologyChange = entries.LastOrDefault(x => x.IsTopologyChange == true);
            if (topologyChange != null)
            {
                var topologyCommand = topologyChange.Command as TopologyChangeCommand;
                if (topologyCommand == null)
                {
                    ReplyAppendEntriesResponseExpectedTopologyChange(topologyChange.Index, messageHandler);
                    return false;
                }
                Engine.PersistentState.SetCurrentTopology(topologyCommand.Requested, topologyChange.Index);
                Engine.StartTopologyChange(topologyCommand);
            }
            var entriesLength = entries.Length;
            var lastIndex = entriesLength == 0 ?Engine.PersistentState.LastLogEntry().Index : entries[entriesLength - 1].Index;
            var nextCommitIndex = Math.Min(leaderCommit, lastIndex);
            if (nextCommitIndex > Engine.CommitIndex)
            {
                Engine.CommitEntries(entries, nextCommitIndex); //this should be handled on a different thread
            }
            ReplyAppendEntriesResponseSuccessful(lastIndex, messageHandler);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReplyAppendEntriesResponseSuccessful(long lastLogIndex, IMessageHandler messageHandler)
        {
            messageHandler.WriteMessage(new AppendEntriesResponse
            {
                Success = true,
                CurrentTerm = Engine.PersistentState.CurrentTerm,
                LastLogIndex = lastLogIndex
            });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReplyAppendEntriesResponseExpectedTopologyChange(long index, IMessageHandler messageHandler)
        {
            var msg =
                $"Rejecting append entries because entry in index {index} is marked as topology change but contains a different command";
            //TODO: log as error
            messageHandler.WriteMessage(new AppendEntriesResponse
            {
                Success = false,
                CurrentTerm = Engine.PersistentState.CurrentTerm,
                Message = msg,
                LeaderId = Engine.CurrentLeader,
            });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReplyAppendEntriesResponsePriviesLogMissmatch(long midpointIndex, long midpointTerm,long maxIndex,long minIndex, IMessageHandler messageHandler)
        {
            //TODO: log the fact that we don't agree with the leader on the privies log entry and starting a binary search
            messageHandler.WriteMessage(new TermNegotiationRequest
            {
                Success = false,
                MidpointIndex = midpointIndex,
                MidpointTerm = midpointTerm,
                MinIndex = minIndex,
                MaxIndex = maxIndex
            });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReplyAppendEntriesResponseLowerTerm(IMessageHandler messageHandler, long lastLogIndex,
            AppendEntries appendEntriesMessage)
        {
            //TODO: log the fact that we got an append entries request with a lower term than us
            messageHandler.WriteMessage(new AppendEntriesResponse
            {
                Success = false,
                CurrentTerm = Engine.PersistentState.CurrentTerm,
                LastLogIndex = lastLogIndex,
                LeaderId = Engine.CurrentLeader,
                Message = appendEntriesMessage.ToString(),
            });
        }

        //This method might move to the base class in the future.
        //The idea is to have the main logic as compact as possible.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReplyAppendEntriesResponseOutsideMyTopology(IMessageHandler messageHandler, long lastLogIndex)
        {
            //TODO: log the fact that we got an append entries request outside our topology
            messageHandler.WriteMessage(
                new AppendEntriesResponse
                {
                    Success = false,
                    CurrentTerm = Engine.PersistentState.CurrentTerm,
                    LastLogIndex = lastLogIndex,
                    LeaderId = Engine.CurrentLeader,
                    Message =
                        $"Cannot accept append entries from a node outside my cluster. My topology id is: {Engine.CurrentTopology.TopologyId}",
                });
        }
    }
}
