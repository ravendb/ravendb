using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Rachis.Commands;
using Rachis.Communication;
using Rachis.Messages;
using Rachis.Storage;

namespace Rachis.Behaviors
{
    public class LeaderStateBehavior: AbstractRaftStateBehavior
    {
        private readonly CancellationTokenSource _cancellation;
        private readonly Dictionary<string,Thread> _peersThreads = new Dictionary<string, Thread>();
        private readonly Thread _eventLoop;
        public LeaderStateBehavior(RaftEngine engine,bool confirmLeadership = true) : base(engine)
        {
            Engine = engine;
            _cancellation = new CancellationTokenSource();            
            _eventLoop = new Thread(StartEventLoop)
            {
                Name = $"{Engine.Name} event loop"
            };

            _eventLoop.Start();

            TimeoutEvent = new TimeoutEvent(engine.Options.ElectionTimeout);
            TimeoutEvent.TimeoutHappened += HandleTimeout;
            if (confirmLeadership)
            {
                AppendCommand(new NoOpCommand());
            }
        }

        private void StartEventLoop()
        {
            try
            {
                CommitIndexDictionary = new Dictionary<string, FollowerAppendEntriesLastIndexAndTiming>();
                while (true)
                {
                    var aeliwt = _appendEntriesLastIndexWithTiming.Take(_cancellation.Token);
                    CommitIndexDictionary[aeliwt.Name] = aeliwt;
                    if (aeliwt.LastIndex > Engine.CommitIndex)
                    {
                        UpdateCommitIndexAndApplyEntries(CommitIndexDictionary);
                    }
                }
            }
            catch (OperationCanceledException oc)
            {
                
            }
        }

        private Dictionary<string, FollowerAppendEntriesLastIndexAndTiming> CommitIndexDictionary { get; set; }

        public void AppendCommand(Command command)
        {
            var commandIndex = Engine.PersistentState.AppendToLeaderLog(command);
            if (Engine.CurrentTopology.QuorumSize == 1)
            {
                Engine.CommitEntries(commandIndex);
            }
        }

        private void UpdateCommitIndexAndApplyEntries(Dictionary<string, FollowerAppendEntriesLastIndexAndTiming> commitIndexDictionary)
        {
            var sortedLastIndex = commitIndexDictionary.Values.Select(x => x.LastIndex).ToList();
            sortedLastIndex.Sort((l1, l2) => l2.CompareTo(l1));
            var accumilator = 1; //our last log index is bigger or equal to our peers
            long commitIndex = 0;
            foreach (var index in sortedLastIndex)
            {
                accumilator++;
                if (accumilator == Engine.CurrentTopology.QuorumSize)
                {
                    commitIndex = index;
                    break;
                }
            }
            Engine.CommitEntries(Engine.StateMachine.LastAppliedIndex, commitIndex);
        }

        public void StartCommunicationWithPeers()
        {
            foreach (var peer in Engine.CurrentTopology.AllNodes)
            {
                if (peer == Engine.Options.SelfConnectionInfo)
                {
                    continue;
                }
                StartCommunicationWithPeer(peer);
            }
        }
        private void StartCommunicationWithPeer(NodeConnectionInfo peer)
        {
            _peersThreads[peer.Name] = new Thread(TalkWithPeer)
            {
                Name = $"Leader {Engine.Name} to Follower {peer.Name} in term {CurrentTermWhenWeBecameFollowers}"  
            };
            _peersThreads[peer.Name].Start(peer);
        }

        private void TalkWithPeer(object obj)
        {
            var nodeInfo = (NodeConnectionInfo) obj;
            using (var stream = OpenCommunicationWith(nodeInfo))
            {
                MessageHandler messageHandler = new MessageHandler(stream);
                //We start by sending the last entry details and then negotiate if needed.
                var lastEntry = Engine.PersistentState.LastLogEntry();
                SendEmptyAppendEntry(lastEntry, messageHandler);
                long lastMatchedLogIndex = SyncWithFollower(messageHandler);
                var stopWatch = new Stopwatch();
                while (true)
                {
                    var entries = Engine.PersistentState.LogEntriesAfter(lastMatchedLogIndex)
                        .Take(Engine.Options.MaxEntriesPerRequest)
                        .ToArray();
                    //TODO: keep track of the last topology change index
                    int positionOfTopologyChange = entries.Length-1;
                    for (; positionOfTopologyChange > 0; positionOfTopologyChange--)
                    {
                        var entry = entries[positionOfTopologyChange];
                        if (entry.IsTopologyChange.HasValue && entry.IsTopologyChange.Value)
                        {
                            break;
                        }
                    }
                    var append = new AppendEntries()
                    {
                        Term = Engine.PersistentState.CurrentTerm,
                        PrevLogIndex = lastMatchedLogIndex,
                        PrevLogTerm = Engine.PersistentState.TermFor(lastMatchedLogIndex),
                        Entries = entries,
                        LeaderCommit = Engine.StateMachine.LastAppliedIndex,
                        PositionOfTopologyChange = positionOfTopologyChange
                    };
                    Console.WriteLine($"Sending heartbeat to {nodeInfo.Name} at {DateTime.UtcNow}\n {JsonConvert.SerializeObject(append)}");

                    messageHandler.WriteMessage(MessageType.AppendEntries, append);
                    if(_cancellation.IsCancellationRequested)
                        throw new OperationCanceledException();
                    var response = messageHandler.ReadMessage<AppendEntriesResponse>();
                    //This can only mean that there is a new leader in town
                    if (response.Success == false)
                        return;
                    lastMatchedLogIndex = entries.LastOrDefault()?.Index ?? lastMatchedLogIndex;
                    if (Engine.CurrentTopology.IsPromotable(nodeInfo.Name) && response.LastLogIndex == Engine.CommitIndex)
                    {
                        PromoteNodeToVoter(nodeInfo);
                    }
                    if (_cancellation.IsCancellationRequested)
                        throw new OperationCanceledException();
                    Thread.Sleep(Engine.PersistentState.HeartbeatTimeInMs - (int)stopWatch.ElapsedMilliseconds);
                    stopWatch.Restart(); //shaving down the time it takes to read/write the messages
                }
            }
        }

        private void PromoteNodeToVoter(NodeConnectionInfo nodeInfo)
        {
            var upgradedNode = Engine.CurrentTopology.GetNodeByName(nodeInfo.Name);
            if (upgradedNode == null)
                return;
            var requestTopology = new Topology(
                Engine.CurrentTopology.TopologyId,
                Engine.CurrentTopology.AllVotingNodes.Union(new[] { upgradedNode }),
                Engine.CurrentTopology.NonVotingNodes,
                Engine.CurrentTopology.PromotableNodes.Where(x => x != upgradedNode)
                );
            if (Engine.CurrentlyChangingTopology() == false)
            {
                return;
            }

            Engine.ModifyTopology(requestTopology);
        }

        private long SyncWithFollower(MessageHandler messageHandler)
        {
            var response = messageHandler.ReadMessage<AppendEntriesResponse>();
            if (response.Success)
            {
                return response.LastLogIndex;
            }
            //We are been refused, there is probably a new leader in town           
            if(response.Success == false && response.Negotiation == null)
                throw new Exception("I'm going to be replaced soon");//TODO:throw suitable exception
            var Term = Engine.PersistentState.CurrentTerm;
            var maxIndex = response.Negotiation.MaxIndex;
            var minIndex = response.Negotiation.MaxIndex;
            var midIndex = response.Negotiation.MidpointIndex;
            do
            {
                if (response.Negotiation.MidpointTerm == Engine.PersistentState.TermFor(midIndex))
                {
                    minIndex = midIndex + 1;
                }
                else
                {
                    maxIndex = midIndex - 1;
                }
                midIndex = (maxIndex + minIndex)/2;
                messageHandler.WriteMessage(MessageType.AppendEntries, new AppendEntries()
                {
                    Term = Term,
                    PrevLogIndex = midIndex,
                    PrevLogTerm = Engine.PersistentState.TermFor(midIndex),
                    Entries = null,
                });
                if (_cancellation.IsCancellationRequested)
                    throw new OperationCanceledException(); //TODO:better message 
                response = messageHandler.ReadMessage<AppendEntriesResponse>();
            } while (minIndex < maxIndex);
            return response.LastLogIndex;
        }

        private void SendEmptyAppendEntry(LogEntry lastEntry, MessageHandler messageHandler)
        {
            if (lastEntry == null)
            {
                messageHandler.WriteMessage(MessageType.AppendEntries, new AppendEntries()
                {
                    Term = Engine.PersistentState.CurrentTerm,
                    PrevLogIndex = 0L,
                    PrevLogTerm = 0L,
                    Entries = null,
                    LeaderCommit = 0L,
                });
            }
            else
            {
                messageHandler.WriteMessage(MessageType.AppendEntries, new AppendEntries()
                {
                    Term = Engine.PersistentState.CurrentTerm,
                    PrevLogIndex = lastEntry.Index,
                    PrevLogTerm = lastEntry.Term,
                    Entries = null,
                    LeaderCommit = Engine.StateMachine.LastAppliedIndex,
                });
            }
        }

        private Stream OpenCommunicationWith(NodeConnectionInfo nodeInfo)
        {            
            return nodeInfo.ConnectFunc.Invoke(Engine.Name);
        }

        private Stopwatch sw = new Stopwatch();
        public override void HandleTimeout()
        {
            Console.WriteLine(sw.ElapsedMilliseconds);            
            sw.Restart();
            var quorumSize = Engine.CurrentTopology.QuorumSize;
            if (quorumSize == 1)
            {
                return;
            }
            var latency = GetQuorumLatencyInMilliseconds(quorumSize);
            if (latency < Engine.PersistentState.ElectionTimeInMs)
            {
                return;
            }
            //TODO: maybe request most up to date peer to become a candidate?
            Engine.SetState(RaftEngineState.FollowerAfterSteppingDown);
        }

        private long GetQuorumLatencyInMilliseconds(int quorumSize)
        {
            var now = DateTime.UtcNow;
            //TODO:change linq to for
            var responses =
                CommitIndexDictionary.Values.Where(x => Engine.CurrentTopology.IsVoter(x.Name))
                    .OrderBy(x => x.LastResponseTime)
                    .Select(x => (long)(now - x.LastResponseTime).TotalMilliseconds)
                    .ToList();

            return responses[Math.Min(quorumSize - 1, responses.Count - 1)];
        }

        public override RaftEngineState State => RaftEngineState.Leader;

        public override void Dispose()
        {
            TimeoutEvent.Dispose();
            _cancellation.Cancel();
            _eventLoop.Join();
            foreach (var thread in _peersThreads.Values)
            {
                thread.Join(); //TODO: timeout and such
                base.Dispose();
            }            
        }
        
        private BlockingCollection<FollowerAppendEntriesLastIndexAndTiming> _appendEntriesLastIndexWithTiming = new BlockingCollection<FollowerAppendEntriesLastIndexAndTiming>();
        private class FollowerAppendEntriesLastIndexAndTiming
        {
            public string Name { get; set; }
            public long LastIndex { get; set; }
            public DateTime LastResponseTime { get; set; }
        }

    }
}
