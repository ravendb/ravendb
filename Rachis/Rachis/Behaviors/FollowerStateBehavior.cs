using System;
using System.IO;
using System.Threading;
using Newtonsoft.Json;
using Rachis.Commands;
using Rachis.Messages;

namespace Rachis.Behaviors
{
    public class FollowerStateBehavior : AbstractRaftStateBehavior
    {
        private Thread _thread;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private MessageHandler _messageHandler;

        public FollowerStateBehavior(RaftEngine engine,bool avoidLeadership = false) : base(engine)
        {            
            _cancellationTokenSource = new CancellationTokenSource();
            AvoidLeadership = avoidLeadership;
            var random = new Random();
            var timeoutPeriod = random.Next(engine.Options.ElectionTimeout / 2, engine.Options.ElectionTimeout);

            TimeoutEvent = new TimeoutEvent(timeoutPeriod);
            TimeoutEvent.TimeoutHappened += HandleTimeout;
        }

        public void Start(AppendEntries firstMsg,Stream s)
        {
            _thread = new Thread(TalkWithLeader)
            {
                Name = "Follower behavior on " + Engine.Name
            };
            _thread.Start(new Tuple<AppendEntries,Stream>(firstMsg,s));
        }

        private void TalkWithLeader(object o)
        {
            var msgWithStream = (Tuple<AppendEntries,Stream>)o;
            var msg = msgWithStream.Item1;
            var stream = msgWithStream.Item2;
            _messageHandler = new MessageHandler(stream);
            // TODO: error handling
            try
            {
                using (stream)
                {


                    var termForPrevEntry = Engine.PersistentState.TermFor(msg.PrevLogIndex);
                    if (termForPrevEntry != msg.PrevLogTerm)
                    {
                        //We are going to binary search a matching entry and apply the matching batch
                        NegotiateMatchEntryWithLeaderAndApplyEntries(msg);
                    }

                    //From here we should be in sync with the leader
                    while (true)
                    {
                        HandleValidAppendEntries(msg);

                        msg = ReadAppendEntries();
                        Console.WriteLine($"Received message from leader at {DateTime.UtcNow}\n {JsonConvert.SerializeObject(msg)}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                
            }
        }

        public override RaftEngineState State => RaftEngineState.Follower;


        private void NegotiateMatchEntryWithLeaderAndApplyEntries(AppendEntries appendEntries)
        {
            var minIndex = Math.Max(
                Engine.StateMachine.LastAppliedIndex,
                Engine.PersistentState.FirstLogIndex() ?? 0
            );
            var maxIndex =
                Math.Min(
                    Engine.PersistentState.LastLogIndex() ?? 0,
                    appendEntries.PrevLogIndex
                );

            var midpointIndex = maxIndex + minIndex / 2;

            var midpointTerm = Engine.PersistentState.TermFor(midpointIndex);
            while (minIndex < maxIndex)
            {
                _cancellationTokenSource.Token.ThrowIfCancellationRequested();

                //TODO: log the fact that we don't agree with the leader on the previous log entry and starting a binary search
                //The leader is expected to send entries between minIndex and maxIndex so we don't need to do boundary checks
                _messageHandler.WriteMessage(MessageType.AppendEntriesResponse, new AppendEntriesResponse
                {
                    Success = false,
                    Negotiation = new Negotiation
                    {
                        MidpointIndex = midpointIndex,
                        MidpointTerm = midpointTerm,
                        MinIndex = minIndex,
                        MaxIndex = maxIndex
                    }
                });

                var response = ReadAppendEntries();
                if (Engine.PersistentState.TermFor(response.PrevLogIndex) == response.PrevLogTerm)
                {
                    minIndex = midpointIndex + 1;
                }
                else
                {
                    maxIndex = midpointIndex - 1;
                }
                midpointIndex = (maxIndex + minIndex) / 2;
                midpointTerm = Engine.PersistentState.TermFor(midpointIndex);
            }

            _messageHandler.WriteMessage(MessageType.AppendEntriesResponse, new AppendEntriesResponse
            {
                Success = true,
                LastLogIndex = minIndex,
            });
        }

        private AppendEntries ReadAppendEntries()
        {
            _cancellationTokenSource.Token.ThrowIfCancellationRequested();            
            var appendEntries = _messageHandler.ReadMessage<AppendEntries>();
            TimeoutEvent.Defer();
            return appendEntries;
        }


        private void HandleValidAppendEntries(AppendEntries appendEntries)
        {
            TimeoutEvent.Defer();
            var lastIndex = Engine.PersistentState.AppendToLog(appendEntries.Entries);
            if (appendEntries.PositionOfTopologyChange >= 0)
            {
                var topologychange =
                    Command.FromBytes<TopologyChangeCommand>(
                        appendEntries.Entries[appendEntries.PositionOfTopologyChange].Data);
                Engine.PersistentState.SetCurrentTopology(topologychange.Requested);
                Engine.StartTopologyChange(topologychange);
            }
            var nextCommitIndex = Math.Min(appendEntries.LeaderCommit, lastIndex);
            if (nextCommitIndex > Engine.CommitIndex)
            {
                Engine.CommitEntries(nextCommitIndex); //this should be handled on a different thread
            }
            _messageHandler.WriteMessage(MessageType.AppendEntriesResponse, new AppendEntriesResponse
            {
                Success = true,
                LastLogIndex = lastIndex
            });
        }

        public override void Dispose()
        {
            _cancellationTokenSource.Cancel();
            if (Thread.CurrentThread != _thread)
                _thread.Join();
            base.Dispose();
        }
    }
}
