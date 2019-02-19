using System;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Raven.Server.Rachis
{
    public class CandidateAmbassador : IDisposable
    {
        private readonly RachisConsensus _engine;
        private readonly Candidate _candidate;
        private readonly string _tag;
        private readonly string _url;
        private string _statusMessage;
        public string StatusMessage
        {
            get => _statusMessage;
            set
            {
                if (_statusMessage == value)
                    return;

                _statusMessage = value;
                _engine.NotifyTopologyChange();
            }
        }
        public AmbassadorStatus Status;
        private PoolOfThreads.LongRunningWork _candidateAmbassadorLongRunningWork;
        private readonly MultipleUseFlag _running = new MultipleUseFlag(true);
        public long TrialElectionWonAtTerm { get; set; }
        public long RealElectionWonAtTerm { get; set; }
        public volatile int ClusterCommandsVersion; // default is 0
        public string Tag => _tag;

        private RemoteConnection _connection;
        private RemoteConnection _publishedConnection;

        public CandidateAmbassador(RachisConsensus engine, Candidate candidate, string tag, string url)
        {
            _engine = engine;
            _candidate = candidate;
            _tag = tag;
            _url = url;
            Status = AmbassadorStatus.Started;
            StatusMessage = $"Started Candidate Ambassador for {_engine.Tag} > {_tag}";
        }

        public void Start()
        {
            _candidateAmbassadorLongRunningWork =
                PoolOfThreads.GlobalRavenThreadPool.LongRunning(x => Run(), null, $"Candidate Ambassador for {_engine.Tag} > {_tag}");
        }

        public void Dispose()
        {
            _running.Lower();
            DisposeConnectionIfNeeded();
            if (_candidateAmbassadorLongRunningWork != null && _candidateAmbassadorLongRunningWork.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
            {
                while (_candidateAmbassadorLongRunningWork.Join(1000) == false)
                {
                    // the thread may have create a new connection, so need
                    // to dispose that as well
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info(
                            $"CandidateAmbassador for {_tag}: Waited for a full second for thread {_candidateAmbassadorLongRunningWork.ManagedThreadId} " +
                            $"({(_candidateAmbassadorLongRunningWork.Join(0) ? "finished" : "running")}) to finish, after the elections were {_candidate.ElectionResult}");
                    }
                    DisposeConnectionIfNeeded();
                }
            }

            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"Dispose CandidateAmbassador for {_tag} after the elections were {_candidate.ElectionResult}");
            }
        }

        private void DisposeConnectionIfNeeded()
        {
            if (_candidate.ElectionResult != ElectionResult.Won)
            {
                Volatile.Read(ref _connection)?.Dispose();
            }
        }

        /// <summary>
        /// This method may run for a long while, as we are trying to get agreement 
        /// from a majority of the cluster
        /// </summary>
        private void Run()
        {
            try
            {
                while (_candidate.Running && _running)
                {
                    long currentElectionTerm = -1;
                    try
                    {
                        Stream stream;
                        Action disconnect;
                        try
                        {
                            var connection = _engine.ConnectToPeer(_url, _tag, _engine.ClusterCertificate).Result;
                            stream = connection.Stream;
                            disconnect = connection.Disconnect;

                            if (_candidate.Running == false)
                                break;
                        }
                        catch (Exception e)
                        {
                            Status = AmbassadorStatus.FailedToConnect;
                            StatusMessage = $"Failed to connect with {_tag}.{Environment.NewLine} " + e.Message;
                            if (_engine.Log.IsInfoEnabled)
                            {
                                _engine.Log.Info($"CandidateAmbassador for {_tag}: Failed to connect to remote peer: " + _url, e);
                            }

                            // wait a bit
                            _candidate.WaitForChangeInState();
                            continue; // we'll retry connecting
                        }

                        Status = AmbassadorStatus.Connected;
                        StatusMessage = $"Connected to {_tag}";

                        Stopwatch sp;
                        _connection?.Dispose();
                        _connection = new RemoteConnection(_tag, _engine.Tag, _candidate.ElectionTerm, stream, disconnect);

                        using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        {
                            ClusterTopology topology;
                            long lastLogIndex;
                            long lastLogTerm;
                            using (context.OpenReadTransaction())
                            {
                                topology = _engine.GetTopology(context);
                                lastLogIndex = _engine.GetLastEntryIndex(context);
                                lastLogTerm = _engine.GetTermForKnownExisting(context, lastLogIndex);
                            }

                            Debug.Assert(topology.TopologyId != null);
                            _connection.Send(context, new RachisHello
                            {
                                TopologyId = topology.TopologyId,
                                DebugSourceIdentifier = _engine.Tag,
                                DebugDestinationIdentifier = _tag,
                                ElectionTimeout = (int)_engine.ElectionTimeout.TotalMilliseconds,
                                SendingThread = Thread.CurrentThread.ManagedThreadId,
                                InitialMessageType = InitialMessageType.RequestVote,
                                DestinationUrl = _url,
                                SourceUrl = _engine.Url
                            });

                            while (_candidate.Running)
                            {
                                RequestVoteResponse rvr;
                                
                                currentElectionTerm = _candidate.ElectionTerm;
                                if (_candidate.IsForcedElection == false &&
                                    _candidate.RunRealElectionAtTerm != currentElectionTerm)
                                {
                                    sp = Stopwatch.StartNew();
                                    _connection.Send(context, new RequestVote
                                    {
                                        Source = _engine.Tag,
                                        Term = currentElectionTerm,
                                        IsForcedElection = false,
                                        IsTrialElection = true,
                                        LastLogIndex = lastLogIndex,
                                        LastLogTerm = lastLogTerm
                                    });

                                    rvr = _connection.Read<RequestVoteResponse>(context);

                                    ClusterCommandsVersion = rvr.ClusterCommandsVersion ?? 400;

                                    if (_engine.Log.IsInfoEnabled)
                                        _engine.Log.Info($"Candidate RequestVote trial vote req/res took {sp.ElapsedMilliseconds:#,#;;0} ms");

                                    if (rvr.Term > currentElectionTerm)
                                    {
                                        var message =
                                            $"Candidate ambassador for {_tag}: found election term {rvr.Term:#,#;;0} that is higher than ours {currentElectionTerm:#,#;;0}";
                                        // we need to abort the current elections

                                        if (_engine.Log.IsInfoEnabled)
                                        {
                                            _engine.Log.Info($"CandidateAmbassador for {_tag}: {message}");
                                        }

                                        _engine.FoundAboutHigherTerm(rvr.Term, "Higher term found from node " + Tag);
                                        _engine.SetNewState(RachisState.Follower, null, rvr.Term, message);
                                        RachisInvalidOperationException.Throw(message);
                                    }

                                    NotInTopology = rvr.NotInTopology;
                                    if (rvr.VoteGranted == false)
                                    {
                                        if (_engine.Log.IsInfoEnabled)
                                        {
                                            _engine.Log.Info($"CandidateAmbassador for {_tag}: Got a negative response " +
                                                             $"from {_tag} in {rvr.Term:#,#;;0} reason: {rvr.Message}");
                                        }

                                        // we go a negative response here, so we can't proceed
                                        // we'll need to wait until the candidate has done something, like
                                        // change term or given up
                                        _candidate.WaitForChangeInState();
                                        continue;
                                    }

                                    if (_engine.Log.IsInfoEnabled)
                                    {
                                        _engine.Log.Info($"CandidateAmbassador for {_tag}: Got a positive response " +
                                                         $"for trial elections from {_tag} in {rvr.Term:#,#;;0}: {rvr.Message}");
                                    }

                                    TrialElectionWonAtTerm = rvr.Term;
                                    _candidate.WaitForChangeInState();
                                    continue;
                                }

                                sp = Stopwatch.StartNew();
                                _connection.Send(context, new RequestVote
                                {
                                    Source = _engine.Tag,
                                    Term = currentElectionTerm,
                                    IsForcedElection = _candidate.IsForcedElection,
                                    IsTrialElection = false,
                                    LastLogIndex = lastLogIndex,
                                    LastLogTerm = lastLogTerm
                                });

                                rvr = _connection.Read<RequestVoteResponse>(context);
                                ClusterCommandsVersion = rvr.ClusterCommandsVersion ?? 400;

                                if (_engine.Log.IsInfoEnabled)
                                    _engine.Log.Info($"Candidate RequestVote real vote req/res took {sp.ElapsedMilliseconds:#,#;;0} ms");

                                if (rvr.Term > currentElectionTerm)
                                {
                                    var message = $"CandidateAmbassador for {_tag}: found election term {rvr.Term:#,#;;0} " +
                                                  $"that is higher than ours {currentElectionTerm:#,#;;0}";
                                    if (_engine.Log.IsInfoEnabled)
                                    {
                                        _engine.Log.Info($"CandidateAmbassador for {_tag}: {message}");
                                    }

                                    // we need to abort the current elections
                                    _engine.FoundAboutHigherTerm(rvr.Term, "Got higher term from node: " + Tag);
                                    _engine.SetNewState(RachisState.Follower, null, rvr.Term, message);
                                    RachisInvalidOperationException.Throw(message);
                                }

                                NotInTopology = rvr.NotInTopology;
                                if (rvr.VoteGranted == false)
                                {
                                    if (_engine.Log.IsInfoEnabled)
                                    {
                                        _engine.Log.Info($"CandidateAmbassador for {_tag}: Got a negative response " +
                                                         $"from {_tag} in {rvr.Term:#,#;;0} reason: {rvr.Message}");
                                    }

                                    // we go a negative response here, so we can't proceed
                                    // we'll need to wait until the candidate has done something, like
                                    // change term or given up
                                    _candidate.WaitForChangeInState();
                                    continue;
                                }

                                if (_engine.Log.IsInfoEnabled)
                                {
                                    _engine.Log.Info($"CandidateAmbassador for {_tag}: Got a positive response " +
                                                     $"from {_tag} in {rvr.Term:#,#;;0}: {rvr.Message}");
                                }

                                RealElectionWonAtTerm = rvr.Term;

                                var copy = _connection;
                                Interlocked.Exchange(ref _publishedConnection, copy);

                                _candidate.WaitForChangeInState();

                                if (Interlocked.CompareExchange(ref _publishedConnection, null, copy) != copy)
                                {
                                    if (_engine.Log.IsInfoEnabled)
                                    {
                                        _engine.Log.Info($"The candidate ambassador connection for {_tag} will be reused.");
                                    }
                                    // we won the election so our connection is being reused.
                                    _connection = null; // we no longer own the connection
                                    return;
                                }
                            }
                        }
                    }
                    catch (Exception e) when (RachisConsensus.IsExpectedException(e) || e is RachisConcurrencyException)
                    {
                        NotifyAboutAmbassadorClosing(_connection, currentElectionTerm, e);
                        break;
                    }
                    catch (Exception e)
                    {
                        Status = AmbassadorStatus.FailedToConnect;
                        StatusMessage = $"Failed to get vote from {_tag}.{Environment.NewLine}" + e.Message;
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"CandidateAmbassador for {_tag}: Failed to get vote from remote peer url={_url} tag={_tag}", e);
                        }

                        _connection?.Dispose();
                        _candidate.WaitForChangeInState();
                    }
                }
                // we reach this code if the elections are over but our candidate was not elected.
                _connection?.Dispose();
            }
            catch (Exception e)
            {
                Status = AmbassadorStatus.FailedToConnect;
                StatusMessage = $"Failed to talk to {_url}.{Environment.NewLine}" + e;
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info("Failed to talk to remote peer: " + _url, e);
                }
            }
        }
        
        public bool TryGetPublishedConnection(out RemoteConnection connection)
        {
            var copy = _publishedConnection;
            if (copy == null)
            {
                connection = default;
                return false;
            }

            if (Interlocked.CompareExchange(ref _publishedConnection, null, copy) != copy)
            {
                connection = default;
                return false;
            }

            connection = copy;
            return true;
        }

        private void NotifyAboutAmbassadorClosing(RemoteConnection connection, long currentElectionTerm, Exception e)
        {
            Status = AmbassadorStatus.Closed;
            StatusMessage = "Closed";
            if (e is OperationCanceledException)
                SendElectionResult(_engine, connection, currentElectionTerm, _candidate.ElectionResult);
        }

        public static void SendElectionResult(RachisConsensus engine, RemoteConnection connection, long currentElectionTerm, ElectionResult result)
        {
            using (engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                if (engine.Log.IsInfoEnabled)
                {
                    engine.Log.Info($"Sending from {connection.Source} to {connection.Dest} the election result: " +
                                     $"'{result}' at term {currentElectionTerm:#,#;;0}");
                }
                connection.Send(context, new RequestVote
                {
                    Source = engine.Tag,
                    Term = currentElectionTerm,
                    ElectionResult = result
                });
            }
        }

        public bool NotInTopology { get; private set; }
    }
}
