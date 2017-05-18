using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Raven.Client.Http;
using Raven.Client.Server.Tcp;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis
{
    public class CandidateAmbassador : IDisposable
    {
        private readonly RachisConsensus _engine;
        private readonly Candidate _candidate;
        private readonly string _tag;
        private readonly string _url;
        private readonly string _apiKey;
        public string Status;
        private Thread _thread;
        private Stream _conenctToPeer;
        public long TrialElectionWonAtTerm { get; set; }
        public long ReadlElectionWonAtTerm { get; set; }

        public CandidateAmbassador(RachisConsensus engine, Candidate candidate, string tag, string url, string apiKey)
        {
            _engine = engine;
            _candidate = candidate;
            _tag = tag;
            _url = url;
            _apiKey = apiKey;
            Status = "Started";
        }


        public void Start()
        {
            _thread = new Thread(Run)
            {
                Name = $"Candidate Ambasaddor for {_engine.Tag} > {_tag}",
                IsBackground = true
            };
            _thread.Start();
        }

        public void Dispose()
        {
            _conenctToPeer?.Dispose();
            if (_thread != null && _thread.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
            {
                while (_thread.Join(16) == false)
                {
                    _conenctToPeer?.Dispose();
                }
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info($"CandidateAmbassador {_engine.Tag}: Dispose");
                }
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
                while (_candidate.Running)
                {
                    _conenctToPeer = null;
                    try
                    {
                        try
                        {
                            TransactionOperationContext context;
                            using (_engine.ContextPool.AllocateOperationContext(out context))
                            {
                                _conenctToPeer = _engine.ConenctToPeer(_url, _apiKey, context).Result; 
                            }

                            if (_candidate.Running == false)
                                break; 
                        }
                        catch (Exception e)
                        {
                            Status = "Failed - " + e.Message;
                            if (_engine.Log.IsInfoEnabled)
                            {
                                _engine.Log.Info($"CandidateAmbassador {_engine.Tag}: Failed to connect to remote peer: " + _url, e);
                            }
                            // wait a bit
                            _candidate.WaitForChangeInState();
                            continue; // we'll retry connecting
                        }
                        Status = "Connected";
                        using (var connection = new RemoteConnection(_tag, _engine.Tag, _conenctToPeer))
                        {
                            _engine.AppendStateDisposable(_candidate, connection);
                            while (_candidate.Running)
                            {
                                TransactionOperationContext context;
                                using (_engine.ContextPool.AllocateOperationContext(out context))
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
                                    connection.Send(context, new RachisHello
                                    {
                                        TopologyId = topology.TopologyId,
                                        DebugSourceIdentifier = _engine.Tag,
                                        DebugDestinationIdentifier = _tag,
                                        InitialMessageType = InitialMessageType.RequestVote,
                                    });

                                    RequestVoteResponse rvr;
                                    var currentElectionTerm = _candidate.ElectionTerm;
                                    var engineCurrentTerm = _engine.CurrentTerm;
                                    if (_candidate.IsForcedElection == false || 
                                        _candidate.RunRealElectionAtTerm != currentElectionTerm)
                                    {
                                        connection.Send(context, new RequestVote
                                        {
                                            Source = _engine.Tag,
                                            Term = currentElectionTerm,
                                            IsForcedElection = false,
                                            IsTrialElection = true,
                                            LastLogIndex = lastLogIndex,
                                            LastLogTerm = lastLogTerm
                                        });

                                        rvr = connection.Read<RequestVoteResponse>(context);
                                        if (rvr.Term > currentElectionTerm)
                                        {
                                            var message = "Found election term " + rvr.Term + " that is higher than ours " + currentElectionTerm;
                                            // we need to abort the current elections
                                            _engine.SetNewState(RachisConsensus.State.Follower, null, engineCurrentTerm, message);
                                            if (_engine.Log.IsInfoEnabled)
                                            {
                                                _engine.Log.Info($"CandidateAmbassador {_engine.Tag}: {message}");
                                            }
                                            _engine.FoundAboutHigherTerm(rvr.Term);
                                            return;
                                        }
                                        NotInTopology = rvr.NotInTopology;
                                        if (rvr.VoteGranted == false)
                                        {
                                            if (_engine.Log.IsInfoEnabled)
                                            {
                                                _engine.Log.Info($"CandidateAmbassador {_engine.Tag}: Got a negative response from {_tag} reseason:{rvr.Message}");
                                            }
                                            // we go a negative response here, so we can't proceed
                                            // we'll need to wait until the candidate has done something, like
                                            // change term or given up
                                            _candidate.WaitForChangeInState();
                                            continue;
                                        }
                                        TrialElectionWonAtTerm = rvr.Term;

                                        _candidate.WaitForChangeInState();
                                    }

                                    connection.Send(context, new RequestVote
                                    {
                                        Source = _engine.Tag,
                                        Term = currentElectionTerm,
                                        IsForcedElection = _candidate.IsForcedElection,
                                        IsTrialElection = false,
                                        LastLogIndex = lastLogIndex,
                                        LastLogTerm = lastLogTerm
                                    });

                                    rvr = connection.Read<RequestVoteResponse>(context);
                                    if (rvr.Term > currentElectionTerm)
                                    {
                                        var message = "Found election term " + rvr.Term + " that is higher than ours " + currentElectionTerm;
                                        if (_engine.Log.IsInfoEnabled)
                                        {
                                            _engine.Log.Info($"CandidateAmbassador {_engine.Tag}: {message}");
                                        }
                                        // we need to abort the current elections
                                        _engine.SetNewState(RachisConsensus.State.Follower, null, engineCurrentTerm, message);
                                        _engine.FoundAboutHigherTerm(rvr.Term);
                                        return;
                                    }
                                    NotInTopology = rvr.NotInTopology;
                                    if (rvr.VoteGranted == false)
                                    {
                                        if (_engine.Log.IsInfoEnabled)
                                        {
                                            _engine.Log.Info($"CandidateAmbassador {_engine.Tag}: Got a negative response from {_tag} reseason:{rvr.Message}");
                                        }
                                        // we go a negative response here, so we can't proceed
                                        // we'll need to wait until the candidate has done something, like
                                        // change term or given up
                                        _candidate.WaitForChangeInState();
                                        continue;
                                    }
                                    ReadlElectionWonAtTerm = rvr.Term;
                                    _candidate.WaitForChangeInState();
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Status = "Failed - " + e.Message;
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"CandidateAmbassador {_engine.Tag}: Failed to get vote from remote peer url={_url} tag={_tag}", e);
                        }
                        _candidate.WaitForChangeInState();
                    }
                    finally
                    {
                        _conenctToPeer?.Dispose();
                        Status = "Disconnected";
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Status = "Closed";
            }
            catch (ObjectDisposedException)
            {
                Status = "Closed";
            }
            catch (AggregateException ae)
                when (ae.InnerException is OperationCanceledException || ae.InnerException is ObjectDisposedException)
            {
                Status = "Closed";
            }
            catch (Exception e)
            {
                Status = "Failed - " + e.Message;
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info("Failed to talk to remote peer: " + _url, e);
                }
            }
        }

        public bool NotInTopology { get; private set; }
    }
}