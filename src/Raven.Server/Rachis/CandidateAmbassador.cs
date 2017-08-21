using System;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis
{
    public class CandidateAmbassador : IDisposable
    {
        private readonly RachisConsensus _engine;
        private readonly Candidate _candidate;
        private readonly string _tag;
        private readonly string _url;
        private readonly X509Certificate2 _certificate;
        public string StatusMessage;
        public AmbassadorStatus Status;
        private Thread _thread;
        private Stream _conenctToPeer;
        private bool _disposed;
        public long TrialElectionWonAtTerm { get; set; }
        public long ReadlElectionWonAtTerm { get; set; }
        public string Tag => _tag;

        public CandidateAmbassador(RachisConsensus engine, Candidate candidate, string tag, string url, X509Certificate2 certificate)
        {
            _engine = engine;
            _candidate = candidate;
            _tag = tag;
            _url = url;
            _certificate = certificate;
            Status = AmbassadorStatus.Started;
            StatusMessage = $"Started Candidate Ambasaddor for {_engine.Tag} > {_tag}";
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
            _disposed = true;
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
                while (_candidate.Running && _disposed == false)
                {
                    _conenctToPeer = null;
                    try
                    {
                        try
                        {
                            using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                            {
                                _conenctToPeer = _engine.ConnectToPeer(_url, _certificate, context).Result;
                            }

                            if (_candidate.Running == false)
                                break; 
                        }
                        catch (Exception e)
                        {
                            Status = AmbassadorStatus.FailedToConnect;
                            StatusMessage = $"Failed to connect with {_tag}.{Environment.NewLine} " + e.Message;
                            if (_engine.Log.IsInfoEnabled)
                            {
                                _engine.Log.Info($"CandidateAmbassador {_engine.Tag}: Failed to connect to remote peer: " + _url, e);
                            }
                            // wait a bit
                            _candidate.WaitForChangeInState();
                            continue; // we'll retry connecting
                        }
                        Status = AmbassadorStatus.Connected;
                        StatusMessage = $"Connected to {_tag}";
                        using (var connection = new RemoteConnection(_tag, _engine.Tag, _conenctToPeer))
                        {
                            try
                            {
                                _engine.AppendStateDisposable(_candidate, connection);
                            }
                            catch (ConcurrencyException)
                            {
                                // we probably lost the election, because someone else changed our state to follower
                                // we'll still return to the top of the loop to ensure that this is the case
                                continue; 
                            }
                            
                            while (_candidate.Running)
                            {
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
                                    connection.Send(context, new RachisHello
                                    {
                                        TopologyId = topology.TopologyId,
                                        DebugSourceIdentifier = _engine.Tag,
                                        DebugDestinationIdentifier = _tag,
                                        InitialMessageType = InitialMessageType.RequestVote
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
                                            throw new InvalidOperationException(message);
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
                                        throw new InvalidOperationException(message);
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
                        Status = AmbassadorStatus.FailedToConnect;
                        StatusMessage = $"Failed to get vote from {_tag}.{Environment.NewLine}" + e.Message;
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"CandidateAmbassador {_engine.Tag}: Failed to get vote from remote peer url={_url} tag={_tag}", e);
                        }
                        _candidate.WaitForChangeInState();
                    }
                    finally
                    {
                        _conenctToPeer?.Dispose();
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Status = AmbassadorStatus.Closed;
                StatusMessage = "Closed";
            }
            catch (ObjectDisposedException)
            {
                Status = AmbassadorStatus.Closed;
                StatusMessage = "Closed";
            }
            catch (AggregateException ae)
                when (ae.InnerException is OperationCanceledException || ae.InnerException is ObjectDisposedException)
            {
                Status = AmbassadorStatus.Closed;
                StatusMessage = "Closed";
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

        public bool NotInTopology { get; private set; }
    }
}
