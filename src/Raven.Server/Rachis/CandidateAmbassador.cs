using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis
{
    public class CandidateAmbassador : IDisposable
    {
        private readonly RachisConsensus _engine;
        private readonly Candidate _candidate;
        private readonly string _url;
        private readonly string _apiKey;
        public string Status;
        private Thread _thread;
        public long TrialElectionWonAtTerm { get; set; }
        public long ReadlElectionWonAtTerm { get; set; }

        public CandidateAmbassador(RachisConsensus engine, Candidate candidate, string url, string apiKey)
        {
            _engine = engine;
            _candidate = candidate;
            _url = url;
            _apiKey = apiKey;
            Status = "Started";
        }


        public void Start()
        {
            _thread = new Thread(Run)
            {
                Name = "Candidate Ambasaddor for " + _url,
                IsBackground = true
            };
            _thread.Start();
        }

        public void Dispose()
        {
            //TODO: shutdown notification of some kind?
            if (_thread != null && _thread.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
                _thread.Join();
        }

        /// <summary>
        /// This method may run for a long while, as we are trying to get agreement 
        /// from a majority of the cluster
        /// </summary>
        private void Run()
        {
            try
            {
                while (true)
                {
                    //TODO: need a way to shut this down when we are no longer leader / shutting down
                    Stream stream = null;
                    try
                    {
                        try
                        {
                            stream = RachisConsensus.ConenctToPeer(_url, _apiKey);
                        }
                        catch (Exception e)
                        {
                            Status = "Failed - " + e.Message;
                            if (_engine.Log.IsInfoEnabled)
                            {
                                _engine.Log.Info("Failed to connect to remote peer: " + _url, e);
                            }
                            // wait a bit
                            _candidate.WaitForChangeInState();
                            continue; // we'll retry connecting
                        }
                        Status = "Connected";
                        using (var connection = new RemoteConnection(stream))
                        {
                            while (true)
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

                                    connection.Send(context, new RachisHello
                                    {
                                        TopologyId = topology.TopologyId,
                                        DebugSourceIdentifier = _engine.GetDebugInformation(),
                                        InitialMessageType = InitialMessageType.RequestVote
                                    });

                                    RequestVoteResponse rvr;
                                    if (_candidate.IsForcedElection == false)
                                    {
                                        connection.Send(context, new RequestVote
                                        {
                                            Term = _candidate.ElectionTerm,
                                            IsForcedElection = false,
                                            IsTrialElection = true,
                                            LastLogIndex = lastLogIndex,
                                            LastLogTerm = lastLogTerm
                                        });

                                        rvr = connection.Read<RequestVoteResponse>(context);
                                        if (rvr.Term != _engine.CurrentTerm)
                                        {
                                            _candidate.HigherTermDiscovered(rvr.Term);
                                            return;
                                        }

                                        if (rvr.VoteGranted == false)
                                        {
                                            // we go a negative response here, so we can't proceed
                                            // we'll need to wait until the candidate has done something, like
                                            // change term or given up
                                            _candidate.WaitForChangeInState();
                                            continue;
                                        }
                                        TrialElectionWonAtTerm = rvr.Term;
                                    }

                                    _candidate.WaitForChangeInState();

                                    if (_candidate.RunRealElection == false)
                                        continue;

                                    connection.Send(context, new RequestVote
                                    {
                                        Term = _candidate.ElectionTerm,
                                        IsForcedElection = _candidate.IsForcedElection,
                                        IsTrialElection = false,
                                        LastLogIndex = lastLogIndex,
                                        LastLogTerm = lastLogTerm
                                    });

                                    rvr = connection.Read<RequestVoteResponse>(context);
                                    if (rvr.Term != _engine.CurrentTerm)
                                    {
                                        _candidate.HigherTermDiscovered(rvr.Term);
                                        return;
                                    }

                                    if (rvr.VoteGranted == false)
                                    {
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
                            _engine.Log.Info("Failed to get vote from remote peer: " + _url, e);
                        }
                        _candidate.WaitForChangeInState();
                    }
                    finally
                    {
                        stream?.Dispose();
                        Status = "Disconnected";
                    }
                }
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
    }
}