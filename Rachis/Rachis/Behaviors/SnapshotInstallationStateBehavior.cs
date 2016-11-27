using System;
using System.IO;
using System.Threading.Tasks;
using Rachis.Commands;
using Rachis.Messages;

using Raven.Abstractions.Logging;

namespace Rachis.Behaviors
{
    public class SnapshotInstallationStateBehavior : AbstractRaftStateBehavior
    {
        private readonly Random _random;

        private Task _installingSnapshot;

        public SnapshotInstallationStateBehavior(RaftEngine engine) : base(engine)
        {
            _random = new Random((int)(engine.Name.GetHashCode() + DateTime.UtcNow.Ticks));
            Timeout = _random.Next(engine.Options.ElectionTimeout / 2, engine.Options.ElectionTimeout);
        }

        public override RaftEngineState State
        {
            get { return RaftEngineState.SnapshotInstallation; }
        }

        public override CanInstallSnapshotResponse Handle(CanInstallSnapshotRequest req)
        {
            if (_installingSnapshot == null)
            {
                return base.Handle(req);
            }
            return new CanInstallSnapshotResponse
            {
                From = Engine.Name,
                ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                IsCurrentlyInstalling = true,
                Message = "The node is in the process of installing a snapshot",
                Success = false
            };
        }

        public override InstallSnapshotResponse Handle(MessageContext context, InstallSnapshotRequest req, Stream stream)
        {
            if (_installingSnapshot != null)
            {
                return new InstallSnapshotResponse
                {
                    Success = false,
                    Message = "Cannot install snapshot because we are already installing a snapshot",
                    CurrentTerm = Engine.PersistentState.CurrentTerm,
                    From = Engine.Name,
                    ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                    LastLogIndex = Engine.PersistentState.LastLogEntry().Index
                };
            }


            if (FromOurTopology(req) == false)
            {
                _log.Info("Got an install snapshot message outside my cluster topology (id: {0}), ignoring", req.ClusterTopologyId);

                return new InstallSnapshotResponse
                {
                    Success = false,
                    Message = "Cannot install snapshot because the cluster topology id doesn't match, mine is: " + Engine.CurrentTopology.TopologyId,
                    CurrentTerm = Engine.PersistentState.CurrentTerm,
                    From = Engine.Name,
                    ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                    LastLogIndex = Engine.PersistentState.LastLogEntry().Index
                };
            }

            var lastLogEntry = Engine.PersistentState.LastLogEntry();
            if (req.Term < lastLogEntry.Term || req.LastIncludedIndex < lastLogEntry.Index)
            {
                stream.Dispose();

                return new InstallSnapshotResponse
                {
                    From = Engine.Name,
                    ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                    CurrentTerm = lastLogEntry.Term,
                    LastLogIndex = lastLogEntry.Index,
                    Message = string.Format("Snapshot is too old (term {0} index {1}) while we have (term {2} index {3})",
                        req.Term, req.LastIncludedIndex, lastLogEntry.Term, lastLogEntry.Index),
                    Success = false
                };
            }

            _log.Info("Received InstallSnapshotRequest from {0} until term {1} / {2}", req.From, req.LastIncludedTerm, req.LastIncludedIndex);

            Engine.OnSnapshotInstallationStarted();
            
            // this can be a long running task
            _installingSnapshot = Task.Run(() =>
            {
                try
                {
                    Engine.StateMachine.ApplySnapshot(req.LastIncludedTerm, req.LastIncludedIndex, stream);
                    Engine.PersistentState.MarkSnapshotFor(req.LastIncludedIndex, req.LastIncludedTerm, int.MaxValue);
                    Engine.PersistentState.SetCurrentTopology(req.Topology, req.LastIncludedIndex);
                    var tcc = new TopologyChangeCommand { Requested = req.Topology };
                    Engine.StartTopologyChange(tcc);
                    Engine.CommitTopologyChange(tcc);
                }
                catch (Exception e)
                {
                    _log.Warn(string.Format("Failed to install snapshot term {0} index {1}", req.LastIncludedIndex, req.LastIncludedIndex), e);
                    context.ExecuteInEventLoop(() =>
                    {
                        _installingSnapshot = null;
                    });
                }

                // we are doing it this way to ensure that we are single threaded
                context.ExecuteInEventLoop(() =>
                {
                    Engine.UpdateCurrentTerm(req.Term, req.From); // implicitly put us in follower state
                    _log.Info("Updating the commit index to the snapshot last included index of {0}", req.LastIncludedIndex);
                    Engine.OnSnapshotInstallationEnded(req.Term);

                    context.Reply(new InstallSnapshotResponse
                    {
                        From = Engine.Name,
                        ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                        CurrentTerm = req.Term,
                        LastLogIndex = req.LastIncludedIndex,
                        Success = true
                    });
                });
            });

            return null;
        }

        public override AppendEntriesResponse Handle(AppendEntriesRequest req)
        {
            if (_installingSnapshot == null)
            {
                return base.Handle(req);
            }

            var lastLogEntry = Engine.PersistentState.LastLogEntry();
            return new AppendEntriesResponse
                {
                    From = Engine.Name,
                    ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                    CurrentTerm = Engine.PersistentState.CurrentTerm,
                    LastLogIndex = lastLogEntry.Index,
                    LeaderId = Engine.CurrentLeader,
                    Message = "I am in the process of receiving a snapshot, so I cannot accept new entries at the moment",
                    Success = false
                };
        }


        public override void HandleTimeout()
        {
            Timeout = _random.Next(Engine.Options.ElectionTimeout / 2, Engine.Options.ElectionTimeout);
            LastHeartbeatTime = DateTime.UtcNow;// avoid busy loop while waiting for the snapshot
            _log.Info("Received timeout during installation of a snapshot. Doing nothing, since the node should finish receiving snapshot before it could change into candidate");
            //do nothing during timeout --> this behavior will go on until the snapshot installation is finished
        }

        public override void Dispose()
        {
            if (_log.IsDebugEnabled)
                _log.Debug("Disposing of SnapshotInstallationStateBehavior");
            base.Dispose();
        }
    }
}
