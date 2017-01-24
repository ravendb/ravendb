using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rachis.Communication;
using Rachis.Messages;

namespace Rachis.Behaviors
{
    public class CandidateStateBehavior : AbstractRaftStateBehavior
    {
        public CandidateStateBehavior(RaftEngine engine) : base(engine)
        {
        }

        public override RaftEngineState State => RaftEngineState.Candidate;

        public override void HandleTimeout()
        {
            throw new NotImplementedException();
        }

        //This override should never be called
        public override void HandleOnGoingCommunicationFromLeader(ITransportBus transport, CancellationToken ct, AppendEntries ae)
        {
            //TODO:log race condition
            Engine.SetState(RaftEngineState.Follower);
            Engine.StateBehavior.HandleOnGoingCommunicationFromLeader(transport, ct, ae);
        }

        public override void Dispose()
        {
            //TODO:Will need to dispose of event loop and any tcp connection
            base.Dispose();
        }

        public override void HandleNewConnection(ITransportBus transport, CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}
