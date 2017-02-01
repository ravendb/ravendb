using System;
using System.Collections.Generic;
using System.IO;
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

    }
}