using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Rachis
{
    public enum RaftEngineState
    {
        None,
        Follower,
        FollowerAfterSteppingDown, //this state has the same behavior as a follower just sets a flag to avoid leadership
        Leader,
        Candidate
    }
}
