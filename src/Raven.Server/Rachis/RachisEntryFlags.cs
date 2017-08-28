using System;

namespace Raven.Server.Rachis
{
    [Flags]
    public enum RachisEntryFlags
    {
        Invalid,
        Noop = 1,// first commit in every term
        Topology = 2,
        StateMachineCommand = 4
    }
}
