using System;

namespace Raven.Client.Documents.Operations.Replication
{
    [Flags]
    public enum PullReplicationMode
    {
        None = 0,
        Outgoing = 1,
        Incoming = 2,
    }
}
