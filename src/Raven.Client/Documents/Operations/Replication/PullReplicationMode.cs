using System;

namespace Raven.Client.Documents.Operations.Replication
{
    [Flags]
    public enum PullReplicationMode
    {
        None = 0,
        Read = 1,
        Write = 2,
    }
}
