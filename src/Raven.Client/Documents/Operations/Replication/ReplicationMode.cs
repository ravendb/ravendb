using System;

namespace Raven.Client.Documents.Operations.Replication
{
    [Flags]
    public enum ReplicationMode
    {
        None = 0,
        Pull = 1,
        Push = 2,
        Invalid = 4
    }
}
