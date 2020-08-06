using System;

namespace Raven.Client.Documents.Operations.Replication
{
    [Flags]
    public enum PullReplicationMode
    {
        None = 0,
        HubToSink = 1,
        SinkToHub = 2,
    }
}
