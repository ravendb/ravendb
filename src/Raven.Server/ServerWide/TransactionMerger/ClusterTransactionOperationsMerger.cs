using System;
using System.Threading;
using JetBrains.Annotations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.TransactionMerger;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Platform;

namespace Raven.Server.ServerWide.TransactionMerger;

public class ClusterTransactionOperationsMerger : AbstractTransactionOperationsMerger<ClusterOperationContext, ClusterTransaction>
{
    public ClusterTransactionOperationsMerger(RachisConsensus engine, RavenConfiguration configuration, SystemTime time, CancellationToken shutdown)
        : base("Cluster", configuration, time, shutdown)
    {
        IsEncrypted = engine.IsEncrypted;
        Is32Bits = PlatformDetails.Is32Bits || configuration.Storage.ForceUsing32BitsPager;
    }

    protected override bool IsEncrypted { get; }
    protected override bool Is32Bits { get; }

    internal override ClusterTransaction BeginAsyncCommitAndStartNewTransaction(ClusterTransaction previousTransaction, ClusterOperationContext currentContext)
    {
        throw new NotImplementedException();
    }

    internal override void UpdateGlobalReplicationInfoBeforeCommit(ClusterOperationContext context)
    {
    }

    protected override void UpdateLastAccessTime(DateTime time)
    {
    }
}
