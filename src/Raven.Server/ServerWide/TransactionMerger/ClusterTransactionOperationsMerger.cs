using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.TransactionMerger;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.ServerWide.TransactionMerger;

public sealed class ClusterTransactionOperationsMerger : AbstractTransactionOperationsMerger<ClusterOperationContext, ClusterTransaction>
{
    public ClusterTransactionOperationsMerger(RavenConfiguration configuration, SystemTime time, CancellationToken shutdown)
        : base("Cluster", configuration, time, RavenLogManager.Instance.GetLoggerForServer<ClusterTransactionOperationsMerger>(), shutdown)
    {
    }

    internal override ClusterTransaction BeginAsyncCommitAndStartNewTransaction(ClusterTransaction previousTransaction, ClusterOperationContext currentContext)
    {
        return previousTransaction.BeginAsyncCommitAndStartNewTransaction(currentContext);
    }

    internal override void UpdateGlobalReplicationInfoBeforeCommit(ClusterOperationContext context)
    {
    }

    protected override void UpdateLastAccessTime(DateTime time)
    {
    }

    public Task Enqueue(Func<ClusterOperationContext, long> executeFunc)
    {
        var cmd = new ExecutableMergedCommand(executeFunc);
        return Enqueue(cmd);
    }

    public void EnqueueSync(Func<ClusterOperationContext, long> executeFunc)
    {
        Enqueue(executeFunc).GetAwaiter().GetResult();
    }

    private readonly ManualResetEventSlim _disposeEvent = new ManualResetEventSlim(false);
    public bool IsDisposed => _disposeEvent.IsSet;

    public override void Dispose()
    {
        if (IsDisposed)
            return;
        _disposeEvent.Set();
        base.Dispose();
    }

    private sealed class ExecutableMergedCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
    {
        private Func<ClusterOperationContext, long> _executeFunc;

        public ExecutableMergedCommand(Func<ClusterOperationContext, long> executeFunc)
        {
            _executeFunc = executeFunc;
        }

        protected override long ExecuteCmd(ClusterOperationContext context)
        {
            if (_executeFunc == null)
            {
                return 1;
            }
            return _executeFunc.Invoke(context);
        }

        public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
        {
            throw new NotImplementedException();
        }
    }
}
