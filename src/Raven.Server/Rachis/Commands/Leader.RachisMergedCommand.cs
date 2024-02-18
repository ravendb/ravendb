using System;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using System.Diagnostics.CodeAnalysis;
using Sparrow.Json;
using Voron.Impl;

namespace Raven.Server.Rachis
{
    public partial class Leader
    {
        public sealed class RachisMergedCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
        {
            public CommandBase Command;
            public BlittableJsonReaderObject CommandAsJson;
            public Task<(long Index, object Result)> TaskResult { get; private set; }
            public BlittableResultWriter BlittableResultWriter { get; init; }

            private readonly Leader _leader;
            private readonly RachisConsensus _engine;
            private const string _leaderDisposedMessage = "We are no longer the leader, this leader is disposed";

            public RachisMergedCommand([NotNull] Leader leader, [NotNull] RachisConsensus engine)
            {
                _leader = leader ?? throw new ArgumentNullException(nameof(leader));
                _engine = engine ?? throw new ArgumentNullException(nameof(engine));
            }

            protected override long ExecuteCmd(ClusterOperationContext context)
            {
                try
                {
                    InsertCommandToLeaderLog(context);
                }
                catch (Exception e)
                {
                    if (e is AggregateException ae)
                        e = ae.ExtractSingleInnerException();

                    if (_leader._running.IsRaised() == false)
                        e = new NotLeadingException(_leaderDisposedMessage, e);

                    _leader._errorOccurred.TrySetException(e);

                    TaskResult = Task.FromException<(long, object)>(e);
                }

                return 1;
            }

            private void InsertCommandToLeaderLog(ClusterOperationContext context)
            {
                _engine.GetLastCommitIndex(context, out var lastCommitted, out _);
                if (_leader._running.IsRaised() == false) // not longer leader
                {
                    // throw lostLeadershipException;
                    TaskResult = Task.FromException<(long, object)>(new NotLeadingException(_leaderDisposedMessage));
                    return;
                }

                if (_engine.LogHistory.HasHistoryLog(context, Command.UniqueRequestId, out var index, out var result, out var exception))
                {
                    // if this command is already committed, we can skip it and notify the caller about it
                    if (lastCommitted >= index)
                    {
                        if (exception != null)
                        {
                            TaskResult = Task.FromException<(long, object)>(exception);
                        }
                        else
                        {
                            if (result != null)
                            {
                                if (BlittableResultWriter != null)
                                {
                                    BlittableResultWriter.CopyResult(result);
                                    //The result are consumed by the `CopyResult` and the context of the result from `HasHistoryLog` is not valid outside
                                    //so we `TrySetResult` to null to make sure no use of invalid context 
                                    result = null;
                                }
                                else
                                {
                                    result = Command.FromRemote(result);
                                }
                            }

                            TaskResult = Task.FromResult<(long, object)>((index, result));
                        }
                        return;
                    }
                }
                else
                {
                    _engine.InvokeBeforeAppendToRaftLog(context, this);
                    index = _engine.InsertToLeaderLog(context, _leader.Term, CommandAsJson, RachisEntryFlags.StateMachineCommand);
                }
               
                if (_leader._entries.TryGetValue(index, out var state) == false)
                {
                    var tcs = new TaskCompletionSource<(long, object)>(TaskCreationOptions.RunContinuationsAsynchronously);
                    state = new CommandState
                    {
                        // we need to add entry inside write tx lock to avoid
                        // a situation when command will be applied (and state set)
                        // before it is added to the entries list

                        CommandIndex = index,
                        TaskCompletionSource = tcs,
                    };
                    _leader._entries[index] = state;
                    context.Transaction.InnerTransaction.LowLevelTransaction.AfterCommitWhenNewTransactionsPrevented += AfterCommit;
                }

                TaskResult = state.TaskCompletionSource.Task;

                if (BlittableResultWriter != null)
                    //If we need to return a blittable as a result the context must be valid for each command that tries to read from it.
                    //So we let the command provide the method to handle the write while the command is aware of its context validation status.
                    //We can have multiple delegates if the same command was sent multiple times (multiple attempts)
                    //https://issues.hibernatingrhinos.com/issue/RavenDB-20762
                    state.WriteResultAction += BlittableResultWriter.CopyResult;

            }

            private void AfterCommit(LowLevelTransaction tx)
            {
                try
                {
                    _leader._newEntry.Set();
                }
                catch (ObjectDisposedException)
                {
                    // _newEntry is disposed because _leader is already disposed
                }
            }

            public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
            {
                throw new NotImplementedException();
            }
        }

    }
}
