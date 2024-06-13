using System;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using System.Diagnostics.CodeAnalysis;
using Raven.Client.Exceptions.Cluster;
using Sparrow.Json;
using Voron.Impl;

namespace Raven.Server.Rachis
{
    public partial class Leader
    {
        public sealed class RachisMergedCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>, IDisposable
        {
            private const string _leaderDisposedMessage = "We are no longer the leader, this leader is disposed";

            public CommandBase Command;
            private readonly TimeSpan _timeout;
            public BlittableResultWriter BlittableResultWriter { get; private set; }
            private TaskCompletionSource<Task<(long Index, object Result)>> _tcs = new TaskCompletionSource<Task<(long Index, object Result)>>(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly Leader _leader;
            private readonly RachisConsensus _engine;
            private readonly ClusterContextPool _pool;
            private IDisposable _ctxReturn;

            public RachisMergedCommand([NotNull] Leader leader, CommandBase command, TimeSpan timeout)
            {
                _leader = leader ?? throw new ArgumentNullException(nameof(leader));
                _engine = _leader._engine;
                _pool = _engine.ContextPool;
                Command = command;
                _timeout = timeout;
            }

            public void Initialize()
            {
                BlittableResultWriter = Command is IBlittableResultCommand crCommand ? new BlittableResultWriter(crCommand.WriteResult) : null;

                // we prepare the command _not_ under the write lock
                if (Command.Raw == null)
                {
                    _ctxReturn = _pool.AllocateOperationContext(out JsonOperationContext context);
                    var djv = Command.ToJson(context);
                    Command.Raw = context.ReadObject(djv, "prepare-raw-command");
                }
            }

            public async Task<(long Index, object Result)> Result()
            {
                var inner = await _tcs.Task;

                if (await inner.WaitWithTimeout(_timeout) == false)
                {
                    throw new TimeoutException($"Waited for {_timeout} but the command {Command.RaftCommandIndex} was not applied in this time.");
                }

                var r = await inner;
                return BlittableResultWriter == null ? r : (r.Index, BlittableResultWriter.Result);
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

                    _tcs.TrySetResult(Task.FromException<(long, object)>(e));
                }

                return 1;
            }

            private void InsertCommandToLeaderLog(ClusterOperationContext context)
            {
                _engine.GetLastCommitIndex(context, out var lastCommitted, out _);
                if (_leader._running.IsRaised() == false) // not longer leader
                {
                    _tcs.TrySetResult(Task.FromException<(long, object)>(new NotLeadingException(_leaderDisposedMessage)));
                    return;
                }

                if (_engine.LogHistory.HasHistoryLog(context, Command.UniqueRequestId, out var index, out var result, out var exception))
                {
                    // if this command is already committed, we can skip it and notify the caller about it
                    if (lastCommitted >= index)
                    {
                        if (exception != null)
                        {
                            _tcs.TrySetResult(Task.FromException<(long, object)>(exception));
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

                            _tcs.TrySetResult(Task.FromResult<(long, object)>((index, result)));
                        }
                        return;
                    }
                }
                else
                {
                    _engine.InvokeBeforeAppendToRaftLog(context, this);
                    var term = _leader.Term;
                    if (_engine.ForTestingPurposes?.ModifyTermBeforeRachisMergedCommandInsertToLeaderLog != null)
                        term = _engine.ForTestingPurposes.ModifyTermBeforeRachisMergedCommandInsertToLeaderLog.Invoke(Command, term);

                    index = _engine.InsertToLeaderLog(context, term, Command.Raw, RachisEntryFlags.StateMachineCommand);
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

                _tcs.TrySetResult(state.TaskCompletionSource.Task);

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

            public void Dispose()
            {
                Command.Raw?.Dispose();
                Command.Raw = null;
                BlittableResultWriter?.Dispose();
                _ctxReturn?.Dispose();
            }
        }
    }
}
