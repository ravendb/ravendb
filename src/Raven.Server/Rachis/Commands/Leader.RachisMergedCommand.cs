using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using System.Diagnostics.CodeAnalysis;
using Voron.Impl;

namespace Raven.Server.Rachis
{
    public partial class Leader
    {
        internal class RachisMergedCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
        {
            public CommandBase Command;
            public Task<(long, object)> TaskResult { get; private set; }
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
                var lostLeadershipException = new NotLeadingException(_leaderDisposedMessage);

                try
                {
                    _engine.GetLastCommitIndex(context, out var lastCommitted, out _);
                    if (_leader._running.IsRaised() == false) // not longer leader
                    {
                        // throw lostLeadershipException;
                        TaskResult = Task.FromException<(long, object)>(lostLeadershipException);
                        return 1;
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
                                    result = GetConvertResult(Command)?.Apply(result) ?? Command.FromRemote(result);
                                }

                                TaskResult = Task.FromResult<(long, object)>((index, result));
                            }
                            return 1;
                        }

                    }

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
                _engine.InvokeBeforeAppendToRaftLog(context, Command);
                var djv = Command.ToJson(context);
                var commandJson = context.ReadObject(djv, "raft/command");

                long index = _engine.InsertToLeaderLog(context, _leader.Term, commandJson, RachisEntryFlags.StateMachineCommand);
                if (_leader._entries.TryGetValue(index, out var state))
                {
                    TaskResult = state.TaskCompletionSource.Task;
                }
                else
                {
                    var tcs = new TaskCompletionSource<(long, object)>(TaskCreationOptions.RunContinuationsAsynchronously);
                    TaskResult = tcs.Task; //will set only after leader gets consensus on this command and finish with execute 'Command'.
                    var newState = new CommandState
                    {
                        // we need to add entry inside write tx lock to avoid
                        // a situation when command will be applied (and state set)
                        // before it is added to the entries list

                        CommandIndex = index,
                        TaskCompletionSource = tcs,
                        ConvertResult = GetConvertResult(Command),
                    };
                    _leader._entries[index] = newState;
                    context.Transaction.InnerTransaction.LowLevelTransaction.AfterCommitWhenNewTransactionsPrevented += AfterCommit;
                }
            }

            private void AfterCommit(LowLevelTransaction tx)
            {
                try
                {
                    _leader._newEntry.Set();
                }
                catch (ObjectDisposedException ex)
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
