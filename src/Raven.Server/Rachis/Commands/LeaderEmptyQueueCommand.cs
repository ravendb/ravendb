using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Rachis.Commands;

internal class LeaderEmptyQueueCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly RachisConsensus _engine;
    private readonly Leader _leader;
    private readonly List<(Leader.RachisMergedCommand Command, BlittableJsonReaderObject CommandJson)> _commandsToProcess;

    public List<Task<(long, object)>> Tasks { get; private set; }

    public LeaderEmptyQueueCommand([NotNull] RachisConsensus engine, [NotNull] Leader leader, [NotNull] List<(Leader.RachisMergedCommand Command, BlittableJsonReaderObject CommandJson)> commandsToProcess)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _leader = leader ?? throw new ArgumentNullException(nameof(leader));
        _commandsToProcess = commandsToProcess ?? throw new ArgumentNullException(nameof(commandsToProcess));
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        Tasks = new List<Task<(long, object)>>();

        _engine.GetLastCommitIndex(context, out var lastCommitted, out _);

        foreach (var value in _commandsToProcess)
        {
            var command = value.Command;
            var commandJson = value.CommandJson;

            if (_engine.LogHistory.HasHistoryLog(context, command.Command.UniqueRequestId, out var index, out var result, out var exception))
            {
                var tcs = new TaskCompletionSource<(long, object)>(TaskCreationOptions.RunContinuationsAsynchronously);
                Tasks.Add(tcs.Task);

                // if this command is already committed, we can skip it and notify the caller about it
                if (lastCommitted >= index)
                {
                    if (exception != null)
                    {
                        tcs.TrySetException(exception);
                    }
                    else
                    {
                        if (result != null)
                            result = Leader.GetConvertResult(command.Command)?.Apply(result) ?? command.Command.FromRemote(result);

                        tcs.TrySetResult((index, result));
                    }

                    continue;
                }
            }
            else
            {
                index = _engine.InsertToLeaderLog(context, _leader.Term, commandJson, RachisEntryFlags.StateMachineCommand);
            }

            if (_leader._entries.TryGetValue(index, out var state))
            {
                Tasks.Add(state.TaskCompletionSource.Task);
            }
            else
            {
                var tcs = new TaskCompletionSource<(long, object)>(TaskCreationOptions.RunContinuationsAsynchronously);
                Tasks.Add(tcs.Task);
                state = new Leader.CommandState
                // we need to add entry inside write tx lock to avoid
                // a situation when command will be applied (and state set)
                // before it is added to the entries list
                {
                    CommandIndex = index,
                    TaskCompletionSource = tcs,
                    ConvertResult = Leader.GetConvertResult(command.Command),
                };
                _leader._entries[index] = state;
            }
        }

        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(JsonOperationContext context)
    {
        throw new NotImplementedException();
    }
}
