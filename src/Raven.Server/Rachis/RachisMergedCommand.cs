using System;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Threading;

namespace Raven.Server.Rachis;

public class RachisMergedCommand : IDisposable
{
    private readonly ClusterContextPool _pool;
    private IDisposable _ctxReturn;

    public CommandBase Command;
    public TaskCompletionSource<Task<(long Index, object Result)>> Tcs = new TaskCompletionSource<Task<(long Index, object Result)>>(TaskCreationOptions.RunContinuationsAsynchronously);
    public readonly MultipleUseFlag Consumed = new MultipleUseFlag();
    public BlittableResultWriter BlittableResultWriter { get; private set; }
    public BlittableJsonReaderObject Raw;
    public RachisMergedCommand(ClusterContextPool pool, CommandBase command)
    {
        _pool = pool;
        Command = command;
    }

    public void Initialize()
    {
        BlittableResultWriter = Command is IBlittableResultCommand crCommand ? new BlittableResultWriter(crCommand.WriteResult) : null;

        // prepare the command outside the write lock.
        _ctxReturn = _pool.AllocateOperationContext(out JsonOperationContext context);
        var djv = Command.ToJson(context);
        Raw = context.ReadObject(djv, "prepare-raw-command");
    }

    public async Task<(long Index, object Result)> Result()
    {
        var inner = await Tcs.Task;
        var r = await inner;
        return BlittableResultWriter == null ? r : (r.Index, BlittableResultWriter.Result);
    } 

    public void Dispose()
    {
        Raw?.Dispose();
        Raw = null;
        BlittableResultWriter?.Dispose();
        _ctxReturn?.Dispose();
    }
}
