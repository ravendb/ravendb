using System;
using System.IO;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Handlers.Batches;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.BulkInsert;

public class ShardedCommandData : IDisposable
{
    private readonly JsonOperationContext _ctx;

    public ShardedCommandData(BatchRequestParser.CommandData command, JsonOperationContext ctx)
    {
        _ctx = ctx;
        Data = command;
        Stream = ctx.CheckoutMemoryStream();
    }

    public MemoryStream Stream { get; }

    public BatchRequestParser.CommandData Data { get; }

    public void Dispose()
    {
        _ctx.ReturnMemoryStream(Stream);
    }
}
