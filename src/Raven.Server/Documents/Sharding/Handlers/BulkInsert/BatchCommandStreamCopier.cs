using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Batches;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Handlers.BulkInsert;

public class BatchCommandStreamCopier : AbstractBatchCommandParsingObserver, IDisposable
{
#if DEBUG
    private static bool ValidateCommands = false;
#endif

    private const int UninitializedPosition = -1;

    private readonly JsonOperationContext _ctx;
    private int _commandStartBufferPosition = UninitializedPosition;
    public readonly MemoryStream CommandStream;


    public BatchCommandStreamCopier(JsonOperationContext ctx)
    {
        _ctx = ctx;
        CommandStream = ctx.CheckoutMemoryStream();
    }

    public override void OnCommandStart(UnmanagedJsonParser parser)
    {
        _commandStartBufferPosition = parser.BufferOffset - 1;
    }

    public override void OnCommandEnd(UnmanagedJsonParser parser)
    {
        parser.CopyParsedChunk(CommandStream, _commandStartBufferPosition);

        _commandStartBufferPosition = UninitializedPosition;
    }

    public override void OnParserBufferRefill(UnmanagedJsonParser parser)
    {
        if (_commandStartBufferPosition == UninitializedPosition)
            return;

        parser.CopyParsedChunk(CommandStream, _commandStartBufferPosition);

        _commandStartBufferPosition = 0;
    }

    public async Task CopyToAsync(MemoryStream stream)
    {
        CommandStream.Position = 0;

        await CommandStream.CopyToAsync(stream);

        CommandStream.Position = 0;
        CommandStream.SetLength(0);

#if DEBUG
        if (ValidateCommands)
        {
            stream.Position = 0;

            var state = new JsonParserState();
            var batchRequestParser = new BatchRequestParser();

            using (var parser = new UnmanagedJsonParser(_ctx, state, "copied-bulk-insert-command-validation"))
            using (_ctx.GetMemoryBuffer(out var buffer))
            using (var modifier = new BlittableMetadataModifier(_ctx))
            {
                while (parser.Read() == false)
                    await batchRequestParser.RefillParserBuffer(stream, buffer, parser, CancellationToken.None);

                try
                {
                    await BatchRequestParser.Instance.ReadSingleCommand(_ctx, stream, state, parser, buffer, modifier, CancellationToken.None);
                    stream.Position = 0;
                }
                catch (Exception e)
                {
                    stream.Position = 0;

                    using (var sr = new StreamReader(stream))
                    {
                        var json = await sr.ReadToEndAsync();

                        throw new InvalidOperationException($"Invalid batch command - json: {json}", e);
                    }
                }
            }
        }
#endif
    }

    public void Dispose()
    {
        _ctx.ReturnMemoryStream(CommandStream);
    }
}
