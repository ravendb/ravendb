using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Batches;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Handlers.BulkInsert;

public class BatchCommandStreamCopier : AbstractBatchCommandParsingObserver
{
#if DEBUG
    private static bool ValidateCommands = false;
#endif

    private const int UninitializedPosition = -1;

    protected int CommandStartBufferPosition = UninitializedPosition;
    protected Stream CommandStream;

    public override void OnCommandStart(UnmanagedJsonParser parser)
    {
        CommandStartBufferPosition = parser.BufferOffset - 1;
    }

    public override void OnCommandEnd(UnmanagedJsonParser parser)
    {
        parser.CopyParsedChunk(CommandStream, CommandStartBufferPosition);

        CommandStartBufferPosition = UninitializedPosition;
    }

    public override void OnParserBufferRefill(UnmanagedJsonParser parser)
    {
        if (CommandStartBufferPosition == UninitializedPosition)
            return;

        parser.CopyParsedChunk(CommandStream, CommandStartBufferPosition);

        CommandStartBufferPosition = 0;
    }

    public override void OnId(UnmanagedJsonParser parser, int idLength, bool isNull)
    {
    }

    public override void OnIdsStart(UnmanagedJsonParser parser)
    {
    }

    public override void OnIdsEnd(UnmanagedJsonParser parser)
    {
    }

    public override void OnNullChangeVector(UnmanagedJsonParser parser)
    {

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

            var batchRequestParser = new BatchRequestParser();

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (ctx.AcquireParserState(out var state))                
            using (var parser = new UnmanagedJsonParser(ctx, state, "copied-bulk-insert-command-validation"))
            using (ctx.GetMemoryBuffer(out var buffer))
            using (var modifier = new BlittableMetadataModifier(ctx))
            {
                while (parser.Read() == false)
                    await batchRequestParser.RefillParserBuffer(stream, buffer, parser, CancellationToken.None);

                try
                {
                    await BatchRequestParser.Instance.ReadSingleCommand(ctx, stream, state, parser, buffer, modifier, CancellationToken.None);
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

    public IDisposable UseStream(MemoryStream stream)
    {
        CommandStream = stream;

        return new StreamScope(this);
    }

    private readonly struct StreamScope : IDisposable
    {
        private readonly BatchCommandStreamCopier _parent;

        public StreamScope(BatchCommandStreamCopier parent)
        {
            _parent = parent;
        }

        public void Dispose()
        {
            _parent.CommandStream = null;
        }
    }
}
