using System;
using Raven.Server.Documents.Sharding.Handlers.BulkInsert;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Handlers.Batches;

public class BufferedCommandCopier : BatchCommandStreamCopier
{
    private BufferedCommand _bufferedCommand;

    public IDisposable UseCommand(BufferedCommand command)
    {
        _bufferedCommand = command;

        UseStream(command.CommandStream);

        return new StreamScope(this);
    }

    public override void OnId(UnmanagedJsonParser parser, int idLength)
    {
        _bufferedCommand.IdStartPosition = checked((int)(CommandStream.Position + parser.BufferOffset - CommandStartBufferPosition - idLength - 1));
        _bufferedCommand.IdLength = idLength;
    }

    public override void OnIdsStart(UnmanagedJsonParser parser)
    {
        _bufferedCommand.IsBatchPatch = true;
        _bufferedCommand.IdsStartPosition = checked((int)(CommandStream.Position + parser.BufferOffset - CommandStartBufferPosition));
    }

    public override void OnIdsEnd(UnmanagedJsonParser parser)
    {
        _bufferedCommand.IdsEndPosition = checked((int)(CommandStream.Position + parser.BufferOffset - CommandStartBufferPosition));
    }

    public override void OnNullChangeVector(UnmanagedJsonParser parser)
    {
        // we need this only for identities and we expect to have always null

        _bufferedCommand.ChangeVectorPosition = checked((int)(CommandStream.Position + parser.BufferOffset - CommandStartBufferPosition - 4));
    }

    private readonly struct StreamScope : IDisposable
    {
        private readonly BufferedCommandCopier _parent;

        public StreamScope(BufferedCommandCopier parent)
        {
            _parent = parent;
        }

        public void Dispose()
        {
            _parent.CommandStream = null;
            _parent._bufferedCommand = null;
        }
    }
}
