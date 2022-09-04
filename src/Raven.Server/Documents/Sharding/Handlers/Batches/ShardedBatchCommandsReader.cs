using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.ETL.Providers.SQL.Test;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.TransactionCommands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Handlers.Batches;

public class ShardedBatchCommandsReader : AbstractBatchCommandsReader<ShardedBatchCommand, TransactionOperationContext>
{
    public List<Stream> Streams;
    public List<BufferedCommand> BufferedCommands = new();

    private readonly ShardedDatabaseContext _databaseContext;
    private readonly BufferedCommandCopier _bufferedCommandCopier;

    public ShardedBatchCommandsReader(ShardedDatabaseRequestHandler handler) :
            base(handler, handler.DatabaseContext.DatabaseName, handler.DatabaseContext.IdentityPartsSeparator, new BatchRequestParser())
    {
        _databaseContext = handler.DatabaseContext;
        BatchRequestParser.CommandParsingObserver = _bufferedCommandCopier = new BufferedCommandCopier();
    }

    public override async ValueTask SaveStreamAsync(JsonOperationContext context, Stream input)
    {
        Streams ??= new List<Stream>();
        var attachment = ServerStore.GetTempFile($"{_databaseContext.DatabaseName}.attachment", "sharded", _databaseContext.Encrypted).StartNewStream();
        await input.CopyToAsync(attachment, Handler.AbortRequestToken);
        await attachment.FlushAsync(Handler.AbortRequestToken);
        Streams.Add(attachment);
    }

    public override async Task<BatchRequestParser.CommandData> ReadCommandAsync(
        JsonOperationContext ctx,
        Stream stream, JsonParserState state,
        UnmanagedJsonParser parser,
        JsonOperationContext.MemoryBuffer buffer,
        BlittableMetadataModifier modifier,
        CancellationToken token)
    {
        var ms = new MemoryStream();
        try
        {
            var bufferedCommand = new BufferedCommand { CommandStream = ms };

            BatchRequestParser.CommandData result;

            using (_bufferedCommandCopier.UseCommand(bufferedCommand))
            {
                result = await BatchRequestParser.ReadSingleCommand(ctx, stream, state, parser, buffer, modifier, token);
                ValidateSupportedCommand(result);
            }

            bufferedCommand.IsIdentity = IsIdentityCommand(ref result);
            bufferedCommand.IsServerSideIdentity = bufferedCommand.IsIdentity == false && IsServerSideIdentityCommand(ref result, _databaseContext.IdentityPartsSeparator);
            bufferedCommand.IsEmptyId = result.Id == string.Empty;

            BufferedCommands.Add(bufferedCommand);

            return result;
        }
        catch
        {
            await ms.DisposeAsync();
            throw;
        }
    }

    private static void ValidateSupportedCommand(BatchRequestParser.CommandData command)
    {
        switch (command.Type)
        {
            case CommandType.TimeSeriesCopy:
            case CommandType.AttachmentMOVE:
            case CommandType.AttachmentCOPY:
                throw new NotSupportedInShardingException($"Command type {command.Type} for ID {command.Id} is not supported");
        }
    }

    public override async ValueTask<ShardedBatchCommand> GetCommandAsync(TransactionOperationContext context)
    {
        await ExecuteGetIdentitiesAsync();

        return new ShardedBatchCommand(context, _databaseContext)
        {
            ParsedCommands = Commands,
            BufferedCommands = BufferedCommands,
            AttachmentStreams = Streams,
            IsClusterTransaction = IsClusterTransactionRequest
        };
    }
}
