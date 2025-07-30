using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Batches;

public sealed class DatabaseBatchCommandsReader : AbstractBatchCommandsReader<MergedBatchCommand, DocumentsOperationContext>
{
    private readonly DocumentDatabase _database;
    public List<MergedBatchCommand.AttachmentStream> AttachmentStreams;
    public StreamsTempFile AttachmentStreamsTempFile;

    public DatabaseBatchCommandsReader(RequestHandler handler, DocumentDatabase database) : base(handler, database.Name, database.IdentityPartsSeparator, BatchRequestParser.Instance)
    {
        _database = database;
    }

    public override async ValueTask SaveStreamAsync(JsonOperationContext context, Stream input, CancellationToken token)
    {
        if (AttachmentStreams == null)
        {
            AttachmentStreams = new List<MergedBatchCommand.AttachmentStream>();
            AttachmentStreamsTempFile = _database.DocumentsStorage.AttachmentsStorage.GetTempFile("batch");
        }

        var attachmentStream = new MergedBatchCommand.AttachmentStream
        {
            Stream = AttachmentStreamsTempFile.StartNewStream()
        };
        attachmentStream.Hash = await AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(context, input, attachmentStream.Stream, token);
        await attachmentStream.Stream.FlushAsync(token);
        AttachmentStreams.Add(attachmentStream);
    }

    public override async ValueTask<MergedBatchCommand> GetCommandAsync(DocumentsOperationContext context)
    {
        await ExecuteGetIdentitiesAsync();

        return new MergedBatchCommand(_database)
        {
            ParsedCommands = Commands,
            AttachmentStreams = AttachmentStreams,
            AttachmentStreamsTempFile = AttachmentStreamsTempFile,
            IsClusterTransaction = IsClusterTransactionRequest
        };
    }
}
