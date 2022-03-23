using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Batches;

public class DatabaseBatchCommandsReader : AbstractBatchCommandsReader<MergedBatchCommand, DocumentsOperationContext>
{
    private readonly DocumentDatabase _database;
    public List<MergedBatchCommand.AttachmentStream> AttachmentStreams;
    public StreamsTempFile AttachmentStreamsTempFile;

    public DatabaseBatchCommandsReader(RequestHandler handler, DocumentDatabase database) : base(handler, database.Name, database.IdentityPartsSeparator, BatchRequestParser.Instance)
    {
        _database = database;
    }

    public override async Task SaveStream(JsonOperationContext context, Stream input)
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
        attachmentStream.Hash = await AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(context, input, attachmentStream.Stream, _database.DatabaseShutdown);
        await attachmentStream.Stream.FlushAsync();
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
