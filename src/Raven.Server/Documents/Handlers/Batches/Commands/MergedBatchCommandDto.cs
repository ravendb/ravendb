using System.Collections.Generic;
using Raven.Client.Documents.Commands.Batches;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Batches.Commands;

public sealed class MergedBatchCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedBatchCommand>
{
    public BatchRequestParser.CommandData[] ParsedCommands { get; init; }
    public List<MergedBatchCommand.AttachmentStream> AttachmentStreams { get; init; }
    public bool IncludeReply { get; init; }

    public MergedBatchCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
    {
        foreach (var parsedCommand in ParsedCommands)
        {
            if (parsedCommand.Type != CommandType.PATCH)
                continue;

            parsedCommand.PatchCommand = new PatchDocumentCommand(
                context: context,
                id: parsedCommand.Id,
                expectedChangeVector: parsedCommand.ChangeVector,
                skipPatchIfChangeVectorMismatch: false,
                patch: (parsedCommand.Patch, parsedCommand.PatchArgs),
                patchIfMissing: (parsedCommand.PatchIfMissing, parsedCommand.PatchIfMissingArgs),
                identityPartsSeparator: database.IdentityPartsSeparator,
                createIfMissing: parsedCommand.CreateIfMissing,
                isTest: false,
                debugMode: false,
                collectResultsNeeded: true,
                returnDocument: parsedCommand.ReturnDocument
            );
        }

        var newCmd = new MergedBatchCommand(database)
        {
            IncludeReply = IncludeReply,
            ParsedCommands = ParsedCommands,
            AttachmentStreams = AttachmentStreams
        };

        return newCmd;
    }
}
