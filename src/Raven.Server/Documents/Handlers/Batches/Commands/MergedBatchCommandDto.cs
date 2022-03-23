using System.Collections.Generic;
using Raven.Client.Documents.Commands.Batches;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Batches.Commands;

public class MergedBatchCommandDto : TransactionOperationsMerger.IReplayableCommandDto<MergedBatchCommand>
{
    public BatchRequestParser.CommandData[] ParsedCommands { get; set; }
    public List<MergedBatchCommand.AttachmentStream> AttachmentStreams;

    public MergedBatchCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
    {
        for (var i = 0; i < ParsedCommands.Length; i++)
        {
            if (ParsedCommands[i].Type != CommandType.PATCH)
            {
                continue;
            }

            ParsedCommands[i].PatchCommand = new PatchDocumentCommand(
                context: context,
                id: ParsedCommands[i].Id,
                expectedChangeVector: ParsedCommands[i].ChangeVector,
                skipPatchIfChangeVectorMismatch: false,
                patch: (ParsedCommands[i].Patch, ParsedCommands[i].PatchArgs),
                patchIfMissing: (ParsedCommands[i].PatchIfMissing, ParsedCommands[i].PatchIfMissingArgs),
                identityPartsSeparator: database.IdentityPartsSeparator,
                createIfMissing: ParsedCommands[i].CreateIfMissing,
                isTest: false,
                debugMode: false,
                collectResultsNeeded: true,
                returnDocument: ParsedCommands[i].ReturnDocument
            );
        }

        var newCmd = new MergedBatchCommand(database)
        {
            ParsedCommands = ParsedCommands,
            AttachmentStreams = AttachmentStreams
        };

        return newCmd;
    }
}
