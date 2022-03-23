using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Batches.Commands;

public abstract class TransactionMergedCommand : TransactionOperationsMerger.MergedTransactionCommand, IBatchCommand
{
    protected readonly DocumentDatabase Database;

    public DynamicJsonArray Reply = new();

    public HashSet<string> ModifiedCollections { get; set; }

    public string LastChangeVector { get; set; }

    public long LastTombstoneEtag { get; set; }

    public bool IsClusterTransaction { get; set; }

    protected TransactionMergedCommand(DocumentDatabase database)
    {
        Database = database;
    }

    protected void AddPutResult(DocumentsStorage.PutOperationResults putResult)
    {
        LastChangeVector = putResult.ChangeVector;
        ModifiedCollections?.Add(putResult.Collection.Name);

        // Make sure all the metadata fields are always been add
        var putReply = new DynamicJsonValue
        {
            ["Type"] = nameof(CommandType.PUT),
            [Constants.Documents.Metadata.Id] = putResult.Id,
            [Constants.Documents.Metadata.Collection] = putResult.Collection.Name,
            [Constants.Documents.Metadata.ChangeVector] = putResult.ChangeVector,
            [Constants.Documents.Metadata.LastModified] = putResult.LastModified
        };

        if (putResult.Flags != DocumentFlags.None)
            putReply[Constants.Documents.Metadata.Flags] = putResult.Flags;

        Reply.Add(putReply);
    }

    protected void AddDeleteResult(DocumentsStorage.DeleteOperationResult? deleted, string id)
    {
        var reply = new DynamicJsonValue
        {
            [nameof(BatchRequestParser.CommandData.Id)] = id,
            [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.DELETE),
            ["Deleted"] = deleted != null
        };

        if (deleted != null)
        {
            if (deleted.Value.Collection != null)
                ModifiedCollections?.Add(deleted.Value.Collection.Name);

            LastTombstoneEtag = deleted.Value.Etag;
            reply[nameof(BatchRequestParser.CommandData.ChangeVector)] = deleted.Value.ChangeVector;
        }

        Reply.Add(reply);
    }

    protected void DeleteWithPrefix(DocumentsOperationContext context, string id)
    {
        var deleteResults = Database.DocumentsStorage.DeleteDocumentsStartingWith(context, id);

        var deleted = deleteResults.Count > 0;
        if (deleted)
        {
            LastChangeVector = deleteResults[deleteResults.Count - 1].ChangeVector;
            for (var j = 0; j < deleteResults.Count; j++)
            {
                ModifiedCollections?.Add(deleteResults[j].Collection.Name);
            }
        }

        Reply.Add(new DynamicJsonValue
        {
            [nameof(BatchRequestParser.CommandData.Id)] = id,
            [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.DELETE),
            ["Deleted"] = deleted
        });
    }

    public abstract void Dispose();
}
