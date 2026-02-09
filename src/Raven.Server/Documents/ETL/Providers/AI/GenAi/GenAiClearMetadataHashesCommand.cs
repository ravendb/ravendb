using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi;

internal sealed class GenAiClearMetadataHashesCommand : DocumentMergedTransactionCommand
{
    private readonly List<string> _docIds;
    private readonly string _taskIdentifier;

    public GenAiClearMetadataHashesCommand(List<string> docIds, string taskIdentifier)
    {
        _docIds = docIds ?? throw new ArgumentException(nameof(docIds));
        _taskIdentifier = taskIdentifier ?? throw new ArgumentException(nameof(taskIdentifier));
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        long updated = 0;

        foreach (var id in _docIds)
        {
            if (string.IsNullOrEmpty(id))
                continue;

            if (TryRemoveTaskHashesFromMetadata(context, id, _taskIdentifier) == false)
                continue;

            updated++;
        }

        return updated;
    }

    private static bool TryRemoveTaskHashesFromMetadata(DocumentsOperationContext context, string id, string taskIdentifier)
    {
        var doc = context.DocumentDatabase.DocumentsStorage.Get(context, id);
        if (doc == null)
            return false;

        if (doc.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
            metadata == null)
            return false;

        if (metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashesSection) == false ||
            hashesSection == null)
            return false;

        // If there is no entry for this task, nothing to do.
        if (hashesSection.TryGet(taskIdentifier, out object _) == false)
            return false;

        if (hashesSection.Count == 1)
        {
            // If this is the only entry, we can remove the entire section
            metadata.Modifications = new DynamicJsonValue(metadata);
            metadata.Modifications.Remove(Constants.Documents.Metadata.GenAiHashes);
        }

        else
        {
            // Remove the task entry from @gen-ai-hashes
            hashesSection.Modifications = new DynamicJsonValue(hashesSection);
            hashesSection.Modifications.Remove(taskIdentifier);

            metadata.Modifications = new DynamicJsonValue(metadata)
            {
                [Constants.Documents.Metadata.GenAiHashes] = hashesSection
            };
        }

        doc.Data.Modifications = new DynamicJsonValue(doc.Data)
        {
            [Constants.Documents.Metadata.Key] = metadata
        };

        using (var old = doc.Data)
        {
            doc.Data = context.ReadObject(old, id);
        }

        context.DocumentDatabase.DocumentsStorage.Put(context, id, expectedChangeVector: null, doc.Data);

        return true;
    }


    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
    {
        throw new NotSupportedException($"Replay not supported for {nameof(GenAiClearMetadataHashesCommand)}");
    }
}

