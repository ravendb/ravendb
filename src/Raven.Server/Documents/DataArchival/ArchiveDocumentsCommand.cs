using System;
using System.Collections.Generic;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.DataArchival;

internal class ArchiveDocumentsCommand(Dictionary<Slice, List<(Slice LowerId, string Id)>> toArchive, DocumentDatabase database, DateTime currentTime)
    : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
{
    public int ArchivedDocsCount;

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        ArchivedDocsCount = database.DocumentsStorage.DataArchivalStorage.ProcessDocuments(context, toArchive, currentTime);
        return ArchivedDocsCount;
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, ArchiveDocumentsCommand> ToDto(DocumentsOperationContext context)
    {
        var keyValuePairs = new KeyValuePair<Slice, List<(Slice LowerId, string Id)>>[toArchive.Count];
        var i = 0;
        foreach (var item in toArchive)
        {
            keyValuePairs[i] = item;
            i++;
        }

        return new ArchiveDocumentsCommandDto 
        {
            ToArchive = keyValuePairs,
            CurrentTime = currentTime
        };
    }
}


internal class ArchiveDocumentsCommandDto: IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, ArchiveDocumentsCommand>
{
    public ArchiveDocumentsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
    {
        var toArchive = new Dictionary<Slice, List<(Slice LowerId, string Id)>>();
        foreach (var item in ToArchive)
        {
            toArchive[item.Key] = item.Value;
        }
        var command = new ArchiveDocumentsCommand(toArchive, database, CurrentTime);
        return command;
    }

    public KeyValuePair<Slice, List<(Slice LowerId, string Id)>>[] ToArchive { get; set; }

    public DateTime CurrentTime { get; set; }
}
