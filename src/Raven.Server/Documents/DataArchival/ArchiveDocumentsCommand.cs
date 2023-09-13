using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.DataArchival;

internal class ArchiveDocumentsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
{
    private readonly Dictionary<Slice, List<(Slice LowerId, string Id)>> _toArchive;
    private readonly DocumentDatabase _database;
    private readonly DateTime _currentTime;

    public int ArchivedDocsCount;

    public ArchiveDocumentsCommand([NotNull] Dictionary<Slice, List<(Slice LowerId, string Id)>> toArchive, [NotNull] DocumentDatabase database, DateTime currentTime)
    {
        _toArchive = toArchive ?? throw new ArgumentNullException(nameof(toArchive));
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _currentTime = currentTime;
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        ArchivedDocsCount = _database.DocumentsStorage.DataArchivalStorage.ProcessDocuments(context, _toArchive, _currentTime);
        return ArchivedDocsCount;
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, ArchiveDocumentsCommand> ToDto(DocumentsOperationContext context)
    {
        var keyValuePairs = new KeyValuePair<Slice, List<(Slice LowerId, string Id)>>[_toArchive.Count];
        var i = 0;
        foreach (var item in _toArchive)
        {
            keyValuePairs[i] = item;
            i++;
        }

        return new ArchiveDocumentsCommandDto 
        {
            ToArchive = keyValuePairs,
            CurrentTime = _currentTime
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
