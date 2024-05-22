using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Voron;
using static Raven.Server.Documents.AbstractBackgroundWorkStorage;

namespace Raven.Server.Documents.DataArchival;

internal class ArchiveDocumentsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
{
    private readonly Queue<DocumentExpirationInfo> _toArchive;
    private readonly DocumentDatabase _database;
    private readonly DateTime _currentTime;

    public int ArchivedDocsCount;

    public ArchiveDocumentsCommand([NotNull] Queue<DocumentExpirationInfo> toArchive, [NotNull] DocumentDatabase database, DateTime currentTime)
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
        return new ArchiveDocumentsCommandDto 
        {
            ToArchive = _toArchive.Select(x => (Ticks: x.Ticks, LowerId: x.LowerId, Id: x.Id)).ToArray(),
            CurrentTime = _currentTime
        };
    }
}


internal class ArchiveDocumentsCommandDto: IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, ArchiveDocumentsCommand>
{
    public ArchiveDocumentsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
    {
        var toArchive = new Queue<DocumentExpirationInfo>();
        foreach (var item in ToArchive)
        {
            toArchive.Enqueue(new DocumentExpirationInfo(item.Item1, item.Item2, item.Item3));
        }
        var command = new ArchiveDocumentsCommand(toArchive, database, CurrentTime);
        return command;
    }

    public (Slice, Slice, string)[] ToArchive { get; set; }

    public DateTime CurrentTime { get; set; }
}
