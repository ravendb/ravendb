using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.TransactionMerger.Commands;

public class DeleteDocumentsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
{
    private readonly List<string> _ids;
    private readonly DocumentDatabase _database;

    public DeleteDocumentsCommand(List<string> ids, DocumentDatabase database)
    {
        _ids = ids;
        _database = database;
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        foreach (string id in _ids)
        {
            _database.DocumentsStorage.Delete(context, id, null);
        }

        return _ids.Count;
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
    {
        return new DeleteDocumentsCommandDto
        {
            Ids = _ids
        };
    }

    public class DeleteDocumentsCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DeleteDocumentsCommand>
    {
        public List<string> Ids { get; set; }

        public DeleteDocumentsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new DeleteDocumentsCommand(Ids, database);
        }
    }
}
