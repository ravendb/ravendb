using System;

namespace Raven.Server.Documents.Sharding;

public class ShardedDocumentsStorage : DocumentsStorage
{
    public ShardedDocumentsStorage(DocumentDatabase documentDatabase, Action<string> addToInitLog) 
        : base(documentDatabase, addToInitLog)
    {
    }

    protected override DocumentPutAction CreateDocumentPutAction()
    {
        return new ShardedDocumentPutAction(this, DocumentDatabase);
    }
}
