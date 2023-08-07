using System.Collections.Generic;
using System.Threading;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Enumerators;
using Sparrow;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class CoraxDocumentTrainSourceEnumerator
{
    private readonly DocumentsStorage _documentsStorage;

    public CoraxDocumentTrainSourceEnumerator(DocumentsStorage documentsStorage)
    {
        _documentsStorage = documentsStorage;
    }
    
    public IEnumerable<Document> GetUniformlyDistributedDocumentsFrom(DocumentsOperationContext context, string collection, CoraxDocumentTrainSourceState state, DocumentFields fields = DocumentFields.All)
    {
        var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
        if (collectionName == null)
            yield break;

        var table = context.Transaction.InnerTransaction.OpenTable(_documentsStorage.DocsSchema, collectionName.GetTableName(CollectionTableType.Documents));
        if (table == null)
            yield break;
        
        state.InitializeState(table);
        
        foreach (var (key, result) in table.IterateUniformly(_documentsStorage.DocsSchema.FixedSizeIndexes[Schemas.Documents.CollectionEtagsSlice], state.DocumentSkip, seek: state.CurrentKey))
        {
            //Update inner key in order to seek after transaction refresh
            state.CurrentKey = key;
            
            if (state.Take <= 0)
                yield break;

            state.Token.ThrowIfCancellationRequested();
            yield return _documentsStorage.TableValueToDocument(context, ref result.Reader, fields);
        }
    }

    public IEnumerable<Document> GetUniformlyDistributedDocumentsFrom(DocumentsOperationContext context, CoraxDocumentTrainSourceState state, DocumentFields fields = DocumentFields.All)
    {
        var table = new Table(_documentsStorage.DocsSchema, context.Transaction.InnerTransaction);
        state.InitializeState(table);
        foreach (var (key, result) in table.IterateUniformly(_documentsStorage.DocsSchema.FixedSizeIndexes[Schemas.Documents.AllDocsEtagsSlice], state.DocumentSkip, state.CurrentKey))
        {
            state.CurrentKey = key;
            
            if (state.Take <= 0)
                yield break;
            
            state.Token.ThrowIfCancellationRequested();
            yield return _documentsStorage.TableValueToDocument(context, ref result.Reader, fields);
        }
    }
}

public class CoraxDocumentTrainSourceState : PulsedEnumerationState<Document>
{
    private readonly long _takeLimit;
    public long Take;
    private bool _initialized;
    public long CurrentKey = 0;
    public readonly CancellationToken Token;
    
    
    // Creates cycle when we've to return a document.
    public long DocumentSkip;
    public CoraxDocumentTrainSourceState(DocumentsOperationContext context, Size pulseLimit, long takeLimit, CancellationToken token, int numberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded = DefaultNumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded) : base(context, pulseLimit, numberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded)
    {
        _takeLimit = takeLimit;
        Token = token;
    }

    public void InitializeState(Table documentsTable)
    {
        if (_initialized)
            return;
        _initialized = true;
        DocumentSkip = documentsTable.NumberOfEntries < _takeLimit ? 1 : documentsTable.NumberOfEntries / _takeLimit;
        Take = _takeLimit;
    }
    
    
    public override void OnMoveNext(Document current)
    {
        ReadCount++;
        Take--;
    }
}
