using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries;

public class QueriedDocument : Document
{
    private int _refCount = 1;

    // For example, Map Reduce output is projected from index storage and is not persisted within the LRU due to entry uniqueness.
    public bool NotPersistedInCache;
    
    // Cloned document is not stored in LRU, that means disposing this object should only decrease reference at original document. 
    private QueriedDocument _sourceDocument;

    // This is used for document(s) that were loaded additionally to our document from query.
    private bool _useList;
    private QueriedDocument _referencingSingle;
    private List<QueriedDocument> _referencing;

    public bool CanDispose => _refCount == 1;
    
#if DEBUG
    private bool _isCloned;
    internal int RefCount => _refCount;
#endif

    public void LinkReferencedDocument(QueriedDocument referencedDocument)
    {
        // loaded document doesn't exists
        if (referencedDocument is null)
            return;

        //Cycle reference doesn't make sense.
        if (ReferenceEquals(this, referencedDocument))
            return;

        switch (_useList)
        {
            // Since most of queries loads only one document
            // e.g. load originalDoc.ReferenceId as ref
            // We can avoid creating a list object for single reference and store it in variable.
            case false:
                _referencingSingle = referencedDocument;
                _useList = true;
                break;
            
            case true when _referencingSingle != null:
                _referencing ??= new();
                _referencing.Add(_referencingSingle);
                _referencingSingle = null;
                break;
            
            default:
                _referencing.Add(referencedDocument);
                break;
        }

        referencedDocument.IncreaseReference();
    }

    public override TDocument Clone<TDocument>(JsonOperationContext context)
    {
        var data = Data.Clone(context);
        return CloneWith<TDocument>(context, data);
    }


    public override TDocument CloneWith<TDocument>(JsonOperationContext context, BlittableJsonReaderObject newData)
    {
        if (typeof(TDocument) != typeof(QueriedDocument))
            return base.Clone<TDocument>(context);

        IncreaseReference();

        var cloned = (QueriedDocument)(object)base.CloneWith<TDocument>(context, newData);

        if (this.Data is null)
            return (TDocument)(object)cloned;
        
#if DEBUG
        cloned._isCloned = true;
#endif
        cloned._sourceDocument = this;

        //We cloned document for projection purposes. That indicates that all loaded references are related to the copy.
        cloned._referencing = _referencing;
        _referencing = null;
        
        cloned._referencingSingle = _referencingSingle;
        _referencingSingle = null;
        
        cloned._useList = _useList;
        _useList = false;
        cloned.NotPersistedInCache = NotPersistedInCache;
        
        return (TDocument)(object)cloned;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void IncreaseReference() => _refCount++;

    public override void Dispose()
    {
        _refCount--;
        Debug.Assert(_refCount >= 0, "This document is already disposed, however we kept reference in other document. This should not be possible.");

        if (_sourceDocument != null)
        {
#if DEBUG
            Debug.Assert(_isCloned, "IsCloned");
            Debug.Assert(_refCount == 0, $"We should not have reference copy to clone. {_refCount}");
            Debug.Assert(NotPersistedInCache || _sourceDocument._refCount >= 2, $"_sourceDocument._refCount ({_sourceDocument._refCount}) >= 2");
            Debug.Assert(_sourceDocument.Data != null, "_sourceDocument.Data != null (0)");

            Debug.Assert(ReferenceEquals(_sourceDocument.Data, Data) == false);
#endif            
            //Decrease reference at parent document.
            _sourceDocument.Dispose();
            
            Debug.Assert(NotPersistedInCache || _sourceDocument.Data != null, "_sourceDocument.Data != null (1)");
            // We can dispose this document since it's owns a copy of the data.
            base.Dispose();
            Debug.Assert(NotPersistedInCache || _sourceDocument.Data != null, "_sourceDocument.Data != null (2)");
            
            if (_referencingSingle != null || _useList)
            {
                if (_referencingSingle != null)
                {
                    _referencingSingle.Dispose();
                    _referencingSingle = null;
                }
                else
                {
                    Debug.Assert(_referencing != null, "_referencing != null");
                    foreach (var reference in _referencing)
                    {
                        reference.Dispose();
                    }

                    _referencing.Clear();
                }
            }

            return;
        }
#if DEBUG
        Debug.Assert(_isCloned == false, "IsCloned == false");
#endif        
        
        // Disposing item must happened when document is only referenced in LRU.
        if (_refCount >= 1)
            return;

        base.Dispose();
    }
}
