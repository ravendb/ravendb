using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries;

public sealed class QueriedDocument : Document
{
    private static readonly QueriedDocument ExplicitNullReference = new();
    private int _refCount = 1;
    public int RefCount => _refCount;

    // Cloned document is not stored in LRU, that means disposing this object should only decrease reference at original document. 
    private QueriedDocument _sourceDocument;

    // This is used by document that were loaded additionaly to our document from query.
    private QueriedDocument _referencingSingle;
    private List<QueriedDocument> _referencing;

    public bool CanDispose => _refCount == 1;
    public bool IsCloned;

    public void LinkReferencedDocument(QueriedDocument additionalDocument)
    {
        // loaded document doesn't exists
        if (additionalDocument is null)
            return;

        // Since most of queries loads only one document
        // e.g. load originalDoc.ReferenceId as ref
        // We can avoid creating a list object for single reference and store it in variable.
        if (_referencingSingle == null)
        {
            _referencingSingle = additionalDocument;
        }
        else if (ReferenceEquals(_referencingSingle, ExplicitNullReference) == false)
        {
            _referencing ??= new();
            _referencing.Add(_referencingSingle);
            // ExplicitNullReference means we've multiple references.
            _referencingSingle = ExplicitNullReference;
        }
        else
        {
            _referencing.Add(additionalDocument);
        }

        additionalDocument.IncreaseReference();
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

        cloned.IsCloned = true;
        cloned._sourceDocument = this;

        //We cloned document for projection purposes. That indicates that all loaded references are related to the copy.
        cloned._referencing = _referencing;
        _referencing = null;
        cloned._referencingSingle = _referencingSingle;
        _referencingSingle = null;

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
            Debug.Assert(IsCloned);
            Debug.Assert(_refCount == 0, "We should not have reference copy to clone.");
            //Decrease reference at parent document.
            Debug.Assert(_sourceDocument._refCount >= 2);
            Debug.Assert(_sourceDocument.Data != null);

            Debug.Assert(ReferenceEquals(_sourceDocument.Data, Data) == false);
            _sourceDocument.Dispose();
            Debug.Assert(_sourceDocument.Data != null);

            // We can dispose this document since it's owns a copy of the data.
            base.Dispose();
            Debug.Assert(_sourceDocument.Data != null);

            if (_referencingSingle != null)
            {
                if (ReferenceEquals(_referencingSingle, ExplicitNullReference) == false)
                {
                    _referencingSingle.Dispose();
                    _referencingSingle = null;
                }
                else
                {
                    Debug.Assert(_referencing != null);
                    foreach (var reference in _referencing)
                    {
                        reference.Dispose();
                    }

                    _referencing.Clear();
                }
            }

            return;
        }

        Debug.Assert(IsCloned == false);
        // Disposing item must happened when document is only referenced in LRU.
        if (_refCount >= 1)
            return;

        base.Dispose();
    }
}
