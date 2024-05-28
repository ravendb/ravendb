using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Queries.Results;

public sealed class QueriedDocumentCache : LruDictionary<string, QueriedDocument>
{
    private readonly DocumentsOperationContext _context;

    public QueriedDocumentCache(DocumentsOperationContext context, int maxCapacity) : base(maxCapacity)
    {
        _context = context;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Release(in QueriedDocument value)
    {
        _context.Transaction.ForgetAbout(value);
        value.Dispose();
    }

    public override bool IsTrackingSupported => true;

    public override void TrackReferences(QueriedDocument parent, QueriedDocument queriedDocument, bool shouldIncreaseReferences = true)
    {
        if (shouldIncreaseReferences)
            queriedDocument?.IncreaseReference();
        parent.LinkReferencedDocument(queriedDocument);
    }

    public override void IncreaseReference(QueriedDocument value)
    {
        value?.IncreaseReference();
    }

    public override void Clear()
    {
        foreach (var (_, valueTuple) in Cache)
        {
            if (valueTuple.Value is null)
                continue;

#if DEBUG
            if (valueTuple.Value.CanDispose == false)
            {
                throw new InvalidOperationException($"Document '{valueTuple.Value.Id}' should be disposable but it still has hanging reference (RefCount: {valueTuple.Value.RefCount}). This indicates a bug.");
            }
#endif
            Release(valueTuple.Value);
        }

        Cache.Clear();
        List.Clear();
    }

    public override QueriedDocument this[string key]
    {
        get
        {
            TryGetValue(key, out var value);
            value.AssertNotDisposed();
            
            return value;
        }
        set
        {
            Debug.Assert(value?.StorageId != Voron.Global.Constants.Compression.NonReturnableStorageId, "value.StorageId != Voron.Global.Constants.Compression.NonReturnableStorageId");
            
            if (Cache.TryGetValue(key, out var node))
            {
                List.Remove(node.Node);
                List.AddFirst(node.Node);

                Cache[key] = (node.Node, value);
            }
            else
            {
                if (Cache.Count >= MaxCapacity)
                {
                    var removeKey = List.Last!.Value;
                    if (Cache.TryGetValue(removeKey, out var toRemove) && toRemove.Value != null)
                    {
                        var element = toRemove.Value;
                        if (element.CanDispose == false)
                            goto AddOnly;
                        
                        Release(toRemove.Value);
                    }
                    
                    Cache.Remove(removeKey);
                    List.RemoveLast();
                }

                AddOnly:
                Cache.Add(key, (List.AddFirst(key), value));
            }
        }
    }
}
