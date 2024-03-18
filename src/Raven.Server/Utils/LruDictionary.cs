using System;
using Raven.Server.ServerWide.Context;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Server.Documents;
using Raven.Server.Documents.Queries;

namespace Raven.Server.Utils;

public sealed class LruDictionary<TKey, TValue> 
    where TKey : notnull, IComparable
{
    private readonly int _maxCapacity;
    private readonly Dictionary<TKey, (LinkedListNode<TKey> Node, TValue Value)> _cache;
    private readonly LinkedList<TKey> _list;

    private readonly LruCacheHelpers.ICacheReleaser<TValue> _releaser;
    public LruDictionary(int maxCapacity, LruCacheHelpers.ICacheReleaser<TValue> releaser = null)
    {
        _maxCapacity = maxCapacity;  
        _cache = new Dictionary<TKey, (LinkedListNode<TKey> Node, TValue Value)>(maxCapacity);
        _list = new LinkedList<TKey>();
        _releaser = releaser;
    }
    
    public bool TryGetValue(TKey key, out TValue value)
    {
        if (_cache.TryGetValue(key, out var node) == false)
        {
            value = default;
            return false;
        }

        _list.Remove(node.Node);
        _list.AddFirst(node.Node);

        value = node.Value;
        return true;
    }
    
    public void Clear()
    {
        foreach (var (_, valueTuple) in _cache)
        {
            if (valueTuple.Value is null)
                continue;
            
#if DEBUG
            if (typeof(TValue) == typeof(QueriedDocument))
                Debug.Assert(((QueriedDocument)(object)valueTuple.Value).CanDispose);
#endif

            _releaser?.ReleaseItem(valueTuple.Value);
            _cache.Clear();
            _list.Clear();
        }
    }
    
    public TValue this[TKey key]
    {
        get
        {
            TryGetValue(key, out var value);
            Debug.Assert(value is not Document || value is Document {IsDisposed: false});
            return value;
        }

        set
        {
            Debug.Assert(value is not QueriedDocument queriedDocument || queriedDocument.StorageId != Voron.Global.Constants.Compression.NonReturnableStorageId );
            
            if (_cache.TryGetValue(key, out var node))
            {
                _list.Remove(node.Node);
                _list.AddFirst(node.Node);

                _cache[key] = (node.Node, value);
            }
            else
            {
                if (_cache.Count >= _maxCapacity)
                {
                    var removeKey = _list.Last!.Value;
                    if (_cache.TryGetValue(removeKey, out var toRemove) && typeof(TValue) == typeof(QueriedDocument) && _releaser != null && toRemove.Value != null)
                    {
                        var element = (QueriedDocument)(object)(toRemove.Value);
                        if (element is {CanDispose: false})
                            goto AddOnly;
                        
                        _releaser?.ReleaseItem(toRemove.Value);
                    }
                    
                    _cache.Remove(removeKey);
                    _list.RemoveLast();
                }

                AddOnly:
                _cache.Add(key, (_list.AddFirst(key), value));
            }
        }
    }
}

public static class LruCacheHelpers
{ 
    public interface ICacheReleaser<in TValue>
    {
        void ReleaseItem(TValue value);
    }
    
    public class DocumentReleaser : ICacheReleaser<QueriedDocument>
    {
        private readonly DocumentsOperationContext _context;

        public DocumentReleaser(DocumentsOperationContext context)
        {
            _context = context;
        }
        
        public void ReleaseItem(QueriedDocument value)
        {
            _context.Transaction.ForgetAbout(value);
            value.Dispose();
        }
    }
    
}


