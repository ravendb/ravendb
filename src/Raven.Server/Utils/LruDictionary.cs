using System;
using System.Runtime.InteropServices;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Utils;

using System.Collections.Generic;

public sealed class LruDictionary<TKey, TValue> 
    where TKey : notnull, IComparable
{
    private readonly int _maxCapacity;
    private readonly Dictionary<TKey, (LinkedListNode<TKey> Node, TValue Value)> _cache;
    private readonly LinkedList<TKey> _list;

    private readonly Dictionary<TKey, TValue> _toRelease;
    private readonly LruCacheHelpers.ICacheReleaser<TValue> _releaseValue;
    private readonly int _toReleaseMaxCapacity;
    
    public LruDictionary(int maxCapacity, LruCacheHelpers.ICacheReleaser<TValue> releaseValue = null)
    {
        _maxCapacity = maxCapacity;  
        _cache = new Dictionary<TKey, (LinkedListNode<TKey> Node, TValue Value)>(maxCapacity);
        _list = new LinkedList<TKey>();

        if (releaseValue is not null)
        {
            _toReleaseMaxCapacity = Math.Max(_maxCapacity / 32, 4);
            _toRelease = new(_toReleaseMaxCapacity);
            _releaseValue = releaseValue;
        }
    }
    
    private void ReleaseRemovedItems()
    {
        //Let's release documents in batches.
        foreach (var doc in _toRelease)
        {
            _releaseValue!.ReleaseItem(doc.Value);
        }
        _toRelease.Clear();
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

    public TValue this[TKey key]
    {
        get
        {
            TryGetValue(key, out var value);
            return value;
        }

        set
        {
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
                    
                    //We've to add last document to release list and ensure that currently added document is not persisted 
                    // in release list (since this can be promotion from release list to cache list again).
                    // In such case we've remove it from toRelease list. 
                    if (_toRelease != null)
                    {
                        _toRelease.Add(removeKey, _cache[removeKey].Value);
                        _toRelease.Remove(key);
                    }

                    _cache.Remove(removeKey);
                    _list.RemoveLast();

                    if (_toRelease != null && _toRelease.Count < _toReleaseMaxCapacity)
                        ReleaseRemovedItems();
                }

                // add cache
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
    
    public class DocumentReleaser : ICacheReleaser<Document>
    {
        private readonly DocumentsOperationContext _context;

        public DocumentReleaser(DocumentsOperationContext context)
        {
            _context = context;
        }
        
        public void ReleaseItem(Document value)
        {
            _context.Transaction.ForgetAbout(value);
            value.IgnoreDispose = false;
            value.Dispose();
        }
    }
    
}


