using System;
using Raven.Server.ServerWide.Context;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Raven.Server.Documents;
using Raven.Server.Documents.Queries;

namespace Raven.Server.Utils;

public class LruDictionary<TKey, TValue> 
    where TKey : notnull, IComparable
{
    protected readonly int MaxCapacity;
    protected readonly Dictionary<TKey, (LinkedListNode<TKey> Node, TValue Value)> Cache;
    protected readonly LinkedList<TKey> List;

    public LruDictionary(int maxCapacity)
    {
        MaxCapacity = maxCapacity;  
        Cache = new Dictionary<TKey, (LinkedListNode<TKey> Node, TValue Value)>(maxCapacity);
        List = new LinkedList<TKey>();
    }
    
    public bool TryGetValue(TKey key, out TValue value)
    {
        if (Cache.TryGetValue(key, out var node) == false)
        {
            value = default;
            return false;
        }

        List.Remove(node.Node);
        List.AddFirst(node.Node);

        value = node.Value;
        return true;
    }
    
    public virtual void Clear()
    {
    }

    public virtual bool IsTrackingSupported => false;

    public virtual void TrackReferences(TValue parent, TValue queriedDocument)
    {
        throw new NotSupportedException($"{nameof(LruDictionary<TKey, TValue>)} doesn't support {nameof(TrackReferences)} method.");
    }
    
    public virtual void IncreaseReference(TValue value)
    {
        throw new NotSupportedException($"{nameof(LruDictionary<TKey, TValue>)} doesn't support {nameof(IncreaseReference)} method.");
    }
    
    public virtual TValue this[TKey key]
    {
        get
        {
            TryGetValue(key, out var value);
            return value;
        }
        set
        {
            
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
                    Cache.Remove(removeKey);
                    List.RemoveLast();
                }

                // add cache
                Cache.Add(key, (List.AddFirst(key), value));
            }
        }
    }
}

