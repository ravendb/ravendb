using System.Collections.Generic;
using Voron;

namespace Corax.Utils;

public class Trie
{
    //The idea of having a Trie tree from startsWith is that it is a convenient way to have information about which prefixes
    //have already been returned. Additionally, knowing that Seek from CompactTree is sorted very we can quickly
    //limits the sets that need to be returned.
    
    
    //Please notice this is prototype class. Needs to be optimized before release.
    
    
    private Node _root;
    
    public Trie()
    {
        _root = new();
    }

    public void Add(Slice key, bool value)
    {
        var lastChildren = _root;
        for (int i = 0; i < key.Size; ++i)
        {
            if (key[i] == 0)
                continue;
            
            if (lastChildren._children.TryGetValue(key[i], out var children) == false)
            {
                children = new Node();
                lastChildren._children.Add(key[i], children);
                lastChildren = children;
            }
        }

        lastChildren.Value = value;
    }

    public bool TryGetValue(Slice key, out bool value)
    {
        var lastChildren = _root;
        for (int i = 0; i < key.Size; ++i)
        {
            if (lastChildren._children.TryGetValue(key[i], out var children) == false)
            {
                value = false;
                return false;
            }

            if (children.Value)
            {
                value = true;
                return true;
            }
        }

        value = false;
        return true;
    }
    
    
}

public class Node
{
    public Dictionary<byte, Node> _children;
    
    public bool Value { get; set; }

    
    public Node()
    {
        _children = new();
    }
    
}
