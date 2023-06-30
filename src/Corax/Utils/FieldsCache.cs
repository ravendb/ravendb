using System.Collections.Generic;
using System.Runtime.InteropServices;
using Sparrow.Server;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl;

namespace Corax.Utils;

public struct FieldsCache
{
    private Dictionary<Slice, long> _fieldNameToRootPage;
    private readonly ByteStringContext _allocator;
    private readonly Tree _fieldsTree;
    private Dictionary<long, string> _fieldsRootPages;

    public FieldsCache(Transaction tx, Tree fieldsTree)
    {
        _allocator = tx.Allocator;
        _fieldsTree = fieldsTree;
    }

    public long GetFieldRootPage(Slice name, Tree tree)
    {
        _fieldNameToRootPage ??= new();

        ref var fieldRootPage = ref CollectionsMarshal.GetValueRefOrAddDefault(_fieldNameToRootPage, name, out var exists);
        if (exists)
            return fieldRootPage;
            
        fieldRootPage = tree.GetLookupRootPage(name);
        if (fieldRootPage != -1)
            return fieldRootPage;

        var lookup = tree.CompactTreeFor(name);
        fieldRootPage = lookup.RootPage;
        return fieldRootPage;
    }

    
    public long GetLookupRootPage(string name)
    {
        using var _ = Slice.From(_allocator, name, out var slice);
        return GetLookupRootPage(slice);
    }
    
    public long GetLookupRootPage(Slice name)
    {
        return _fieldsTree?.GetLookupRootPage(name) ?? -1;
    }

    public bool TryGetField(long fieldRootPage, out string fieldName)
    {
        _fieldsRootPages ??= _fieldsTree?.GetFieldsRootPages() ?? new();
        return _fieldsRootPages.TryGetValue(fieldRootPage, out fieldName);
    }
}
