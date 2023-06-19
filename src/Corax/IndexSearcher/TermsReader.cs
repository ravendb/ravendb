using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Server;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Fixed;
using Voron.Data.Lookups;
using Voron.Impl;

namespace Corax;

public unsafe struct TermsReader : IDisposable
{
    private readonly LowLevelTransaction _llt;
    private readonly Lookup<Int64LookupKey> _lookup;
    private readonly CompactKeyCacheScope _xKeyScope, _yKeyScope;

    private const int CacheSize = 64;
    private readonly (long Key, UnmanagedSpan Term)* _cache;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _cacheScope;
    private Page _lastPage;
    private long _dictionaryId;

    public TermsReader(LowLevelTransaction llt, Tree entriesToTermsTree, Slice name)
    {
        _llt = llt;
        _cacheScope = _llt.Allocator.Allocate(sizeof((long, UnmanagedSpan)) * CacheSize, out var bs);
        bs.Clear();
        _lastPage = new();
        _cache = ((long, UnmanagedSpan)*)bs.Ptr;
        _lookup = entriesToTermsTree.LookupFor<Int64LookupKey>(name);
        _xKeyScope = new CompactKeyCacheScope(_llt);
        _yKeyScope = new CompactKeyCacheScope(_llt);
        // temporary: until we move to proper single dic
        _dictionaryId = PersistentDictionary.CreateDefault(llt);
    }

    public string GetTermFor(long id)
    {
        TryGetTermFor(id, out string s);
        return s;
    }
    
    public bool TryGetRawTermFor(long id, out UnmanagedSpan term)
    {
        if (_lookup.TryGetValue(id, out var termContainerId) == false)
        {
            term = default;
            return false;
        }

        var item = Container.Get(_llt, termContainerId);
        term = item.ToUnmanagedSpan();
        return true;
    }
    
    public bool TryGetTermFor(long id, out string term)
    {
        if (_lookup == null || 
            _lookup.TryGetValue(id, out var termContainerId) == false)
        {
            term = null;
            return false;
        }

        var item = Container.Get(_llt, termContainerId);
        int remainderBits = item.Address[0] >> 4;
        int encodedKeyLengthInBits = (item.Length - 1) * 8 - remainderBits;

        _xKeyScope.Key.Set(encodedKeyLengthInBits, item.ToSpan()[1..], _dictionaryId);
        term = _xKeyScope.Key.ToString();
        return true;
    }
    
    public void GetDecodedTerms(long dictionaryId, UnmanagedSpan x, out ReadOnlySpan<byte> xTerm, UnmanagedSpan y, out ReadOnlySpan<byte> yTerm)
    {
        // we have to do this so we won't get both terms from the same scope, maybe overwriting one another
        DecodeKey(_xKeyScope, x.Address, x.Length, dictionaryId, out xTerm);
        DecodeKey(_yKeyScope, y.Address, y.Length, dictionaryId, out yTerm);
    }

    private static void DecodeKey(CompactKeyCacheScope scope, byte* ptr, int len, long dictionaryId, out ReadOnlySpan<byte> term)
    {
        int remainderBits = ptr[0] >> 4;
        int encodedKeyLengthInBits = (len - 1) * 8 - remainderBits;
        scope.Key.Set(encodedKeyLengthInBits, ptr+1, dictionaryId);
        term = scope.Key.Decoded();
    }

    private UnmanagedSpan GetTerm(long entryId)
    {
        var idx = (uint)Hashing.Mix(entryId) % CacheSize;
        ref (long Key, UnmanagedSpan Value) cache = ref _cache[idx];

        if (cache.Key == entryId)
        {
            return cache.Value;
        }

        var hasValue = _lookup.TryGetValue(entryId, out var termId);
        UnmanagedSpan term = UnmanagedSpan.Empty;
        if (hasValue)
        {
            var item = Container.MaybeGetFromSamePage(_llt, ref _lastPage, termId);
            term = item.ToUnmanagedSpan();
        }

        cache = (entryId, term);
        return term;
    }

    public (long, UnmanagedSpan)[] CacheView => new Span<(long, UnmanagedSpan)>(_cache, CacheSize)
        .ToArray().Where(x =>x.Item1 != 0).ToArray();

    public int Compare(long x, long y)
    {
        var xItem = GetTerm(x);
        var yItem = GetTerm(y);

        if (yItem.Address == null)
        {
            return xItem.Address == null ? 0 : 1;
        }

        if (xItem.Address == null)
            return -1;

        var match = AdvMemory.Compare(xItem.Address + 1, yItem.Address + 1, Math.Min(xItem.Length - 1, yItem.Length - 1));
        if (match != 0)
            return match;
        var xItemLengthInBits = (xItem.Length - 1) * 8 - (xItem.Address[0] >> 4);
        var yItemLengthInBits = (yItem.Length - 1) * 8 - (yItem.Address[0] >> 4);
        return xItemLengthInBits - yItemLengthInBits;
    }

    public void Dispose()
    {
        _cacheScope.Dispose();
        _yKeyScope.Dispose();
        _xKeyScope .Dispose();
    }
}
