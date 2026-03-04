using System;
using System.Diagnostics;
using System.Runtime.Intrinsics;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Impl;
using Voron.Util;

namespace Corax.Querying;

public unsafe struct TermsReader : IDisposable
{
    private readonly LowLevelTransaction _llt;
    private readonly Lookup<Int64LookupKey> _lookup;
    private readonly CompactKeyCacheScope _xKeyScope, _yKeyScope;

    private const int CacheSize = 64;
    private readonly (long Key, UnmanagedSpan Term)* _cache;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _cacheScope;
    private Page _lastPage;
    private readonly long _dictionaryId;
    private ContextBoundNativeList<long> _pagesToPrefetch;
    private ContextBoundNativeList<long> _termsLocation;
    private ContextBoundNativeList<UnmanagedSpan> _rawTermsContainer;
    
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
        _dictionaryId = CompactTree.GetDictionaryId(llt);
        _pagesToPrefetch = new ContextBoundNativeList<long>(_llt.Allocator);
        _termsLocation = new ContextBoundNativeList<long>(_llt.Allocator);
        _rawTermsContainer = new ContextBoundNativeList<UnmanagedSpan>(_llt.Allocator);
    }

    public string GetTermFor(long id)
    {
        TryGetTermFor(id, out string s);
        return s;
    }
    
    /// <summary>
    /// Read terms in bulk. The result is not associated with ids by index.
    /// Caution:
    /// - the `termsSet` span is being invalidated between calls.
    /// - the method modifies 'ids' (change the order) of the buffer (!)
    /// - the order of the terms is based on sorted ids values
    /// </summary>
    public int GetAllTermsFromSet(Span<long> ids, out Span<UnmanagedSpan> termsSet)
    {
        const int pageSizeShift = 13;
        Debug.Assert(1 << pageSizeShift == (long)Voron.Global.Constants.Storage.PageSize);
        
        // Process a maximum of 1024 IDs per iteration to limit memory allocations within the terms reader.
        // Testing demonstrated that larger buffer sizes do not provide additional benefits.
        var maxToProcess = Math.Min(ids.Length, 1024); 
        ids = ids[..maxToProcess];

        if (ids.IsEmpty)
        {
            termsSet = default;
            return 0;
        }
        
        _termsLocation.ResetAndEnsureCapacity(ids.Length);
        _termsLocation.Count = ids.Length;
        _lookup.GetFor(ids, _termsLocation.ToSpan(), -1L);
        
        var idX = 0;
        _pagesToPrefetch.ResetAndEnsureCapacity(ids.Length);
        _pagesToPrefetch.Count = ids.Length;
        _termsLocation.CopyTo(_pagesToPrefetch.ToSpan(), startFrom: 0);
        
        if (AdvInstructionSet.IsAcceleratedVector128)
        {
            var N = Vector512<long>.Count;
            for (; idX + N < _pagesToPrefetch.Count; idX += N)
            {
                var ptr = _pagesToPrefetch.RawItems + idX;
                var containers = Vector512.Load(ptr);
                Vector512.ShiftRightArithmetic(containers, pageSizeShift).Store(ptr);
            }
        }

        for (; idX < _pagesToPrefetch.Count; ++idX)
            _pagesToPrefetch[idX] >>= pageSizeShift;
        
        _pagesToPrefetch.Count = Sorting.SortAndRemoveDuplicates(_pagesToPrefetch.RawItems, _pagesToPrefetch.Count); 
        _llt.DataPager.MaybePrefetchMemory(_pagesToPrefetch.GetEnumerator());
        
        _rawTermsContainer.ResetAndEnsureCapacity(ids.Length);
        _rawTermsContainer.Count = ids.Length;
        Container.GetAll(_llt, _termsLocation.ToSpan(), _rawTermsContainer.ToSpan(), -1L, _llt.PageLocator);
        termsSet = _rawTermsContainer.ToSpan();
        return maxToProcess;
    }
    
    public bool TryGetRawTermFor(long id, out UnmanagedSpan term)
    {
        if (_lookup.TryGetValue(id, out var termEntryId) == false)
        {
            term = default;
            return false;
        }

        Container.Get(_llt, new ContainerEntryId(termEntryId), out var item);
        term = item.ToUnmanagedSpan();
        return true;
    }
    
    public bool TryGetTermFor(long id, out string term)
    {
        if (_lookup == null ||
            _lookup.TryGetValue(id, out var termEntryId) == false)
        {
            term = null;
            return false;
        }

        Container.Get(_llt, new ContainerEntryId(termEntryId), out var item);
        Set(_xKeyScope.Key, item, _dictionaryId);
        term = _xKeyScope.Key.ToString();
        return true;
    }

    public static void Set(CompactKey key, in Container.Item item, long dictionaryId)
    {
        int remainderBits = item.Address[0] >> 4;
        int encodedKeyLengthInBits = (item.Length - 1) * 8 - remainderBits;

        key.Set(encodedKeyLengthInBits, item.Address + 1, dictionaryId);
    }

    public ReadOnlySpan<byte> GetDecodedTerm(long dictionaryId, UnmanagedSpan x)
    {
        DecodeKey(_xKeyScope, x.Address, x.Length, dictionaryId, out var xTerm);
        return xTerm;
    }
    
    public void GetDecodedTermsByIds(long dictionaryId, long xIds, out ReadOnlySpan<byte> xTerm, long yIds, out ReadOnlySpan<byte> yTerm)
    {
        var xKey = GetTerm(xIds);
        var yKey = GetTerm(yIds);
        DecodeKey(_xKeyScope, xKey.Address, xKey.Length, dictionaryId, out xTerm);
        DecodeKey(_yKeyScope, yKey.Address, yKey.Length, dictionaryId, out yTerm);
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


    public UnmanagedSpan GetTerm(long entryId, long nullTermId = -1, long nonExistingTermId = -1)
    {
        var idx = (uint)Hashing.Mix(entryId) % CacheSize;
        ref (long Key, UnmanagedSpan Value) cache = ref _cache[idx];

        if (cache.Key == entryId)
        {
            return cache.Value;
        }

        var hasValue = _lookup.TryGetValue(entryId, out var termId);
        if (hasValue && (termId == nullTermId || termId == nonExistingTermId))
        {
            cache = (entryId, default);
            return default;
        }
        
        
        UnmanagedSpan term = UnmanagedSpan.Empty;
        if (hasValue)
        {
            var item = Container.MaybeGetFromSamePage(_llt, ref _lastPage, new ContainerEntryId(termId));
            term = item.ToUnmanagedSpan();
        }

        cache = (entryId, term);
        return term;
    }
    
    public void Dispose()
    {
        _pagesToPrefetch.Dispose();
        _termsLocation.Dispose();
        _rawTermsContainer.Dispose();
        _cacheScope.Dispose();
        _yKeyScope.Dispose();
        _xKeyScope .Dispose();
    }
}
