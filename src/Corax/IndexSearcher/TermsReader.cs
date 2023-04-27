using System;
using System.Collections.Generic;
using Sparrow.Server;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Fixed;
using Voron.Impl;

namespace Corax;

public readonly unsafe struct TermsReader : IDisposable, IComparer<long>
{
    private readonly LowLevelTransaction _llt;
    private readonly FixedSizeTree _fst;
    private readonly CompactKeyCacheScope _xKeyScope, _yKeyScope;

    public TermsReader(LowLevelTransaction llt, Tree entriesToTermsTree, Slice name)
    {
        _llt = llt;
        _fst = entriesToTermsTree.FixedTreeFor(name, sizeof(long));
        _xKeyScope = new CompactKeyCacheScope(_llt);
        _yKeyScope = new CompactKeyCacheScope(_llt);
    }

    public string GetTermFor(long id)
    {
        TryGetTermFor(id, out var s);
        return s;
    }
    
    public bool TryGetTermFor(long id, out string term)
    {
        using var _ = _fst.Read(id, out var termId);
        if (termId.HasValue == false)
        {
            term = null;
            return false;
        }

        long termContainerId = termId.ReadInt64();
        var item = Container.Get(_llt, termContainerId);
        int remainderBits = item.Address[0] >> 4;
        int encodedKeyLengthInBits = (item.Length - 1) * 8 - remainderBits;

        _xKeyScope.Key.Set(encodedKeyLengthInBits, item.ToSpan()[1..], item.PageLevelMetadata);
        term = _xKeyScope.Key.ToString();
        return true;
    }

    public int Compare(long x, long y)
    {
        using var _ = _fst.Read(x, out var ySlice);
        using var __ = _fst.Read(y, out var xSlice);

        if (ySlice.HasValue == false)
        {
            return xSlice.HasValue == false ? 0 : 1;
        }

        if (xSlice.HasValue == false)
            return -1;

        long xTermId = xSlice.ReadInt64();
        long yTermId = ySlice.ReadInt64();

        var xItem = Container.Get(_llt, xTermId);
        var yItem = Container.Get(_llt, yTermId);
        if (xItem.PageLevelMetadata == yItem.PageLevelMetadata)
        {
            // common code path, compare on the same dictionary
            var match = AdvMemory.Compare(xItem.Address + 1, yItem.Address + 1, Math.Min(xItem.Length - 1, yItem.Length - 1));
            if (match != 0)
                return match;
            var xItemLengthInBits = (xItem.Length - 1) * 8 - (xItem.Address[0] >> 4);
            var yItemLengthInBits = (yItem.Length - 1) * 8 - (yItem.Address[0] >> 4);
            return xItemLengthInBits - yItemLengthInBits;
        }

        var xKey = _xKeyScope.Key;
        var yKey = _yKeyScope.Key;
        return CompareTermsFromDifferentDictionaries();

        int CompareTermsFromDifferentDictionaries()
        {
            var xItemLengthInBits = (xItem.Length - 1) * 8 - (xItem.Address[0] >> 4);
            var yItemLengthInBits = (yItem.Length - 1) * 8 - (yItem.Address[0] >> 4);
            xKey.Set(xItemLengthInBits, xItem.Address + 1, xItem.PageLevelMetadata);
            yKey.Set(yItemLengthInBits, yItem.Address + 1, yItem.PageLevelMetadata);
            var xTerm = xKey.Decoded();
            var yTerm = yKey.Decoded();
            return xTerm.SequenceCompareTo(yTerm);
        }
    }

    public void Dispose()
    {
        _yKeyScope.Dispose();
        _xKeyScope .Dispose();
        _fst.Dispose();
    }
}
