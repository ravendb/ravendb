using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Impl;

namespace Corax.IndexSearcher;

public struct SpatialReader : IDisposable
{
    private readonly FixedSizeTree _fst;

    public SpatialReader(LowLevelTransaction llt, Tree entriesToSpatialTree, Slice name)
    {
        _fst = entriesToSpatialTree.FixedTreeFor(name, sizeof(double) + sizeof(double));
    }

    public bool IsValid => _fst != null;
    
    public unsafe bool TryGetSpatialPoint(long id, out (double Lat, double Lng) coords)
    {
        using var _ = _fst.Read(id, out var coordPtr);
        if (coordPtr.HasValue == false)
        {
            coords = default;
            return false;
        }

        Debug.Assert(coordPtr.Size == sizeof(double) + sizeof(double));
        coords = (Unsafe.ReadUnaligned<double>(coordPtr.Content.Ptr),
            Unsafe.ReadUnaligned<double>(coordPtr.Content.Ptr + sizeof(double)));
        return true;
    }

    public void Dispose()
    {
        _fst.Dispose();
    }
}
