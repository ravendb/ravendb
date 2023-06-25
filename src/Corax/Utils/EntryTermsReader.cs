using System;
using System.Runtime.InteropServices;
using Sparrow.Compression;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Impl;

namespace Corax.Utils;

public unsafe struct EntryTermsReader
{
    private readonly LowLevelTransaction _llt;
    private byte* _cur;
    private readonly long _dicId;
    private byte* _end;
    private long _prevTerm;
    private long _prevLong;

    public bool HasNumeric;
    public CompactKey Current;
    public long CurrentLong;
    public double CurrentDouble;
    public long TermMetadata;
    public long TermId;

    public EntryTermsReader(LowLevelTransaction llt, byte* cur, int size, long dicId)
    {
        _llt = llt;
        _cur = cur;
        _dicId = dicId;
        _end = cur + size;
        _prevTerm = 0;
        _prevLong = 0;
        Current = new(llt);
    }

    public bool MoveNext()
    {
        if (_cur >= _end)
            return false;

        byte* cur = _cur;
        var termContainerId = VariableSizeEncoding.Read<long>(cur, out var offset) + _prevTerm;
        _prevTerm = termContainerId;
        cur += offset;
        TermId = termContainerId ^ 3; // clear the marker bits
        var termItem = Container.Get(_llt, TermId);
        TermMetadata = termItem.PageLevelMetadata;
        TermsReader.Set(Current, termItem, _dicId);

        HasNumeric = (termContainerId & 1) != 0;
        if (HasNumeric)
        {
            CurrentLong = ZigZagEncoding.Decode<long>(cur, out offset) + _prevLong;
            _prevLong = CurrentLong;
            cur += offset;

            if ((termContainerId & 2) == 2)
            {
                CurrentDouble = *(double*)cur;
                cur += sizeof(double);
            }
            else
            {
                CurrentDouble = CurrentLong;
            }
        }
        _cur = cur;
        return true;
    }
}
