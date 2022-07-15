using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.IndexEntry;
using Sparrow;
using Sparrow.Server.Compression;

namespace Corax.Utils;

public ref struct SpatialPointFieldIterator
{
    //<type:byte><extended_type:byte><amount_of_items:int><geohashLevel:int><geohash_ptr:int>
    //<longitudes_ptr:int><latitudes_list:double[]><longtitudes_list:double[]><geohashes_list:bytes[]>


    public readonly IndexEntryFieldType Type;
    public readonly bool IsValid;
    public readonly int Count;
    private int _currentIdx;

    private readonly ReadOnlySpan<byte> _buffer;
    private int _geohashLevel;
    private int _geohashOffset;
    private int _latitudeOffset;
    private int _longitudeOffset;

    public SpatialPointFieldIterator(ReadOnlySpan<byte> buffer, int offset)
    {
        _buffer = buffer;
        Type = MemoryMarshal.Read<IndexEntryFieldType>(buffer.Slice(offset));
        offset += sizeof(IndexEntryFieldType);


        if (Type.HasFlag(IndexEntryFieldType.SpatialPointList) == false)
        {
            IsValid = false;
            Unsafe.SkipInit(out _currentIdx);
            Unsafe.SkipInit(out _geohashLevel);
            Unsafe.SkipInit(out _geohashOffset);
            Unsafe.SkipInit(out _latitudeOffset);
            Unsafe.SkipInit(out _longitudeOffset);
            Unsafe.SkipInit(out Count);
            return;
        }

        Count = VariableSizeEncoding.Read<ushort>(_buffer, out var length, offset);
        offset += length;

        _geohashLevel = VariableSizeEncoding.Read<ushort>(_buffer, out length, offset);
        offset += length;


        _geohashOffset = MemoryMarshal.Read<int>(_buffer[offset..]);
        offset += sizeof(int);

        _longitudeOffset = MemoryMarshal.Read<int>(_buffer[offset ..]);
        offset += sizeof(int);

        _latitudeOffset = offset;

        _currentIdx = -1;
        _longitudeOffset -= sizeof(double);
        _latitudeOffset -= sizeof(double);
        _geohashOffset -= _geohashLevel;

        IsValid = true;
    }

    public ReadOnlySpan<byte> Geohash
    {
        get
        {
            if (_currentIdx >= Count)
                throw new IndexOutOfRangeException();

            return _buffer.Slice(_geohashOffset, _geohashLevel);
        }
    }

    public double Latitude
    {
        get
        {
            if (_currentIdx >= Count)
                throw new IndexOutOfRangeException();

            return Unsafe.ReadUnaligned<double>(ref MemoryMarshal.GetReference(_buffer[_latitudeOffset..]));
        }
    }

    public double Longitude
    {
        get
        {
            if (_currentIdx >= Count)
                throw new IndexOutOfRangeException();

            return Unsafe.ReadUnaligned<double>(ref MemoryMarshal.GetReference(_buffer[_longitudeOffset..]));
        }
    }

    public CoraxSpatialPointEntry CoraxSpatialPointEntry
    {
        get
        {
            if (_currentIdx >= Count)
                throw new IndexOutOfRangeException();
            
            return new CoraxSpatialPointEntry(Latitude, Longitude, Encodings.Utf8.GetString(Geohash));
        }
    }
    
    public bool ReadNext()
    {
        _currentIdx++;
        if (_currentIdx >= Count)
            return false;
        
        //All of items have fixed size.
        _longitudeOffset += sizeof(double);
        _latitudeOffset += sizeof(double);
        _geohashOffset += _geohashLevel;

        return true;
    }
}
