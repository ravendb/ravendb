using System;
using System.Runtime.CompilerServices;
using Corax.Utils;
using Sparrow;
using Sparrow.Compression;

namespace Corax;

public unsafe struct SpatialPointFieldIterator
{
    //<type:byte><extended_type:byte><amount_of_items:int><geohashLevel:int><geohash_ptr:int>
    //<longitudes_ptr:int><latitudes_list:double[]><longtitudes_list:double[]><geohashes_list:bytes[]>

    public readonly IndexEntryFieldType Type;
    public readonly bool IsValid;
    public readonly int Count;
    private int _currentIdx;

    private readonly byte* _buffer;
    private readonly int _geohashLevel;

    private int _geohashOffset;
    private int _latitudeOffset;
    private int _longitudeOffset;

    public SpatialPointFieldIterator(byte* buffer, int offset)
    {
        _buffer = buffer;
        Type = *(IndexEntryFieldType*)(buffer + offset);
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

        Count = VariableSizeEncoding.Read<ushort>(_buffer + offset, out var length);
        offset += length;

        _geohashLevel = VariableSizeEncoding.Read<ushort>(_buffer + offset, out length);
        offset += length;


        _geohashOffset = *(int*)(_buffer + offset);
        offset += sizeof(int);

        _longitudeOffset = *(int*)(_buffer + offset);
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

            return new ReadOnlySpan<byte>(_buffer + _geohashOffset, _geohashLevel);
        }
    }

    public double Latitude
    {
        get
        {
            if (_currentIdx >= Count)
                throw new IndexOutOfRangeException();

            return *(double*)(_buffer + _latitudeOffset);
        }
    }

    public double Longitude
    {
        get
        {
            if (_currentIdx >= Count)
                throw new IndexOutOfRangeException();

            return *(double*)(_buffer + _longitudeOffset);
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
