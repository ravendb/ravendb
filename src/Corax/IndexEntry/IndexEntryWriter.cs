using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Fields;
using Corax.Utils;
using Corax.Utils.Spatial;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server.Compression;

namespace Corax.IndexEntry;

// The rationale to use ref structs is not allowing to copy those around so easily. They should be cheap to construct
// and cheaper to use. 
public unsafe ref partial struct IndexEntryWriter
{
    private static int Invalid = unchecked(~0);

    private static int LocationMask = 0x7FFF_FFFF;

    private readonly IndexFieldsMapping _knownFields;

    // The usable part of the buffer, the metadata space will be removed from the usable space.
    private readonly Span<byte> _buffer;

    // Temporary location for the pointers, these will eventually be encoded based on how big they are.
    // <256 bytes we could use a single byte
    // <65546 bytes we could use a single ushort
    // for the rest we will use a uint.
    private readonly Span<int> _knownFieldsLocations;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsEmpty() => Unsafe.SizeOf<IndexEntryHeader>() == _dataIndex;

    // Current pointer.        
    private int _dataIndex;

    // Dynamic fields will use a full integer to store the pointer location at the metadata table. They are supposed to be rare 
    // so we wont even try to make the process more complex just to deal with them efficienly.
    private int _dynamicFieldIndex;

    public IndexEntryWriter(Span<byte> buffer, IndexFieldsMapping knownFields)
    {
        // TODO: For now we will assume that the max size of an index entry is 32Kb, revisit this...
        _knownFields = knownFields;
        int knownFieldMetadataSize = _knownFields.Count * sizeof(uint);
        _knownFieldsLocations = MemoryMarshal.Cast<byte, int>(buffer[^knownFieldMetadataSize..]);
        _knownFieldsLocations.Fill(Invalid); // We prepare the table in order to avoid tracking the writes. 

        _buffer = buffer[..^knownFieldMetadataSize];
        _dynamicFieldIndex = 0;
        _dataIndex = Unsafe.SizeOf<IndexEntryHeader>();
    }

    public void WriteNull(int field)
    {
        // Write known field.
        _knownFieldsLocations[field] = _dataIndex | Constants.IndexWriter.IntKnownFieldMask;
        Unsafe.WriteUnaligned(ref _buffer[_dataIndex], IndexEntryFieldType.Null);
        _dataIndex += sizeof(IndexEntryFieldType);
    }

    /// <summary>
    ///  Writes binary sequence into buffer (strings etc)
    /// </summary>
    public void Write(int field, ReadOnlySpan<byte> value)
    {
        Debug.Assert(field < _knownFields.Count);
        Debug.Assert(_knownFieldsLocations[field] == Invalid);

        // Write known field.
        ref int fieldLocation = ref _knownFieldsLocations[field];                
        fieldLocation = _dataIndex;

        int length = VariableSizeEncoding.Write(_buffer, value.Length, _dataIndex);
        _dataIndex += length;        
        if (value.Length == 0)
            return;

        value.CopyTo(_buffer.Slice(_dataIndex, value.Length));        
        _dataIndex += value.Length;
    }

    /// <summary>
    ///  Writes binary into buffer. This field will contain flag RAW and it will not be indexed.
    /// </summary>
    public void WriteRaw(int field, ReadOnlySpan<byte> binaryValue)
    {
        //STRUCT
        //<type><size_of_binary><binary>
        Debug.Assert(field < _knownFields.Count);
        Debug.Assert(_knownFieldsLocations[field] == Invalid);

        int dataLocation = _dataIndex;

        // Write known field.
        ref int fieldLocation = ref _knownFieldsLocations[field];
        fieldLocation = dataLocation | Constants.IndexWriter.IntKnownFieldMask;

        // Write the list metadata information. 
        ref var indexEntryField = ref Unsafe.AsRef<IndexEntryFieldType>(Unsafe.AsPointer(ref _buffer[dataLocation]));
        indexEntryField = IndexEntryFieldType.Raw;
        dataLocation += sizeof(IndexEntryFieldType);

        if (binaryValue.Length == 0)
        {
            // Signal that we will have to deal with the empties.
            indexEntryField |= IndexEntryFieldType.Empty;
        }
        else
        {
            dataLocation += VariableSizeEncoding.Write(_buffer, binaryValue.Length, dataLocation);

            binaryValue.CopyTo(_buffer.Slice(dataLocation));
            dataLocation += binaryValue.Length;
        }

        _dataIndex = dataLocation;
    }


    public void WriteSpatial(int field, CoraxSpatialPointEntry entry)
    {
        Debug.Assert(field < _knownFields.Count);
        Debug.Assert(_knownFieldsLocations[field] == Invalid);

        if (entry.Geohash.Length == 0)
            return;

        int dataLocation = _dataIndex;

        // Write known field pointer.
        ref int fieldLocation = ref _knownFieldsLocations[field];
        fieldLocation = dataLocation | Constants.IndexWriter.IntKnownFieldMask;

        // Write the spatial point information. 
        ref var indexEntryField = ref Unsafe.AsRef<IndexEntryFieldType>(Unsafe.AsPointer(ref _buffer[dataLocation]));
        indexEntryField = IndexEntryFieldType.SpatialPoint;
        dataLocation += sizeof(IndexEntryFieldType);

        Unsafe.WriteUnaligned(ref _buffer[dataLocation], entry.Latitude);
        dataLocation += sizeof(double);
        Unsafe.WriteUnaligned(ref _buffer[dataLocation], entry.Longitude);
        dataLocation += sizeof(double);

        // Copy the actual string data.
        dataLocation += VariableSizeEncoding.Write(_buffer, (byte)entry.Geohash.Length, dataLocation);

        int geohashLength = entry.Geohash.Length;
        Span<byte> encodedGeohash = Encodings.Utf8.GetBytes(entry.Geohash);
        encodedGeohash.CopyTo(_buffer.Slice(dataLocation, geohashLength));

        _dataIndex = dataLocation + geohashLength;
    }

    public void WriteSpatial(int field, ReadOnlySpan<CoraxSpatialPointEntry> entries, int geohashLevel = SpatialUtils.DefaultGeohashLevel)
    {
        //<type:byte><extended_type:byte><amount_of_items:int><geohashLevel:int><geohash_ptr:int>
        //<longitudes_ptr:int><latitudes_list:double[]><longtitudes_list:double[]><geohashes_list:bytes[]>
        Debug.Assert(field < _knownFields.Count);
        Debug.Assert(_knownFieldsLocations[field] == Invalid);

        if (entries.Length == 0)
            return;

        int dataLocation = _dataIndex;

        // Write known field pointer.
        ref int fieldLocation = ref _knownFieldsLocations[field];
        fieldLocation = dataLocation | Constants.IndexWriter.IntKnownFieldMask;

        // Write the spatial point list. 
        ref var indexEntryField = ref Unsafe.AsRef<IndexEntryFieldType>(Unsafe.AsPointer(ref _buffer[dataLocation]));
        indexEntryField = IndexEntryFieldType.SpatialPointList;
        dataLocation += sizeof(IndexEntryFieldType);

        dataLocation += VariableSizeEncoding.Write(_buffer, entries.Length, dataLocation); // Size of list.

        dataLocation += VariableSizeEncoding.Write(_buffer, geohashLevel, dataLocation); // geohash lvl

        ref int geohashPtrTableLocation = ref Unsafe.AsRef<int>(Unsafe.AsPointer(ref _buffer[dataLocation]));
        dataLocation += sizeof(int);

        ref int longitudesPtrLocation = ref Unsafe.AsRef<int>(Unsafe.AsPointer(ref _buffer[dataLocation]));
        dataLocation += sizeof(int);

        var latitudesList = MemoryMarshal.Cast<byte, double>(_buffer.Slice(dataLocation, entries.Length * sizeof(double)));
        for (int i = 0; i < entries.Length; ++i)
            latitudesList[i] = entries[i].Latitude;
        dataLocation += entries.Length * sizeof(double);

        longitudesPtrLocation = dataLocation;
        var longitudesList = MemoryMarshal.Cast<byte, double>(_buffer.Slice(dataLocation, entries.Length * sizeof(double)));
        for (int i = 0; i < entries.Length; ++i)
            longitudesList[i] = entries[i].Longitude;
        dataLocation += entries.Length * sizeof(double);

        geohashPtrTableLocation = dataLocation;
        for (int i = 0; i < entries.Length; ++i)
        {
            Encodings.Utf8.GetBytes(entries[i].Geohash).CopyTo(_buffer[dataLocation..]);
            dataLocation += geohashLevel;
        }

        _dataIndex = dataLocation;
    }

    public void Write(int field, ReadOnlySpan<byte> value, long longValue, double doubleValue)
    {
        Debug.Assert(field < _knownFields.Count);
        Debug.Assert(_knownFieldsLocations[field] == Invalid);

        int dataLocation = _dataIndex;

        // Write known field pointer.
        ref int fieldLocation = ref _knownFieldsLocations[field];
        fieldLocation = dataLocation | Constants.IndexWriter.IntKnownFieldMask;

        // Write the tuple information. 
        ref var indexEntryField = ref Unsafe.AsRef<IndexEntryFieldType>(Unsafe.AsPointer(ref _buffer[dataLocation]));
        indexEntryField = IndexEntryFieldType.Tuple;
        dataLocation += sizeof(IndexEntryFieldType);

        dataLocation += VariableSizeEncoding.Write(_buffer, longValue, dataLocation);
        Unsafe.WriteUnaligned(ref _buffer[dataLocation], doubleValue);
        dataLocation += sizeof(double);
        dataLocation += VariableSizeEncoding.Write(_buffer, value.Length, dataLocation);

        // Copy the actual string data. 
        if (value.Length != 0)
        {
            value.CopyTo(_buffer.Slice(dataLocation, value.Length));
            dataLocation += value.Length;
        }
        else
        {
            // Signal that we will have to deal with the empties.
            indexEntryField |= IndexEntryFieldType.Empty;
        }

        _dataIndex = dataLocation;
    }

    public void Write<TEnumerator>(int field, TEnumerator values, IndexEntryFieldType type = IndexEntryFieldType.Null) 
        where TEnumerator : IReadOnlySpanIndexer
    {
        Debug.Assert(field < _knownFields.Count);
        Debug.Assert(_knownFieldsLocations[field] == Invalid);

        int dataLocation = _dataIndex;

        // Write known field pointer.
        ref int fieldLocation = ref _knownFieldsLocations[field];
        fieldLocation = dataLocation | Constants.IndexWriter.IntKnownFieldMask;

        // Write the list metadata information. 
        ref var indexEntryField = ref Unsafe.AsRef<IndexEntryFieldType>(Unsafe.AsPointer(ref _buffer[dataLocation]));
        indexEntryField = IndexEntryFieldType.List | type;
        dataLocation += sizeof(IndexEntryFieldType);

        // Size of list.
        dataLocation += VariableSizeEncoding.Write(_buffer, values.Length, dataLocation);

        // Prepare the location to store the pointer where the table of the strings will be (after writing the strings).
        ref int stringPtrTableLocation = ref Unsafe.AsRef<int>(Unsafe.AsPointer(ref _buffer[dataLocation]));
        dataLocation += sizeof(int);

        // Copy the actual string data. 
        if (values.Length == 0)
        {
            // Write the pointer to the location.
            stringPtrTableLocation = Invalid;

            // Signal that we will have to deal with the empties.
            indexEntryField |= IndexEntryFieldType.Empty;
            goto Done;
        }

        int[] stringLengths = ArrayPool<int>.Shared.Rent(values.Length);

        // We start to write the strings in a place we know where it is from implicit positioning...
        // 4b 
        for (int i = 0; i < values.Length; i++)
        {
            var value = values[i];
            value.CopyTo(_buffer[dataLocation..]);
            dataLocation += value.Length;
            stringLengths[i] = value.Length;
        }

        // Write the pointer to the location
        stringPtrTableLocation = dataLocation;
        dataLocation = WriteNullsTableIfRequired(values, dataLocation, ref indexEntryField);
        dataLocation += VariableSizeEncoding.WriteMany<int>(_buffer, stringLengths[0..values.Length], dataLocation);

        ArrayPool<int>.Shared.Return(stringLengths);

        Done:
        _dataIndex = dataLocation;
    }

    private readonly int WriteNullsTableIfRequired<TEnumerator>(TEnumerator values, int dataLocation, ref IndexEntryFieldType indexEntryFieldLocation)
        where TEnumerator : IReadOnlySpanIndexer
    {
        // We will include null values if there are nulls to be stored.           
        int nullBitStreamSize = values.Length / (sizeof(byte) * 8) + (values.Length % (sizeof(byte) * 8) == 0 ? 0 : 1);
        byte* nullStream = stackalloc byte[nullBitStreamSize];
        bool hasNull = false;
        for (int i = 0; i < values.Length; i++)
        {
            if (values.IsNull(i))
            {
                hasNull = true;
                PtrBitVector.SetBitInPointer(nullStream, i, true);
            }
        }

        if (hasNull)
        {
            // Copy the null stream.
            new ReadOnlySpan<byte>(nullStream, nullBitStreamSize)
                .CopyTo(_buffer.Slice(dataLocation, nullBitStreamSize));

            dataLocation += nullBitStreamSize;

            // Signal that we will have to deal with the nulls.
            indexEntryFieldLocation |= IndexEntryFieldType.HasNulls;
        }

        return dataLocation;
    }

    public unsafe void Write(int field, IReadOnlySpanIndexer values, ReadOnlySpan<long> longValues, ReadOnlySpan<double> doubleValues)
    {
        Debug.Assert(field < _knownFields.Count);
        Debug.Assert(_knownFieldsLocations[field] == Invalid);        
        
        if (values.Length != longValues.Length || values.Length != doubleValues.Length)
            throw new ArgumentException("The lengths of the values and longValues and doubleValues must be the same.");

        int dataLocation = _dataIndex;

        // Write known field pointer.
        ref int fieldLocation = ref _knownFieldsLocations[field];
        fieldLocation = dataLocation | Constants.IndexWriter.IntKnownFieldMask;

        // Write the list metadata information. 
        ref var indexEntryField = ref Unsafe.AsRef<IndexEntryFieldType>(Unsafe.AsPointer(ref _buffer[dataLocation]));
        indexEntryField = IndexEntryFieldType.TupleList;
        dataLocation += sizeof(IndexEntryFieldType);       

        dataLocation += VariableSizeEncoding.Write(_buffer, values.Length, dataLocation); // Size of list.

        // Prepare the location to store the pointer where the table of the strings will be (after writing the strings).
        ref int stringPtrTableLocation = ref Unsafe.AsRef<int>(Unsafe.AsPointer(ref _buffer[dataLocation]));
        dataLocation += sizeof(int);

        // Prepare the location to store the pointer where the long values are going to be stored.
        ref int longPtrLocation = ref Unsafe.AsRef<int>(Unsafe.AsPointer(ref _buffer[dataLocation]));
        dataLocation += sizeof(int);
        
        if (values.Length == 0)
        {
            // Write the pointer to the location.
            stringPtrTableLocation = Invalid;
            longPtrLocation = Invalid;

            // Signal that we will have to deal with the empties.
            indexEntryField |= IndexEntryFieldType.Empty;

            goto Done;
        }

        var doubleValuesList = MemoryMarshal.Cast<byte, double>(_buffer.Slice(dataLocation, values.Length * sizeof(double)));
        doubleValues.CopyTo(doubleValuesList);
        dataLocation += doubleValuesList.Length * sizeof(double);

        // We start to write the strings in a place we know where it is from implicit positioning...
        // 4b + 4b + len(values) * 8b
        int[] stringLengths = ArrayPool<int>.Shared.Rent(values.Length);
        for (int i = 0; i < values.Length; i++)
        {
            var value = values[i];
            value.CopyTo(_buffer.Slice(dataLocation, value.Length));
            dataLocation += value.Length;

            stringLengths[i] = value.Length;
        }

        // Write the pointer to the location.
        stringPtrTableLocation = dataLocation;
        dataLocation = WriteNullsTableIfRequired(values, dataLocation, ref indexEntryField);
        dataLocation += VariableSizeEncoding.WriteMany<int>(_buffer, stringLengths[..values.Length], dataLocation);

        // Write the long values
        longPtrLocation = dataLocation;
        dataLocation += VariableSizeEncoding.WriteMany(_buffer, longValues, pos: dataLocation);

        ArrayPool<int>.Shared.Return(stringLengths);
        
        Done:
        _dataIndex = dataLocation;
    }

    public int Finish(out Span<byte> output)
    {
        // We need to know how big the metadata table is going to be.
        int maxOffset = 0;
        for (int i = 0; i < _knownFieldsLocations.Length; i++)
        {
            int offset = _knownFieldsLocations[i];
            if (offset != Invalid)
                maxOffset = Math.Max(offset & LocationMask, maxOffset);
        }

        // We can't use the 'invalid' which is all ones.
        IndexEntryTableEncoding encodeSize = IndexEntryTableEncoding.OneByte;
        if (maxOffset > short.MaxValue - 1)
            encodeSize = IndexEntryTableEncoding.FourBytes;
        else if (maxOffset > sbyte.MaxValue - 1)
            encodeSize = IndexEntryTableEncoding.TwoBytes;

        // The size of the known fields metadata section
        int metadataSection = (int)encodeSize * _knownFields.Count;
        // The size of the unknown/dynamic fields metadata section            
        int dynamicMetadataSection = _dynamicFieldIndex * sizeof(uint);
        int dynamicMetadataSectionOffset = _buffer.Length - dynamicMetadataSection - 1;

        ref var header = ref MemoryMarshal.AsRef<IndexEntryHeader>(_buffer);
        header.Length = (uint)(_dataIndex + dynamicMetadataSection + metadataSection + 1);

        // The known field count is encoded as xxxxxxyy where:
        // x: the count
        // y: the encode size
        header.KnownFieldCount = (ushort)(_knownFields.Count << 2 | (int)encodeSize);
        header.DynamicTable = (uint)_dataIndex;

        // The dynamic metadata fields count. 
        Unsafe.WriteUnaligned(ref _buffer[_dataIndex], (byte)_dynamicFieldIndex);
        _dataIndex += sizeof(byte);

        if (dynamicMetadataSection != 0)
        {
            // From the offset to the end... move the data toward the closest position
            var metadataTable = _buffer[dynamicMetadataSectionOffset..];
            metadataTable.CopyTo(_buffer[_dataIndex..]);

            // Move the pointer to the end of the copied section.
            _dataIndex += dynamicMetadataSection;
        }

        switch (encodeSize)
        {
            case IndexEntryTableEncoding.OneByte:
                _dataIndex += WriteMetadataTable<byte>(_buffer, _dataIndex, _knownFieldsLocations);
                break;
            case IndexEntryTableEncoding.TwoBytes:
                _dataIndex += WriteMetadataTable<ushort>(_buffer, _dataIndex, _knownFieldsLocations);
                break;
            case IndexEntryTableEncoding.FourBytes:
                _dataIndex += WriteMetadataTable<uint>(_buffer, _dataIndex, _knownFieldsLocations);
                break;
            default:
                throw new ArgumentException($"'{encodeSize}' is not a valid {nameof(IndexEntryTableEncoding)}.");
        }

        output = _buffer.Slice(0, _dataIndex);
        return _dataIndex;
    }

    private static int WriteMetadataTable<T>(Span<byte> buffer, int dataIndex, Span<int> locations) where T : unmanaged
    {
        int offset = Unsafe.SizeOf<T>();

        var count = locations.Length;
        for (int i = 0; i < count; i++)
        {
            int value = locations[i];
            int location = value & LocationMask;
            bool isTyped = value != location;

            if (isTyped)
            {
                location |= Unsafe.SizeOf<T>() switch
                {
                    sizeof(int) => Constants.IndexWriter.IntKnownFieldMask,
                    sizeof(short) => Constants.IndexWriter.ShortKnownFieldMask,
                    sizeof(byte) => Constants.IndexWriter.ByteKnownFieldMask,
                    _ => 0
                };
            }

            if (typeof(T) == typeof(uint))
                Unsafe.WriteUnaligned(ref buffer[dataIndex + i * offset], location);
            else if (typeof(T) == typeof(ushort))
                Unsafe.WriteUnaligned(ref buffer[dataIndex + i * offset], (ushort)location);
            else if (typeof(T) == typeof(byte))
                Unsafe.WriteUnaligned(ref buffer[dataIndex + i * offset], (byte)location);
            else
                throw new NotSupportedException("Type is unsupported.");
        }

        return count * offset;
    }
}
