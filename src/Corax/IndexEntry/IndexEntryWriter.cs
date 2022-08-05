using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Utils;
using Corax.Utils.Spatial;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Server.Compression;
using Voron;
using Voron.Impl;

namespace Corax;

// The rationale to use ref structs is not allowing to copy those around so easily. They should be cheap to construct
// and cheaper to use. 
public unsafe partial struct IndexEntryWriter : IDisposable
{
    private static int Invalid = unchecked(~0);

    private static int LocationMask = 0x7FFF_FFFF;

    private readonly IndexFieldsMapping _knownFields;

    private readonly ByteStringContext _context;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _bufferScope;
    private ByteString _rawBuffer;

    private int FreeSpace => _rawBuffer.Length - KnownFieldMetadataSize - _dataIndex - _dynamicFieldIndex;

    // The usable part of the buffer, the metadata space will be removed from the usable space.
    private Span<byte> Buffer => new Span<byte>(_rawBuffer.Ptr, _rawBuffer.Length - KnownFieldMetadataSize);

    // Temporary location for the pointers, these will eventually be encoded based on how big they are.
    // <256 bytes we could use a single byte
    // <65546 bytes we could use a single ushort
    // for the rest we will use a uint.
    private Span<int> KnownFieldsLocations => new Span<int>(_rawBuffer.Ptr + _rawBuffer.Length - KnownFieldMetadataSize, _knownFields.Count);

    private int KnownFieldMetadataSize => _knownFields.Count * sizeof(uint);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsEmpty() => Unsafe.SizeOf<IndexEntryHeader>() == _dataIndex;

    // Current pointer.        
    private int _dataIndex;

    // Dynamic fields will use a full integer to store the pointer location at the metadata table. They are supposed to be rare 
    // so we wont even try to make the process more complex just to deal with them efficienly.
    private int _dynamicFieldIndex;

    public IndexEntryWriter(LowLevelTransaction llt, IndexFieldsMapping knownFields) 
        : this(llt.Allocator, knownFields)
    {
    }

    public IndexEntryWriter(ByteStringContext context, IndexFieldsMapping knownFields) 
    {
        _context = context;
        _knownFields = knownFields;

        _bufferScope = _context.Allocate(16 * Sparrow.Global.Constants.Size.Kilobyte, out _rawBuffer);

        _dynamicFieldIndex = 0;
        _dataIndex = Unsafe.SizeOf<IndexEntryHeader>();

        // We prepare the table in order to avoid tracking the writes. 
        KnownFieldsLocations.Fill(Invalid);
    }

    public void WriteNull(int field)
    {
        Debug.Assert(_dataIndex != int.MinValue);

        if (FreeSpace < sizeof(IndexEntryFieldType))
            UnlikelyGrowAuxiliaryBuffer();

        // Write known field.
        KnownFieldsLocations[field] = _dataIndex | Constants.IndexWriter.IntKnownFieldMask;

        Unsafe.WriteUnaligned(ref Buffer[_dataIndex], IndexEntryFieldType.Null);
        _dataIndex += sizeof(IndexEntryFieldType);
    }

    /// <summary>
    ///  Writes binary sequence into buffer (strings etc)
    /// </summary>
    public void Write(int field, ReadOnlySpan<byte> value)
    {
        Debug.Assert(field < _knownFields.Count, "The field must be known");
        Debug.Assert(KnownFieldsLocations[field] == Invalid, "The field has been written before.");

        if (FreeSpace < value.Length + sizeof(long))
            UnlikelyGrowAuxiliaryBuffer();

        // Write known field.
        ref int fieldLocation = ref KnownFieldsLocations[field];                
        fieldLocation = _dataIndex;

        var buffer = Buffer;
        if (value.Length == 0)
        {
            fieldLocation |= Constants.IndexWriter.IntKnownFieldMask;
            Unsafe.WriteUnaligned(ref buffer[_dataIndex], IndexEntryFieldType.Empty);
            _dataIndex += sizeof(IndexEntryFieldType);
            return;
        }
        
        int length = VariableSizeEncoding.Write(buffer, value.Length, _dataIndex);
        _dataIndex += length;
        value.CopyTo(buffer.Slice(_dataIndex, value.Length));
        _dataIndex += value.Length;
    }

    /// <summary>
    ///  Writes binary into buffer. This field will contain flag RAW and it will not be indexed.
    /// </summary>
    public void WriteRaw(int field, ReadOnlySpan<byte> binaryValue)
    {
        //STRUCT
        //<type><size_of_binary><binary>
        Debug.Assert(field < _knownFields.Count, "The field must be known");
        Debug.Assert(KnownFieldsLocations[field] == Invalid, "The field has been written before.");

        if (FreeSpace < binaryValue.Length + sizeof(long))
            UnlikelyGrowAuxiliaryBuffer();

        int dataLocation = _dataIndex;

        // Write known field.
        ref int fieldLocation = ref KnownFieldsLocations[field];
        fieldLocation = dataLocation | Constants.IndexWriter.IntKnownFieldMask;

        var buffer = Buffer;

        // Write the list metadata information. 
        ref var indexEntryField = ref Unsafe.AsRef<IndexEntryFieldType>(Unsafe.AsPointer(ref buffer[dataLocation]));
        indexEntryField = IndexEntryFieldType.Raw;
        dataLocation += sizeof(IndexEntryFieldType);

        if (binaryValue.Length == 0)
        {
            // Signal that we will have to deal with the empties.
            indexEntryField |= IndexEntryFieldType.Empty;
        }
        else
        {
            dataLocation += VariableSizeEncoding.Write(buffer, binaryValue.Length, dataLocation);

            binaryValue.CopyTo(buffer.Slice(dataLocation));
            dataLocation += binaryValue.Length;
        }

        _dataIndex = dataLocation;
    }


    public void WriteSpatial(int field, CoraxSpatialPointEntry entry)
    {
        Debug.Assert(field < _knownFields.Count, "The field must be known");
        Debug.Assert(KnownFieldsLocations[field] == Invalid, "The field has been written before.");

        if (entry.Geohash.Length == 0)
            return;

        int requiredSpace = sizeof(IndexEntryFieldType) + entry.Geohash.Length + 4 * sizeof(long);
        if (FreeSpace < requiredSpace)
            UnlikelyGrowAuxiliaryBuffer(requiredSpace);

        int dataLocation = _dataIndex;

        // Write known field pointer.
        ref int fieldLocation = ref KnownFieldsLocations[field];
        fieldLocation = dataLocation | Constants.IndexWriter.IntKnownFieldMask;

        var buffer = Buffer;

        // Write the spatial point information. 
        ref var indexEntryField = ref Unsafe.AsRef<IndexEntryFieldType>(Unsafe.AsPointer(ref buffer[dataLocation]));
        indexEntryField = IndexEntryFieldType.SpatialPoint;
        dataLocation += sizeof(IndexEntryFieldType);

        Unsafe.WriteUnaligned(ref buffer[dataLocation], entry.Latitude);
        dataLocation += sizeof(double);
        Unsafe.WriteUnaligned(ref buffer[dataLocation], entry.Longitude);
        dataLocation += sizeof(double);

        // Copy the actual string data.
        dataLocation += VariableSizeEncoding.Write(buffer, (byte)entry.Geohash.Length, dataLocation);

        int geohashLength = entry.Geohash.Length;
        Span<byte> encodedGeohash = Encodings.Utf8.GetBytes(entry.Geohash);
        encodedGeohash.CopyTo(buffer.Slice(dataLocation, geohashLength));

        _dataIndex = dataLocation + geohashLength;
    }

    public void WriteSpatial(int field, ReadOnlySpan<CoraxSpatialPointEntry> entries, int geohashLevel = SpatialUtils.DefaultGeohashLevel)
    {
        //<type:byte><extended_type:byte><amount_of_items:int><geohashLevel:int><geohash_ptr:int>
        //<longitudes_ptr:int><latitudes_list:double[]><longtitudes_list:double[]><geohashes_list:bytes[]>
        Debug.Assert(field < _knownFields.Count, "The field must be known");
        Debug.Assert(KnownFieldsLocations[field] == Invalid, "The field has been written before.");

        if (entries.Length == 0)
            return;

        // Since Geohashes are ASCII characters, the total required space is exactly the length
        int requiredSpace = 0;
        foreach (var entry in entries)
            requiredSpace += entry.Geohash.Length;

        // We are calculating the space based on the necessary space needed to storage the geohashes values in doubles.
        requiredSpace += sizeof(IndexEntryFieldType) + 2 * entries.Length * sizeof(double) + 4 * sizeof(long);

        if (FreeSpace < requiredSpace)
            UnlikelyGrowAuxiliaryBuffer(requiredSpace);

        int dataLocation = _dataIndex;

        // Write known field pointer.
        ref int fieldLocation = ref KnownFieldsLocations[field];
        fieldLocation = dataLocation | Constants.IndexWriter.IntKnownFieldMask;

        var buffer = Buffer;

        // Write the spatial point list. 
        ref var indexEntryField = ref Unsafe.AsRef<IndexEntryFieldType>(Unsafe.AsPointer(ref buffer[dataLocation]));
        indexEntryField = IndexEntryFieldType.SpatialPointList;
        dataLocation += sizeof(IndexEntryFieldType);
        
        dataLocation += VariableSizeEncoding.Write(buffer, entries.Length, dataLocation); // Size of list.

        dataLocation += VariableSizeEncoding.Write(buffer, geohashLevel, dataLocation); // geohash lvl

        ref int geohashPtrTableLocation = ref Unsafe.AsRef<int>(Unsafe.AsPointer(ref buffer[dataLocation]));
        dataLocation += sizeof(int);

        ref int longitudesPtrLocation = ref Unsafe.AsRef<int>(Unsafe.AsPointer(ref buffer[dataLocation]));
        dataLocation += sizeof(int);

        var latitudesList = MemoryMarshal.Cast<byte, double>(buffer.Slice(dataLocation, entries.Length * sizeof(double)));
        for (int i = 0; i < entries.Length; ++i)
            latitudesList[i] = entries[i].Latitude;
        dataLocation += entries.Length * sizeof(double);

        longitudesPtrLocation = dataLocation;
        var longitudesList = MemoryMarshal.Cast<byte, double>(buffer.Slice(dataLocation, entries.Length * sizeof(double)));
        for (int i = 0; i < entries.Length; ++i)
            longitudesList[i] = entries[i].Longitude;
        dataLocation += entries.Length * sizeof(double);

        geohashPtrTableLocation = dataLocation;
        for (int i = 0; i < entries.Length; ++i)
        {
            var geohashBytes = Encodings.Utf8.GetBytes(entries[i].Geohash);
            geohashBytes.CopyTo(buffer[dataLocation..]);
            dataLocation += geohashLevel;
        }

        _dataIndex = dataLocation;
    }

    public void Write(int field, ReadOnlySpan<byte> value, long longValue, double doubleValue)
    {
        Debug.Assert(field < _knownFields.Count, "The field must be known");
        Debug.Assert(KnownFieldsLocations[field] == Invalid, "The field has been written before.");

        if (FreeSpace < sizeof(IndexEntryFieldType) + value.Length + 4 * sizeof(long))
            UnlikelyGrowAuxiliaryBuffer();

        int dataLocation = _dataIndex;
        var buffer = Buffer;        

        // Write known field pointer.
        ref int fieldLocation = ref KnownFieldsLocations[field];
        fieldLocation = dataLocation | Constants.IndexWriter.IntKnownFieldMask;

        // Write the tuple information. 
        ref var indexEntryField = ref Unsafe.AsRef<IndexEntryFieldType>(Unsafe.AsPointer(ref buffer[dataLocation]));
        indexEntryField = IndexEntryFieldType.Tuple;
        dataLocation += sizeof(IndexEntryFieldType);

        dataLocation += VariableSizeEncoding.Write(buffer, longValue, dataLocation);
        Unsafe.WriteUnaligned(ref buffer[dataLocation], doubleValue);
        dataLocation += sizeof(double);
        dataLocation += VariableSizeEncoding.Write(buffer, value.Length, dataLocation);

        // Copy the actual string data. 
        if (value.Length != 0)
        {
            value.CopyTo(buffer.Slice(dataLocation, value.Length));
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
        Debug.Assert(field < _knownFields.Count, "The field must be known");
        Debug.Assert(KnownFieldsLocations[field] == Invalid, "The field has been written before.");        

        // We are calculating the space required based on the necessary space needed to store
        // the lists, metadata and the content. 
        int requiredSpace = sizeof(IndexEntryFieldType) + 2 * values.Length * sizeof(long) + 4 * sizeof(long);
        for (int i = 0; i < values.Length; i++)
            requiredSpace += values[i].Length;        
        
        if (FreeSpace < requiredSpace)
            UnlikelyGrowAuxiliaryBuffer(requiredSpace);

        int dataLocation = _dataIndex;
        var buffer = Buffer;

        // Write known field pointer.
        ref int fieldLocation = ref KnownFieldsLocations[field];
        fieldLocation = dataLocation | Constants.IndexWriter.IntKnownFieldMask;

        // Write the list metadata information. 
        ref var indexEntryField = ref Unsafe.AsRef<IndexEntryFieldType>(Unsafe.AsPointer(ref buffer[dataLocation]));
        indexEntryField = IndexEntryFieldType.List | type;
        dataLocation += sizeof(IndexEntryFieldType);

        // Size of list.
        dataLocation += VariableSizeEncoding.Write(buffer, values.Length, dataLocation);

        // Prepare the location to store the pointer where the table of the strings will be (after writing the strings).
        ref int stringPtrTableLocation = ref Unsafe.AsRef<int>(Unsafe.AsPointer(ref buffer[dataLocation]));
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
            value.CopyTo(buffer[dataLocation..]);
            dataLocation += value.Length;
            stringLengths[i] = value.Length;
        }
                
        // Write the pointer to the location
        stringPtrTableLocation = dataLocation;
        dataLocation = WriteNullsTableIfRequired(values, dataLocation, ref indexEntryField);
        dataLocation += VariableSizeEncoding.WriteMany<int>(buffer, stringLengths[0..values.Length], dataLocation);

        ArrayPool<int>.Shared.Return(stringLengths);

        Done:
        _dataIndex = dataLocation;
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int WriteNullsTableIfRequired<TEnumerator>(TEnumerator values, int dataLocation, ref IndexEntryFieldType indexEntryFieldLocation)
        where TEnumerator : IReadOnlySpanIndexer
    {        
        // If we have an small number of values, we just allocate the space for the nulls table in the stack. 
        if (values.Length < 32 * 8)
        {            
            return WriteNullsTableIfRequiredStackalloc(values, dataLocation, ref indexEntryFieldLocation);
        }

        // If it is big enough, we will just allocate it on the heap and most likely will end up outside of the hot-path when
        // the JIT compiler aggresively optimize it. 
        return WriteNullsTableIfRequiredHeap(values, dataLocation, ref indexEntryFieldLocation);
    }

    private int WriteNullsTableIfRequiredStackalloc<TEnumerator>(TEnumerator values, int dataLocation, ref IndexEntryFieldType indexEntryFieldLocation)
        where TEnumerator : IReadOnlySpanIndexer
    {
        int nullBitStreamSize = values.Length / (sizeof(byte) * 8) + (values.Length % (sizeof(byte) * 8) == 0 ? 0 : 1);
        Debug.Assert(nullBitStreamSize < 64, "The maximum reasonable to allocate here is 64 bytes.");

        byte* nullBitStreamBuffer = stackalloc byte[nullBitStreamSize];        
        
        // We will include null values if there are nulls to be stored.           
        bool hasNull = false;
        for (int i = 0; i < values.Length; i++)
        {
            if (values.IsNull(i))
            {
                hasNull = true;
                PtrBitVector.SetBitInPointer(nullBitStreamBuffer, i, true);
            }
        }

        if (hasNull)
        {
            // Copy the null stream.
            new ReadOnlySpan<byte>(nullBitStreamBuffer, nullBitStreamSize)
                .CopyTo(Buffer.Slice(dataLocation, nullBitStreamSize));

            dataLocation += nullBitStreamSize;

            // Signal that we will have to deal with the nulls.
            indexEntryFieldLocation |= IndexEntryFieldType.HasNulls;
        }

        return dataLocation;
    }

    private int WriteNullsTableIfRequiredHeap<TEnumerator>(TEnumerator values, int dataLocation, ref IndexEntryFieldType indexEntryFieldLocation)
        where TEnumerator : IReadOnlySpanIndexer
    {
        int nullBitStreamSize = values.Length / (sizeof(byte) * 8) + (values.Length % (sizeof(byte) * 8) == 0 ? 0 : 1);
        
        using var _ = _context.Allocate(nullBitStreamSize, out var nullBitStream);
        nullBitStream.ToSpan().Fill(0); // Initialize with zeros.

        var nullBitStreamBuffer = nullBitStream.Ptr; 

        // We will include null values if there are nulls to be stored.           
        bool hasNull = false;
        for (int i = 0; i < values.Length; i++)
        {
            if (values.IsNull(i))
            {
                hasNull = true;
                PtrBitVector.SetBitInPointer(nullBitStreamBuffer, i, true);
            }
        }

        if (hasNull)
        {            
            // Copy the null stream.
            new ReadOnlySpan<byte>(nullBitStreamBuffer, nullBitStreamSize)
                .CopyTo(Buffer.Slice(dataLocation, nullBitStreamSize));

            dataLocation += nullBitStreamSize;

            // Signal that we will have to deal with the nulls.
            indexEntryFieldLocation |= IndexEntryFieldType.HasNulls;
        }

        return dataLocation;
    }

    public unsafe void Write(int field, IReadOnlySpanIndexer values, ReadOnlySpan<long> longValues, ReadOnlySpan<double> doubleValues)
    {
        Debug.Assert(field < _knownFields.Count, "The field must be known");
        Debug.Assert(KnownFieldsLocations[field] == Invalid, "The field has been written before.");

        if (values.Length != longValues.Length || values.Length != doubleValues.Length)
            throw new ArgumentException("The lengths of the values and longValues and doubleValues must be the same.");

        // We are calculating the space required based on the necessary space needed to store
        // the lists, metadata and the content.  
        int requiredSpace = sizeof(IndexEntryFieldType) + 2 * longValues.Length * sizeof(long) + 4 * sizeof(long);
        for (int i = 0; i < values.Length; i++)
            requiredSpace += values[i].Length;


        if (FreeSpace < requiredSpace)
            UnlikelyGrowAuxiliaryBuffer(requiredSpace);

        int dataLocation = _dataIndex;

        // Write known field pointer.
        ref int fieldLocation = ref KnownFieldsLocations[field];
        fieldLocation = dataLocation | Constants.IndexWriter.IntKnownFieldMask;

        var buffer = Buffer;

        // Write the list metadata information. 
        ref var indexEntryField = ref Unsafe.AsRef<IndexEntryFieldType>(Unsafe.AsPointer(ref buffer[dataLocation]));
        indexEntryField = IndexEntryFieldType.TupleList;
        dataLocation += sizeof(IndexEntryFieldType);       

        dataLocation += VariableSizeEncoding.Write(buffer, values.Length, dataLocation); // Size of list.

        // Prepare the location to store the pointer where the table of the strings will be (after writing the strings).
        ref int stringPtrTableLocation = ref Unsafe.AsRef<int>(Unsafe.AsPointer(ref buffer[dataLocation]));
        dataLocation += sizeof(int);

        // Prepare the location to store the pointer where the long values are going to be stored.
        ref int longPtrLocation = ref Unsafe.AsRef<int>(Unsafe.AsPointer(ref buffer[dataLocation]));
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

        var doubleValuesList = MemoryMarshal.Cast<byte, double>(buffer.Slice(dataLocation, values.Length * sizeof(double)));
        doubleValues.CopyTo(doubleValuesList);
        dataLocation += doubleValuesList.Length * sizeof(double);

        // We start to write the strings in a place we know where it is from implicit positioning...
        // 4b + 4b + len(values) * 8b
        int[] stringLengths = ArrayPool<int>.Shared.Rent(values.Length);
        for (int i = 0; i < values.Length; i++)
        {
            var value = values[i];            
            value.CopyTo(buffer.Slice(dataLocation, value.Length));
            dataLocation += value.Length;
            stringLengths[i] = value.Length;
        }

        // Write the pointer to the location.
        stringPtrTableLocation = dataLocation;
        dataLocation = WriteNullsTableIfRequired(values, dataLocation, ref indexEntryField);
        dataLocation += VariableSizeEncoding.WriteMany<int>(buffer, stringLengths[..values.Length], dataLocation);

        // Write the long values
        longPtrLocation = dataLocation;
        dataLocation += VariableSizeEncoding.WriteMany(buffer, longValues, pos: dataLocation);

        ArrayPool<int>.Shared.Return(stringLengths);
        
        Done:
        _dataIndex = dataLocation;
    }

    public ByteStringContext<ByteStringMemoryCache>.InternalScope Finish(out ByteString output)
    {
        // Since we are at the end of the process, as long as we hve 2 longs of space we can
        // finish the preprocessing to write the data into the new allocated buffer. 
        if (FreeSpace < 2 * sizeof(long))
            UnlikelyGrowAuxiliaryBuffer();
        
        var knownFieldsLocations = KnownFieldsLocations;

        // We need to know how big the metadata table is going to be.
        int maxOffset = 0;
        for (int i = 0; i < knownFieldsLocations.Length; i++)
        {
            int offset = knownFieldsLocations[i];
            if (offset != Invalid)
                maxOffset = Math.Max(offset & LocationMask, maxOffset);
        }

        // We can't use the 'invalid' which is all ones.
        IndexEntryTableEncoding encodeSize = IndexEntryTableEncoding.OneByte;
        if (maxOffset > short.MaxValue - 1)
            encodeSize = IndexEntryTableEncoding.FourBytes;
        else if (maxOffset > sbyte.MaxValue - 1)
            encodeSize = IndexEntryTableEncoding.TwoBytes;

        var buffer = Buffer;

        // The size of the known fields metadata section
        int metadataSection = (int)encodeSize * _knownFields.Count;
        // The size of the unknown/dynamic fields metadata section            
        int dynamicMetadataSection = _dynamicFieldIndex * sizeof(uint);
        int dynamicMetadataSectionOffset = buffer.Length - dynamicMetadataSection - 1;

        ref var header = ref MemoryMarshal.AsRef<IndexEntryHeader>(buffer);
        header.Length = (uint)(_dataIndex + dynamicMetadataSection + metadataSection + 1);

        // The known field count is encoded as xxxxxxyy where:
        // x: the count
        // y: the encode size
        header.KnownFieldCount = (ushort)(_knownFields.Count << 2 | (int)encodeSize);
        header.DynamicTable = (uint)_dataIndex;

        // The dynamic metadata fields count. 
        Unsafe.WriteUnaligned(ref buffer[_dataIndex], (byte)_dynamicFieldIndex);
        _dataIndex += sizeof(byte);

        if (dynamicMetadataSection != 0)
        {
            // From the offset to the end... move the data toward the closest position
            var metadataTable = buffer[dynamicMetadataSectionOffset..];
            metadataTable.CopyTo(buffer[_dataIndex..]);

            // Move the pointer to the end of the copied section.
            _dataIndex += dynamicMetadataSection;
        }

        switch (encodeSize)
        {
            case IndexEntryTableEncoding.OneByte:
                _dataIndex += WriteMetadataTable<byte>(buffer, _dataIndex, knownFieldsLocations);
                break;
            case IndexEntryTableEncoding.TwoBytes:
                _dataIndex += WriteMetadataTable<ushort>(buffer, _dataIndex, knownFieldsLocations);
                break;
            case IndexEntryTableEncoding.FourBytes:
                _dataIndex += WriteMetadataTable<uint>(buffer, _dataIndex, knownFieldsLocations);
                break;
            default:
                throw new ArgumentException($"'{encodeSize}' is not a valid {nameof(IndexEntryTableEncoding)}.");
        }

        // Create the actual output memory buffer that we are going to be returning to the caller. 
        var scope = _context.Allocate(_dataIndex, out output);
        buffer.Slice(0, _dataIndex).CopyTo(output.ToSpan());

        // We are done, we are preparing the stage for the next one. One could argue that why to 
        // prepare for the next after finish instead of explicitely call something like `.Reset()`
        // if we dont know if are going to be write another entry. The usage pattern is to write
        // as many as possible with the same instance, so overall the performance impact is negligible. 
        _dynamicFieldIndex = 0;
        _dataIndex = Unsafe.SizeOf<IndexEntryHeader>();
        KnownFieldsLocations.Fill(Invalid); 

        return scope;
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
                location |= offset switch
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

    private void UnlikelyGrowAuxiliaryBuffer(long requiredSize = 0)
    {
        // Since we are duplicating we need to ensure that the extension will fit.
        var newSize = _rawBuffer.Length * 2;
        while (newSize - _rawBuffer.Length < requiredSize)
            newSize *= 2;

        var newBufferScope = _context.Allocate(newSize, out var newBuffer);

        // We need to copy the data over to the new buffer.
        Unsafe.CopyBlock(newBuffer.Ptr, _rawBuffer.Ptr, (uint)_rawBuffer.Length);
        Unsafe.CopyBlock(newBuffer.Ptr + newBuffer.Length - KnownFieldMetadataSize - _dynamicFieldIndex,
                         _rawBuffer.Ptr + _rawBuffer.Length - KnownFieldMetadataSize - _dynamicFieldIndex,
                         (uint)(_dynamicFieldIndex + KnownFieldMetadataSize));

        _bufferScope.Dispose();
        _bufferScope = newBufferScope;
        _rawBuffer = newBuffer;
    }

    public void Dispose()
    {
        _bufferScope.Dispose();
    }
}
