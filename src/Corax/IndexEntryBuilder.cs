using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Compression;
using Sparrow.Server.Platform;
using Voron;

namespace Corax
{
    /*
     *  format: <total_length:short> <dynamic_field_metadata_table_prt:short> <known_field_count:short> <data_section> <dynamic_field_metadata_table> <known_field_pointers:short>[count]
     *  data: if <known_field_pointer> > 0 then type is <content> else is tuple<long,double,string>
     *  dynamic_field_table: <field_pointers>[X] <table_count> [grows in reverse order]
     *  dynamic_field: <name_length> <name> <dynamic_content>
     *                 if <field_pointer> > 0 then dynamic_content [<content>|tuple<long, double, string>]
     *  content: <type> [<sequence<byte>>|<tuple<long, double, string>>|sequence<tuple>]
     *  tuple<long, double, string>: <length:variable_size><string_table_ptr:sizeof(int)><long_ptr:variable_size><double[X]:sizeof(double)>
     *                               <strings[X]:sequence><string_length_table[X]:var_int>
     */

    [Flags]
    public enum IndexEntryFieldType : ushort
    {
        Null = 0,
        Simple = 1,
        Tuple = 1 << 1,
        List = 1 << 2,
        Raw = 1 << 3,
        Extended = 1 << 5,

        EmptyList = 1 << 13, 
        HasNulls = 1 << 14, // Helper for list writer.
        Invalid = 1 << 15,

        ExtendedList = List | Extended,
        TupleList = List | Tuple,
        RawList = List | Raw
    }

    [Flags]
    public enum ExtendedEntryFieldType : byte
    {
        Null = 0,
        SpatialPoint = 1,
        SpatialWkt = 1 << 2,
    }

    [StructLayout(LayoutKind.Explicit, Size = HeaderSize)]
    internal struct IndexEntryHeader
    {
        // Format for the header: [length: uint][known_field_count:ushort][dynamic_table_offset:uint]
        internal const int LengthOffset = 0; // 0b
        internal const int KnownFieldCountOffset = sizeof(uint); // 4b 
        internal const int MetadataTableOffset = KnownFieldCountOffset + sizeof(ushort); // 4b + 2b = 6b
        internal const int HeaderSize = MetadataTableOffset + sizeof(uint); // 4b + 2b + 4b = 10b

        [FieldOffset(LengthOffset)]
        public uint Length;

        // The known field count is encoded as xxxxxxyy where:
        // x: the count
        // y: the encode size
        [FieldOffset(KnownFieldCountOffset)]
        public ushort KnownFieldCount;

        [FieldOffset(MetadataTableOffset)]
        public uint DynamicTable;
    }

    // The rationale to use ref structs is not allowing to copy those around so easily. They should be cheap to construct
    // and cheaper to use. 
    public unsafe ref struct IndexEntryWriter
    {
        private static int Invalid = unchecked(~0);
        private static ushort MaxDynamicFields = byte.MaxValue;

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

        public IndexEntryWriter(Span<byte> buffer, IndexFieldsMapping knownFields = null)
        {
            // TODO: For now we will assume that the max size of an index entry is 32Kb, revisit this...
            if (knownFields == null)
            {
                _knownFields = IndexFieldsMapping.Instance;
                Debug.Assert(_knownFields.Count == 0);
            }
            else
            {
                _knownFields = knownFields;
            }

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
            _knownFieldsLocations[field] = _dataIndex | unchecked((int)0x80000000);
            Unsafe.WriteUnaligned(ref _buffer[_dataIndex], IndexEntryFieldType.Null);
            _dataIndex += sizeof(IndexEntryFieldType);
        }

        public void Write(int field, ReadOnlySpan<byte> value)
        {
            Debug.Assert(field < _knownFields.Count);
            Debug.Assert(_knownFieldsLocations[field] == Invalid);

            if (value.Length == 0)
                return;

            // Write known field.
            _knownFieldsLocations[field] = _dataIndex;

            int length = VariableSizeEncoding.Write(_buffer, value.Length, _dataIndex);

            ref var src = ref Unsafe.AsRef(value[0]);
            ref var dest = ref _buffer[_dataIndex + length];
            Unsafe.CopyBlock(ref dest, ref src, (uint)value.Length);

            _dataIndex += length + value.Length;
        }

        public void WriteRaw(int field, ReadOnlySpan<byte> binaryValue)
        {
            //STRUCT
            //<type><size_of_binary><binary>
            Debug.Assert(field < _knownFields.Count);
            Debug.Assert(_knownFieldsLocations[field] == Invalid);

            if (binaryValue.Length == 0)
                return;

            int dataLocation = _dataIndex;
            // Write known field.
            _knownFieldsLocations[field] = dataLocation | Constants.IndexWriter.KnownFieldMask;

            Unsafe.WriteUnaligned(ref _buffer[dataLocation], IndexEntryFieldType.Raw);
            dataLocation += sizeof(IndexEntryFieldType);

            dataLocation += VariableSizeEncoding.Write(_buffer, binaryValue.Length, dataLocation);

            binaryValue.CopyTo(_buffer.Slice(dataLocation));
            dataLocation += binaryValue.Length;

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
            _knownFieldsLocations[field] = dataLocation | Constants.IndexWriter.KnownFieldMask;

            // Write the tuple information. 
            dataLocation += VariableSizeEncoding.Write(_buffer, (byte)IndexEntryFieldType.Extended, dataLocation);
            dataLocation += VariableSizeEncoding.Write(_buffer, (byte)ExtendedEntryFieldType.SpatialPoint, dataLocation);

            Unsafe.WriteUnaligned(ref _buffer[dataLocation], entry.Latitude);
            dataLocation += sizeof(double);
            Unsafe.WriteUnaligned(ref _buffer[dataLocation], entry.Longitude);
            dataLocation += sizeof(double);

            // Copy the actual string data.
            dataLocation += VariableSizeEncoding.Write(_buffer, (byte)entry.Geohash.Length, dataLocation);

            
            Span<byte> encodedGeohash = Encodings.Utf8.GetBytes(entry.Geohash);
            ref var src = ref Unsafe.AsRef(encodedGeohash[0]);
            ref var dest = ref _buffer[dataLocation];
            Unsafe.CopyBlock(ref dest, ref src, (uint)entry.Geohash.Length);

            _dataIndex = dataLocation + encodedGeohash.Length;
        }

        public void WriteSpatial(int field, ReadOnlySpan<CoraxSpatialPointEntry> entries, int geohashLevel = SpatialHelper.DefaultGeohashLevel)
        {
            //<type:byte><extended_type:byte><amount_of_items:int><geohashLevel:int><geohash_ptr:int>
            //<longitudes_ptr:int><latitudes_list:double[]><longtitudes_list:double[]><geohashes_list:bytes[]>
            Debug.Assert(field < _knownFields.Count);
            Debug.Assert(_knownFieldsLocations[field] == Invalid);

            if (entries.Length == 0)
                return;

            int dataLocation = _dataIndex;

            // Write known field pointer.
            _knownFieldsLocations[field] = dataLocation | Constants.IndexWriter.KnownFieldMask;

            dataLocation += VariableSizeEncoding.Write(_buffer, (byte)IndexEntryFieldType.ExtendedList, dataLocation);
            dataLocation += VariableSizeEncoding.Write(_buffer, (byte)ExtendedEntryFieldType.SpatialPoint, dataLocation);
            dataLocation += VariableSizeEncoding.Write(_buffer, entries.Length, dataLocation); // Size of list.
            
            dataLocation += VariableSizeEncoding.Write(_buffer, geohashLevel, dataLocation); // geohash lvl
            
            var geohashPtrTableLocation = _buffer.Slice(dataLocation, sizeof(int));
            dataLocation += sizeof(int);
            
            
            var longitudesPtrLocation = _buffer.Slice(dataLocation, sizeof(int));
            dataLocation += sizeof(int);

            
            var latitudesList = MemoryMarshal.Cast<byte, double>(_buffer.Slice(dataLocation, entries.Length * sizeof(double)));
            for (int i = 0; i < entries.Length; ++i)
                latitudesList[i] = entries[i].Latitude;
            dataLocation += entries.Length * sizeof(double);
            
            MemoryMarshal.Write(longitudesPtrLocation, ref dataLocation);
            var longitudesList = MemoryMarshal.Cast<byte, double>(_buffer.Slice(dataLocation, entries.Length * sizeof(double)));
            for (int i = 0; i < entries.Length; ++i)
                longitudesList[i] = entries[i].Longitude;
            dataLocation += entries.Length * sizeof(double);
            
            MemoryMarshal.Write(geohashPtrTableLocation, ref dataLocation);
            for (int i = 0; i < entries.Length; ++i)
            {
                entries[i].GeohashAsBytes.CopyTo(_buffer[dataLocation..]);
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
            _knownFieldsLocations[field] = dataLocation | Constants.IndexWriter.KnownFieldMask;

            // Write the tuple information. 
            Unsafe.WriteUnaligned(ref _buffer[dataLocation], IndexEntryFieldType.Tuple);
            dataLocation += sizeof(IndexEntryFieldType);

            dataLocation += VariableSizeEncoding.Write(_buffer, longValue, dataLocation);
            Unsafe.WriteUnaligned(ref _buffer[dataLocation], doubleValue);
            dataLocation += sizeof(double);
            dataLocation += VariableSizeEncoding.Write(_buffer, value.Length, dataLocation);

            // Copy the actual string data. 
            if (value.Length != 0)
            {
                ref var src = ref Unsafe.AsRef(value[0]);
                ref var dest = ref _buffer[dataLocation];
                Unsafe.CopyBlock(ref dest, ref src, (uint)value.Length);
            }

            _dataIndex = dataLocation + value.Length;
        }    
        
        public unsafe void Write<TEnumerator>(int field, TEnumerator values, IndexEntryFieldType type = IndexEntryFieldType.HasNulls) where TEnumerator : IReadOnlySpanIndexer
        {
            Debug.Assert(field < _knownFields.Count);
            Debug.Assert(_knownFieldsLocations[field] == Invalid);

            int dataLocation = _dataIndex;

            // Write known field pointer.
            _knownFieldsLocations[field] = dataLocation | Constants.IndexWriter.KnownFieldMask;

            // Write the list metadata information. 
            int indexEntryFieldLocation = dataLocation;
            Unsafe.WriteUnaligned(ref _buffer[indexEntryFieldLocation], IndexEntryFieldType.List);
            dataLocation += sizeof(IndexEntryFieldType);

            dataLocation += VariableSizeEncoding.Write(_buffer, values.Length, dataLocation); // Size of list.

            // Prepare the location to store the pointer where the table of the strings will be (after writing the strings).
            var stringPtrTableLocation = _buffer.Slice(dataLocation, sizeof(int));
            dataLocation += sizeof(int);

            // Copy the actual string data. 
            if (values.Length == 0)
            {
                MemoryMarshal.Write(stringPtrTableLocation, ref dataLocation);
                _dataIndex = dataLocation;

                // Signal that we will have to deal with the nulls.
                Unsafe.WriteUnaligned(ref _buffer[indexEntryFieldLocation], Unsafe.ReadUnaligned<IndexEntryFieldType>(ref _buffer[indexEntryFieldLocation]) | IndexEntryFieldType.EmptyList);
                return;
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
            MemoryMarshal.Write(stringPtrTableLocation, ref dataLocation);
            dataLocation = WriteNullsTableIfRequired(values, dataLocation, indexEntryFieldLocation);
            dataLocation += VariableSizeEncoding.WriteMany<int>(_buffer, stringLengths[..values.Length], dataLocation);

            ArrayPool<int>.Shared.Return(stringLengths);

            _dataIndex = dataLocation;
        }

        private readonly int WriteNullsTableIfRequired<TEnumerator>(TEnumerator values, int dataLocation, int indexEntryFieldLocation) where TEnumerator : IReadOnlySpanIndexer
        {
            // We will include null values if there are nulls to be stored.           
            int nullBitStreamSize = values.Length / (sizeof(byte) * 8) + values.Length % (sizeof(byte) * 8) == 0 ? 0 : 1;
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
                new ReadOnlySpan<byte>(nullStream, nullBitStreamSize * sizeof(long))
                    .CopyTo(_buffer.Slice(dataLocation));

                dataLocation += nullBitStreamSize;

                // Signal that we will have to deal with the nulls.
                Unsafe.WriteUnaligned(ref _buffer[indexEntryFieldLocation], Unsafe.ReadUnaligned<IndexEntryFieldType>(ref _buffer[indexEntryFieldLocation]) | IndexEntryFieldType.HasNulls);
            }

            return dataLocation;
        }

        public unsafe void Write(int field, IReadOnlySpanIndexer values, ReadOnlySpan<long> longValues, ReadOnlySpan<double> doubleValues)
        {
            Debug.Assert(field < _knownFields.Count);
            Debug.Assert(_knownFieldsLocations[field] == Invalid);
            Debug.Assert(values.Length == longValues.Length && values.Length == doubleValues.Length);

            int dataLocation = _dataIndex;

            // Write known field pointer.
            _knownFieldsLocations[field] = dataLocation | Constants.IndexWriter.KnownFieldMask;

            // Write the list metadata information. 
            int indexEntryFieldLocation = dataLocation;
            Unsafe.WriteUnaligned(ref _buffer[indexEntryFieldLocation], IndexEntryFieldType.TupleList);
            dataLocation += sizeof(IndexEntryFieldType);

            dataLocation += VariableSizeEncoding.Write(_buffer, values.Length, dataLocation); // Size of list.

            // Prepare the location to store the pointer where the table of the strings will be (after writing the strings).
            var stringPtrTableLocation = _buffer.Slice(dataLocation, sizeof(int));
            dataLocation += sizeof(int);

            // Prepare the location to store the pointer where the long values are going to be stored.
            var longPtrLocation = _buffer.Slice(dataLocation, sizeof(int));
            dataLocation += sizeof(int);

            var doubleValuesList = MemoryMarshal.Cast<byte, double>(_buffer.Slice(dataLocation, values.Length * sizeof(double)));
            doubleValues.CopyTo(doubleValuesList);
            dataLocation += values.Length * sizeof(double);

            if (values.Length == 0)
            {
                // Write the pointer to the location.
                MemoryMarshal.Write(stringPtrTableLocation, ref dataLocation);
                _dataIndex = dataLocation;

                // Signal that we will have to deal with the nulls.
                Unsafe.WriteUnaligned(ref _buffer[indexEntryFieldLocation], Unsafe.ReadUnaligned<IndexEntryFieldType>(ref _buffer[indexEntryFieldLocation]) | IndexEntryFieldType.EmptyList);
                return;
            }                

            // We start to write the strings in a place we know where it is from implicit positioning...
            // 4b + 4b + len(values) * 8b
            int[] stringLengths = ArrayPool<int>.Shared.Rent(values.Length);
            for (int i = 0; i < values.Length; i++)
            {
                var value = values[i];
                value.CopyTo(_buffer[dataLocation..]);
                dataLocation += value.Length;

                stringLengths[i] = value.Length;
            }

            // Write the pointer to the location.
            MemoryMarshal.Write(stringPtrTableLocation, ref dataLocation);
            dataLocation = WriteNullsTableIfRequired(values, dataLocation, indexEntryFieldLocation);
            dataLocation += VariableSizeEncoding.WriteMany<int>(_buffer, stringLengths[..values.Length], dataLocation);

            // Write the long values
            MemoryMarshal.Write(longPtrLocation, ref dataLocation);
            dataLocation += VariableSizeEncoding.WriteMany(_buffer, longValues, pos: dataLocation);

            _dataIndex = dataLocation;
        }

        public void Write(Slice name, ReadOnlySpan<byte> value)
        {
            if (_knownFields.TryGetByFieldName(name, out var binding))
            {
                Write(binding.FieldId, value);
                return;
            }

            // TODO: Add debug capabilities to know we are not adding the same element multiple times. 

            if (_dynamicFieldIndex >= MaxDynamicFields)
                throw new NotSupportedException($"More than {MaxDynamicFields} is unsupported.");

            _dynamicFieldIndex++;
            throw new NotImplementedException();
        }

        public void Write(Slice name, ReadOnlySpan<byte> value, long longValue, double doubleValue)
        {
            if (_knownFields.TryGetByFieldName(name, out var binding))
            {
                Write(binding.FieldId, value, longValue, doubleValue);
                return;
            }

            // TODO: Add debug capabilities to know we are not adding the same element multiple times. 
            if (_dynamicFieldIndex >= MaxDynamicFields)
                throw new NotSupportedException($"More than {MaxDynamicFields} is unsupported.");

            _dynamicFieldIndex++;
            throw new NotImplementedException();
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
            int encodeSize = 1;
            if (maxOffset > short.MaxValue - 1)
                encodeSize = 4;
            else if (maxOffset > sbyte.MaxValue - 1)
                encodeSize = 2;

            // The size of the known fields metadata section
            int metadataSection = encodeSize * _knownFields.Count;
            // The size of the unknown/dynamic fields metadata section            
            int dynamicMetadataSection = _dynamicFieldIndex * sizeof(uint);
            int dynamicMetadataSectionOffset = _buffer.Length - dynamicMetadataSection - 1;

            ref var header = ref MemoryMarshal.AsRef<IndexEntryHeader>(_buffer);
            header.Length = (uint)(_dataIndex + dynamicMetadataSection + metadataSection + 1);

            // The known field count is encoded as xxxxxxyy where:
            // x: the count
            // y: the encode size
            header.KnownFieldCount = (ushort)(_knownFields.Count << 2 | encodeSize);
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
                case 1:
                    _dataIndex += WriteMetadataTable<byte>(_buffer, _dataIndex, _knownFieldsLocations);
                    break;
                case 2:
                    _dataIndex += WriteMetadataTable<ushort>(_buffer, _dataIndex, _knownFieldsLocations);
                    break;
                case 4:
                    _dataIndex += WriteMetadataTable<uint>(_buffer, _dataIndex, _knownFieldsLocations);
                    break;
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
                        sizeof(int) => Constants.IndexWriter.KnownFieldMask,
                        sizeof(short) => 0x8000,
                        sizeof(byte) => 0x80,
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

    public ref struct IndexEntryFieldIterator
    {
        public readonly IndexEntryFieldType Type;
        public readonly bool IsValid;
        public readonly int Count;
        private int _currentIdx;

        private readonly ReadOnlySpan<byte> _buffer;
        private int _spanOffset;
        private int _spanTableOffset;
        private int _nullTableOffset;
        private int _longOffset;
        private int _doubleOffset;
        private readonly bool IsTuple => _doubleOffset != 0;


        internal IndexEntryFieldIterator(IndexEntryFieldType type)
        {
            Debug.Assert(type == IndexEntryFieldType.Invalid);
            Type = IndexEntryFieldType.Invalid;
            Count = 0;
            _buffer = ReadOnlySpan<byte>.Empty;
            IsValid = false;

            Unsafe.SkipInit(out _currentIdx);
            Unsafe.SkipInit(out _spanTableOffset);
            Unsafe.SkipInit(out _nullTableOffset);
            Unsafe.SkipInit(out _spanOffset);
            Unsafe.SkipInit(out _longOffset);
            Unsafe.SkipInit(out _doubleOffset);            
        }

        public IndexEntryFieldIterator(ReadOnlySpan<byte> buffer, int offset)
        {
            _buffer = buffer;

            Type = MemoryMarshal.Read<IndexEntryFieldType>(buffer.Slice(offset));
            offset += sizeof(IndexEntryFieldType);

            if (!Type.HasFlag(IndexEntryFieldType.List))
            {
                IsValid = false;
                Unsafe.SkipInit(out _currentIdx);
                Unsafe.SkipInit(out _spanTableOffset);
                Unsafe.SkipInit(out _nullTableOffset);
                Unsafe.SkipInit(out _spanOffset);
                Unsafe.SkipInit(out _longOffset);
                Unsafe.SkipInit(out _doubleOffset);
                Unsafe.SkipInit(out Count);
                return;
            }

            Count = VariableSizeEncoding.Read<ushort>(_buffer, out var length, offset);
            offset += length;

            _nullTableOffset = MemoryMarshal.Read<int>(_buffer[offset..]);
            if (Type.HasFlag(IndexEntryFieldType.Tuple))
            {
                _longOffset = MemoryMarshal.Read<int>(_buffer[(offset + sizeof(int))..]);
                _doubleOffset = (offset + 2 * sizeof(int)); // Skip the pointer from sequences and longs.

                if (Type.HasFlag(IndexEntryFieldType.HasNulls))
                {
                    int nullBitStreamSize = Count / (sizeof(long) * 8) + Count % (sizeof(long) * 8) == 0 ? 0 : 1;
                    _spanTableOffset = _nullTableOffset + nullBitStreamSize; // Point after the null table.                             
                }
                else
                {
                    _spanTableOffset = _nullTableOffset;
                }

                _spanOffset = (_doubleOffset + Count * sizeof(double)); // Skip over the doubles array, and now we are sitting at the start of the sequences table.
            }
            else
            {
                _doubleOffset = 0;                
                _longOffset = 0;

                if (Type.HasFlag(IndexEntryFieldType.HasNulls))
                {
                    int nullBitStreamSize = Count / (sizeof(long) * 8) + Count % (sizeof(long) * 8) == 0 ? 0 : 1;
                    _spanTableOffset = _nullTableOffset + nullBitStreamSize; // Point after the null table.         
                }
                else
                {
                    _spanTableOffset = _nullTableOffset;
                }

                _spanOffset = (offset + sizeof(int));
            }

            _currentIdx = -1;
            IsValid = true;
        }

        public bool IsNull
        {
            get
            {
                if (!Type.HasFlag(IndexEntryFieldType.HasNulls))
                    return false;

                unsafe
                {
                    byte* nullTablePtr = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(_buffer));
                    return PtrBitVector.GetBitInPointer(nullTablePtr + _nullTableOffset, _currentIdx);
                }
            }
        }

        public bool IsEmpty => !IsNull && Count == 0;

        public ReadOnlySpan<byte> Sequence
        {
            get
            {
                if (_currentIdx >= Count)
                    throw new IndexOutOfRangeException();

                int stringLength = VariableSizeEncoding.Read<int>(_buffer, out _, _spanTableOffset);
                return _buffer.Slice(_spanOffset, stringLength);
            }
        }

        public long Long
        {
            get
            {
                if (!IsTuple)
                    throw new InvalidOperationException();
                if (_currentIdx >= Count)
                    throw new IndexOutOfRangeException();

                return VariableSizeEncoding.Read<long>(_buffer, out _, _longOffset);
            }
        }

        public double Double
        {
            get
            {
                if (!IsTuple)
                    throw new InvalidOperationException();
                if (_currentIdx >= Count)
                    throw new IndexOutOfRangeException();

                return Unsafe.ReadUnaligned<double>(ref MemoryMarshal.GetReference(_buffer[_doubleOffset..]));
            }
        }

        public bool ReadNext()
        {
            _currentIdx++;
            if (_currentIdx >= Count)
                return false;

            if (_currentIdx > 0)
            {
                // This two have fixed size. 
                _spanOffset += VariableSizeEncoding.Read<int>(_buffer, out var length, _spanTableOffset);
                _spanTableOffset += length;

                if (IsTuple)
                {
                    // This is a tuple, so we update these too.
                    _doubleOffset += sizeof(double);

                    VariableSizeEncoding.Read<long>(_buffer, out length, _longOffset);
                    _longOffset += length;
                }
            }


            return true;
        }
    }

    // The rationale to use ref structs is not allowing to copy those around so easily. They should be cheap to construct
    // and cheaper to use. 
    public unsafe readonly ref struct IndexEntryReader
    {
        private const int Invalid = unchecked((int)0xFFFF_FFFF);

        private readonly Span<byte> _buffer;

        public int Length => (int)MemoryMarshal.Read<uint>(_buffer);

        public IndexEntryReader(Span<byte> buffer)
        {
            _buffer = buffer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static (int, bool) GetMetadataFieldLocation(Span<byte> buffer, int field)
        {
            ref var header = ref MemoryMarshal.AsRef<IndexEntryHeader>(buffer);

            ushort knownFieldsCount = (ushort)(header.KnownFieldCount >> 2);
            byte encodeSize = (byte)(header.KnownFieldCount & 0b11);

            int locationOffset = buffer.Length - (knownFieldsCount * encodeSize) + field * encodeSize;

            int offset;
            bool isTyped;

            if (encodeSize == 1)
            {
                offset = Unsafe.ReadUnaligned<byte>(ref buffer[locationOffset]);
                if (offset == 0xFF)
                    goto Fail;
                isTyped = (offset & 0x80) != 0;
                offset &= ~0x80;
                goto End;
            }

            if (encodeSize == 2)
            {
                offset = Unsafe.ReadUnaligned<ushort>(ref buffer[locationOffset]);
                if (offset == 0xFFFF)
                    goto Fail;
                isTyped = (offset & 0x8000) != 0;
                offset &= ~0x8000;
                goto End;
            }

            if (encodeSize == 4)
            {
                offset = (int)Unsafe.ReadUnaligned<uint>(ref buffer[locationOffset]);
                if (offset == unchecked((int)0xFFFF_FFFF))
                    goto Fail;
                isTyped = (offset & Constants.IndexWriter.KnownFieldMask) != 0;
                offset &= ~Constants.IndexWriter.KnownFieldMask;
                goto End;
            }

            Fail:
            return (Invalid, false);

            End:
            return (offset, isTyped);
        }


        public bool Read<T>(int field, out IndexEntryFieldType type, out T value) where T : unmanaged
        {
            var (intOffset, isTyped) = GetMetadataFieldLocation(_buffer, field);
            if (intOffset == Invalid)
                goto Fail;                        

            if (isTyped)
            {
                type = Unsafe.ReadUnaligned<IndexEntryFieldType>(ref _buffer[intOffset]);
                if (type == IndexEntryFieldType.Null)
                    goto IsNull;                                

                intOffset += sizeof(IndexEntryFieldType);

                if ((type & IndexEntryFieldType.Tuple) != 0)
                {
                    var lResult = VariableSizeEncoding.Read<long>(_buffer, out int length, intOffset);
                    if (typeof(long) == typeof(T))
                    {
                        value = (T)(object)lResult;
                        return true;
                    }

                    if (typeof(ulong) == typeof(T))
                    {
                        value = (T)(object)(ulong)lResult;
                        return true;
                    }

                    if (typeof(int) == typeof(T))
                    {
                        value = (T)(object)(int)lResult;
                        return lResult is >= int.MinValue and <= int.MaxValue;
                    }

                    if (typeof(uint) == typeof(T))
                    {
                        value = (T)(object)(uint)lResult;
                        return lResult is >= 0 and <= uint.MaxValue;
                    }

                    if (typeof(short) == typeof(T))
                    {
                        value = (T)(object)(short)lResult;
                        return lResult is >= short.MinValue and <= short.MaxValue;
                    }

                    if (typeof(ushort) == typeof(T))
                    {
                        value = (T)(object)(ushort)lResult;
                        return lResult is >= 0 and <= ushort.MaxValue;
                    }

                    if (typeof(byte) == typeof(T))
                    {
                        value = (T)(object)(byte)lResult;
                        return lResult is >= 0 and <= byte.MaxValue;
                    }

                    if (typeof(sbyte) == typeof(T))
                    {
                        value = (T)(object)(sbyte)lResult;
                        return lResult is >= sbyte.MinValue and <= sbyte.MaxValue;
                    }

                    intOffset += length;

                    if (typeof(T) == typeof(double))
                    {
                        value = (T)(object)Unsafe.ReadUnaligned<double>(ref _buffer[intOffset]);
                        return true;
                    }

                    if (typeof(T) == typeof(double))
                    {
                        var dResult = Unsafe.ReadUnaligned<double>(ref _buffer[intOffset]);
                        value = (T)(object)(float)dResult;
                        return true;
                    }

                    throw new NotSupportedException($"The type {nameof(T)} is unsupported.");
                }

                if ((type & IndexEntryFieldType.Extended) != 0)
                {
                    //<type><lat><long><amount_of_geohashes><pointer_to_string_length_table><geohash>
                    ExtendedEntryFieldType extendedEntryFieldType = (ExtendedEntryFieldType)VariableSizeEncoding.Read<byte>(_buffer, out length, intOffset);
                    intOffset += length;

                    if (extendedEntryFieldType.HasFlag(ExtendedEntryFieldType.SpatialPoint))
                    {
                        if (typeof(T) == typeof((double, double)))
                        {
                            var latitude = Unsafe.ReadUnaligned<double>(ref _buffer[intOffset]);
                            intOffset += sizeof(double);
                            var longitude = Unsafe.ReadUnaligned<double>(ref _buffer[intOffset]);
                            value = (T)(object)(latitude, longitude);
                            return true;
                        }
                    }
                    else
                    {
                        throw new NotImplementedException($"{extendedEntryFieldType} not implemented yet.");
                    }
                }

                throw new NotSupportedException($"The type {nameof(T)} is unsupported.");
            }

            Fail:
            Unsafe.SkipInit(out value);
            type = IndexEntryFieldType.Invalid;
            return false;
        IsNull:
            Unsafe.SkipInit(out value);
            type = IndexEntryFieldType.Null;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Read<T>(int field, out T value) where T : unmanaged
        {
            return Read(field, out var _, out value);
        }


        public IndexEntryFieldType GetFieldType(int field, out int intOffset)
        {
            (intOffset, var isTyped) = GetMetadataFieldLocation(_buffer, field);
            if (intOffset == Invalid)
                return IndexEntryFieldType.Invalid;

            if (isTyped)
            {
                var type =  Unsafe.ReadUnaligned<IndexEntryFieldType>(ref _buffer[intOffset]);
                intOffset += Unsafe.SizeOf<IndexEntryFieldType>();
                return type;
            }

            return IndexEntryFieldType.Simple;
        }

        public ExtendedEntryFieldType GetSpecialFieldType(ref int intOffset)
        {
            var specialType = (ExtendedEntryFieldType)VariableSizeEncoding.Read<byte>(_buffer, out var length, intOffset);
            return specialType;
        }

        //<type:byte><extended_type:byte><amount_of_items:int><geohashLevel:int><geohash_ptr:int>
        //<longitudes_ptr:int><latitudes_list:double[]><longtitudes_list:double[]><geohashes_list:bytes[]>
        public SpatialPointFieldIterator ReadManySpatialPoint(int field)
        {
            var (intOffset, isTyped) = GetMetadataFieldLocation(_buffer, field);
            if (intOffset == Invalid)
                return new SpatialPointFieldIterator();

            return new SpatialPointFieldIterator(_buffer, intOffset);
        }
        
        public IndexEntryFieldIterator ReadMany(int field)
        {
            var (intOffset, isTyped) = GetMetadataFieldLocation(_buffer, field);
            if (intOffset == Invalid)
                return new IndexEntryFieldIterator(IndexEntryFieldType.Invalid);

            if (isTyped)
            {
                var type = (IndexEntryFieldType)VariableSizeEncoding.Read<byte>(_buffer, out var length, intOffset);

                if ((type & IndexEntryFieldType.Extended) != 0)
                {
                    intOffset += length;

                    var specialType = (ExtendedEntryFieldType)VariableSizeEncoding.Read<byte>(_buffer, out length, intOffset);
                    // We're moving our pointer to Geohash list. This is what we actually want to index.

                    if ((specialType & ExtendedEntryFieldType.SpatialPoint) != 0)
                    {
                        intOffset += length + 2 * sizeof(double);
                    }
                    else
                    {
                        throw new NotImplementedException("wkt arent done yet");
                    }
                }

                return new IndexEntryFieldIterator(_buffer, intOffset);
            }


            throw new ArgumentException($"Field with index number '{field}' is untyped.");
        }


        public bool TryReadMany(int field, out IndexEntryFieldIterator iterator)
        {
            var (intOffset, isTyped) = GetMetadataFieldLocation(_buffer, field);
            if (intOffset == Invalid)
            {
                iterator = default;
                return false;
            }

            if (!isTyped)
            {
                iterator = default;
                return false;
            }


            var type = (IndexEntryFieldType)VariableSizeEncoding.Read<byte>(_buffer, out var length, intOffset);

            if ((type & IndexEntryFieldType.Extended) != 0)
            {
                intOffset += length;

                var specialType = (ExtendedEntryFieldType)VariableSizeEncoding.Read<byte>(_buffer, out length, intOffset);
                // We're moving our pointer to Geohash list. This is what we actually want to index.

                if ((specialType & ExtendedEntryFieldType.SpatialPoint) != 0)
                {
                    intOffset += length + 2 * sizeof(double);
                }
                else
                {
                    throw new NotImplementedException("wkt arent done yet");
                }
            }

            iterator = new IndexEntryFieldIterator(_buffer, intOffset);

            return iterator.IsValid;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Read(int field, out Span<byte> value, int elementIdx = 0)
        {
            bool result = Read(field, out IndexEntryFieldType type, out value, elementIdx);

            // When we dont ask about the type, we dont usually care about the empty lists either.
            // The behavior in those cases is that trying to access an element by index when the list is empty
            // should return false (as in failure). 
            if (type.HasFlag(IndexEntryFieldType.EmptyList))
                return false;

            return result;
        }

        public bool Read(int field, out IndexEntryFieldType type, out Span<byte> value, int elementIdx = 0)
        {
            var (intOffset, isTyped) = GetMetadataFieldLocation(_buffer, field);
            if (intOffset == Invalid)
                goto Fail;

            int stringLength = 0;

            if (isTyped)
            {                               
                type = Unsafe.ReadUnaligned<IndexEntryFieldType>(ref _buffer[intOffset]);
                if (type == IndexEntryFieldType.Null)
                {
                    if (elementIdx == 0)
                        goto IsNull;
                    else
                        goto FailNull;
                }
                else if (type.HasFlag(IndexEntryFieldType.EmptyList))
                {
                    goto EmptyList;
                }

                intOffset += sizeof(IndexEntryFieldType);
                if (type.HasFlag(IndexEntryFieldType.List))
                {                    
                    int totalElements = VariableSizeEncoding.Read<ushort>(_buffer, out int length, intOffset);
                    if (elementIdx >= totalElements)
                        goto Fail;

                    intOffset += length;
                    var spanTableOffset = Unsafe.ReadUnaligned<int>(ref _buffer[intOffset]);
                    if (type.HasFlag(IndexEntryFieldType.Tuple))
                    {
                        intOffset += 2 * sizeof(int) + totalElements * sizeof(double);
                    }
                    else
                    {
                        intOffset += sizeof(int);
                    }

                    if (type.HasFlag(IndexEntryFieldType.HasNulls))
                    {
                        byte* nullTablePtr = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(_buffer));
                        if (PtrBitVector.GetBitInPointer(nullTablePtr + spanTableOffset, elementIdx) == true)
                            goto HasNull;

                        int nullBitStreamSize = totalElements / (sizeof(long) * 8) + totalElements % (sizeof(long) * 8) == 0 ? 0 : 1;
                        spanTableOffset += nullBitStreamSize; // Point after the null table.                             
                    }

                    // Skip over the number of entries and jump to the string location.
                    for (int i = 0; i < elementIdx; i++)
                    {
                        stringLength = VariableSizeEncoding.Read<int>(_buffer, out length, spanTableOffset);
                        intOffset += stringLength;
                        spanTableOffset += length;
                    }

                    stringLength = VariableSizeEncoding.Read<int>(_buffer, out length, spanTableOffset);
                }
                else if ((type & IndexEntryFieldType.Raw) != 0)
                {
                    if (type.HasFlag(IndexEntryFieldType.HasNulls))
                    {
                        var spanTableOffset = Unsafe.ReadUnaligned<int>(ref _buffer[intOffset]);
                        byte* nullTablePtr = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(_buffer));
                        if (PtrBitVector.GetBitInPointer(nullTablePtr + spanTableOffset, elementIdx) == true)
                            goto HasNull;
                    }

                    stringLength = VariableSizeEncoding.Read<int>(_buffer, out int readOffset, intOffset);
                    intOffset += readOffset;
                    type = IndexEntryFieldType.Raw;
                }
                else if ((type & IndexEntryFieldType.Tuple) != 0)
                {
                    if (type.HasFlag(IndexEntryFieldType.HasNulls))
                    {
                        var spanTableOffset = Unsafe.ReadUnaligned<int>(ref _buffer[intOffset]);
                        byte* nullTablePtr = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(_buffer));
                        if (PtrBitVector.GetBitInPointer(nullTablePtr + spanTableOffset, elementIdx) == true)
                            goto HasNull;
                    }

                    VariableSizeEncoding.Read<long>(_buffer, out int length, intOffset); // Skip
                    intOffset += length;
                    Unsafe.ReadUnaligned<double>(ref _buffer[intOffset]);
                    intOffset += sizeof(double);

                    stringLength = VariableSizeEncoding.Read<int>(_buffer, out int readOffset, intOffset);
                    intOffset += readOffset;
                }
                else if ((type & IndexEntryFieldType.Extended) != 0)
                {
                    var extendedType = VariableSizeEncoding.Read<byte>(_buffer, out length, intOffset); // Skip
                    intOffset += length + 2 * sizeof(double);
                    stringLength = VariableSizeEncoding.Read<byte>(_buffer, out length, intOffset);
                    intOffset += length;
                    type = IndexEntryFieldType.Extended;
                }
            }
            else
            {
                stringLength = VariableSizeEncoding.Read<int>(_buffer, out int readOffset, intOffset);
                intOffset += readOffset;
                type = IndexEntryFieldType.Simple;
            }

            value = _buffer.Slice(intOffset, stringLength);
            return true;

        EmptyList:
            value = Span<byte>.Empty;
            return true;
        HasNull:
            type = IndexEntryFieldType.HasNulls;
            value = Span<byte>.Empty;
            return true;
        IsNull:
            value = Span<byte>.Empty;
            type = IndexEntryFieldType.Null;
            return true;
        Fail:
            value = Span<byte>.Empty;
            type = IndexEntryFieldType.Invalid;
            return false;
        FailNull:
            throw new InvalidOperationException("Cannot request an internal value when the field is null.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Read(int field, out long longValue, out double doubleValue, out Span<byte> sequenceValue)
        {
            bool result = Read(field, out var type, out longValue, out doubleValue, out sequenceValue);

            // When we dont ask about the type, we dont usually care about the empty lists either.
            // The behavior in those cases is that trying to access an element by index when the list is empty
            // should return false (as in failure). 
            if (type.HasFlag(IndexEntryFieldType.EmptyList))
                return false;

            return result;
        }

        public unsafe bool Read(int field, out IndexEntryFieldType type, out long longValue, out double doubleValue, out Span<byte> sequenceValue)
        {
            var (intOffset, isTyped) = GetMetadataFieldLocation(_buffer, field);
            if (intOffset == Invalid)
                goto Fail;

            if (isTyped)
            {
                type = Unsafe.ReadUnaligned<IndexEntryFieldType>(ref _buffer[intOffset]);
                if (type == IndexEntryFieldType.Null)
                    goto IsNull;
                
                intOffset += sizeof(IndexEntryFieldType);
                
                if (type.HasFlag(IndexEntryFieldType.Tuple))
                {
                    int stringLength;
                    if (type.HasFlag(IndexEntryFieldType.List))
                    {
                        int totalElements = VariableSizeEncoding.Read<ushort>(_buffer, out int length, intOffset);
                        intOffset += length;

                        var spanOffset = intOffset + 2 * sizeof(int) + totalElements * sizeof(double);

                        var spanTableOffset = Unsafe.ReadUnaligned<int>(ref _buffer[intOffset]);
                        intOffset += sizeof(int);
                        var longTableOffset = Unsafe.ReadUnaligned<int>(ref _buffer[intOffset]);
                        intOffset += sizeof(int);

                        if (type.HasFlag(IndexEntryFieldType.HasNulls))
                        {
                            byte* nullTablePtr = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(_buffer));
                            if (PtrBitVector.GetBitInPointer(nullTablePtr + spanTableOffset, 0) == true)
                                goto HasNull;

                            int nullBitStreamSize = totalElements / (sizeof(long) * 8) + totalElements % (sizeof(long) * 8) == 0 ? 0 : 1;
                            spanTableOffset += nullBitStreamSize; // Point after the null table.                             
                        }

                        doubleValue = Unsafe.ReadUnaligned<double>(ref _buffer[intOffset]);
                        longValue = VariableSizeEncoding.Read<long>(_buffer, out length, longTableOffset); // Read
                        stringLength = VariableSizeEncoding.Read<ushort>(_buffer, out _, spanTableOffset); // Read

                        // Jump to the string location
                        intOffset = spanOffset;
                    }
                    else
                    {
                        if (type.HasFlag(IndexEntryFieldType.HasNulls))
                            goto HasNull;

                        longValue = VariableSizeEncoding.Read<long>(_buffer, out int length, intOffset); // Read
                        intOffset += length;
                        doubleValue = Unsafe.ReadUnaligned<double>(ref _buffer[intOffset]);
                        intOffset += sizeof(double);

                        stringLength = VariableSizeEncoding.Read<ushort>(_buffer, out length, intOffset); // Read
                        intOffset += length;
                    }

                    sequenceValue = _buffer.Slice(intOffset, stringLength);
                    return true;
                }
            }

        Fail:
            Unsafe.SkipInit(out longValue);
            Unsafe.SkipInit(out doubleValue);
            sequenceValue = Span<byte>.Empty;
            type = IndexEntryFieldType.Invalid;
            return false;

        HasNull:
            Unsafe.SkipInit(out longValue);
            Unsafe.SkipInit(out doubleValue);
            sequenceValue = Span<byte>.Empty;
            type = IndexEntryFieldType.HasNulls;
            return true;
        IsNull:
            Unsafe.SkipInit(out longValue);
            Unsafe.SkipInit(out doubleValue);
            sequenceValue = Span<byte>.Empty;
            type = IndexEntryFieldType.Null;
            return true;
        }


        public string DebugDump(Dictionary<Slice, int> knownFields)
        {
            string result = string.Empty;
            foreach (var (name, field) in knownFields)
            {
                var type = GetFieldType(field, out _);
                if (type.HasFlag(IndexEntryFieldType.Simple))
                {
                    Read(field, out var value);
                    result += $"{name}: {Encodings.Utf8.GetString(value)}{Environment.NewLine}";
                }
                else if (type == IndexEntryFieldType.Invalid)
                {
                    result += $"{name}: null{Environment.NewLine}";
                }
                else if (type.HasFlag(IndexEntryFieldType.List))
                {
                    var iterator = this.ReadMany(field);

                    result = string.Empty;
                    while (iterator.ReadNext())
                    {
                        result += $"{name}: {Encodings.Utf8.GetString(iterator.Sequence)},";
                    }

                    result += $"{name}: [{result[0..^1]}]{Environment.NewLine}";
                }
            }


            return $"{{{Environment.NewLine}{result}{Environment.NewLine}}}";
        }

        public IndexEntryFieldIterator ReadGeohashs(int tokenField)
        {
            throw new NotImplementedException();
        }
    }
}
