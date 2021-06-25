using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Server.Compression;
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
    public enum IndexEntryFieldType : byte
    {
        None = 1,
        
        Tuple = 1 << 2,
        List = 1 << 3,
        Invalid = 1 << 6,
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
    public ref struct IndexEntryWriter
    {
        private static int Invalid = unchecked(~0);
        private static ushort MaxDynamicFields = byte.MaxValue;

        internal static ReadOnlySpan<int> IsTypedMask => new(new int[]
        {
            0,                          // Unused
            0x80,                       // Byte
            0x8000,                     // Short
            0,                          // Unused
            unchecked((int)0x80000000)  // Int
        });

        private static int LocationMask = 0x7FFF_FFFF;

        private readonly Dictionary<Slice, int> _knownFields;

        // The usable part of the buffer, the metadata space will be removed from the usable space.
        private readonly Span<byte> _buffer;        
        
        // Temporary location for the pointers, these will eventually be encoded based on how big they are.
        // <256 bytes we could use a single byte
        // <65546 bytes we could use a single ushort
        // for the rest we will use a uint.
        private readonly Span<int> _knownFieldsLocations;

        // Current pointer.        
        private int _dataIndex;
        
        // Dynamic fields will use a full integer to store the pointer location at the metadata table. They are supposed to be rare 
        // so we wont even try to make the process more complex just to deal with them efficienly.
        private int _dynamicFieldIndex; 

        private static readonly Dictionary<Slice, int> Empty = new();




        public IndexEntryWriter(Span<byte> buffer, Dictionary<Slice, int> knownFields = null)
        {
            // TODO: For now we will assume that the max size of an index entry is 32Kb, revisit this...
            _knownFields = knownFields ?? Empty;

            int knownFieldMetadataSize = _knownFields.Count * sizeof(uint);
            int knownFieldsCount = _knownFields.Count;
            _knownFieldsLocations = MemoryMarshal.Cast<byte, int>(buffer[^knownFieldMetadataSize..]);
            _knownFieldsLocations.Fill(Invalid); // We prepare the table in order to avoid tracking the writes. 

            _buffer = buffer[..^knownFieldMetadataSize];
            _dynamicFieldIndex = 0;
            _dataIndex = Unsafe.SizeOf<IndexEntryHeader>();
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

        public void Write(int field, ReadOnlySpan<byte> value, long longValue, double doubleValue)
        {
            Debug.Assert(field < _knownFields.Count);
            Debug.Assert(_knownFieldsLocations[field] == Invalid);

            if (value.Length == 0)
                return;

            int dataLocation = _dataIndex;

            // Write known field pointer.
            _knownFieldsLocations[field] = dataLocation | IsTypedMask[sizeof(int)];

            // Write the tuple information. 
            dataLocation += VariableSizeEncoding.Write(_buffer, (byte)IndexEntryFieldType.Tuple, dataLocation);
            dataLocation += VariableSizeEncoding.Write(_buffer, longValue, dataLocation);
            Unsafe.WriteUnaligned(ref _buffer[dataLocation], doubleValue);
            dataLocation += sizeof(double);
            dataLocation += VariableSizeEncoding.Write(_buffer, value.Length, dataLocation);

            // Copy the actual string data. 
            ref var src = ref Unsafe.AsRef(value[0]);
            ref var dest = ref _buffer[dataLocation];
            Unsafe.CopyBlock(ref dest, ref src, (uint)value.Length);

            _dataIndex = dataLocation + value.Length;
        }

        public void Write<TEnumerator>(int field, TEnumerator values) where TEnumerator : IReadOnlySpanEnumerator
        {
            Debug.Assert(field < _knownFields.Count);
            Debug.Assert(_knownFieldsLocations[field] == Invalid);

            if (values.Length == 0)
                return;

            int dataLocation = _dataIndex;

            // Write known field pointer.
            _knownFieldsLocations[field] = dataLocation | IsTypedMask[sizeof(int)];

            // Write the list metadata information. 
            dataLocation += VariableSizeEncoding.Write(_buffer, (byte)IndexEntryFieldType.List, dataLocation);
            dataLocation += VariableSizeEncoding.Write(_buffer, values.Length, dataLocation); // Size of list.

            // Prepare the location to store the pointer where the table of the strings will be (after writing the strings).
            var stringPtrTableLocation = _buffer.Slice(dataLocation, sizeof(int));
            dataLocation += sizeof(int);

            // We start to write the strings in a place we know where it is from implicit positioning...
            // 4b 
            int[] stringLengths = ArrayPool<int>.Shared.Rent(values.Length);
            for (int i = 0; i < values.Length; i++)
            {
                var value = values[i];
                value.CopyTo(_buffer[dataLocation..]);
                dataLocation += value.Length;

                stringLengths[i] = value.Length;
            }

            MemoryMarshal.Write(stringPtrTableLocation, ref dataLocation);
            dataLocation += VariableSizeEncoding.WriteMany<int>(_buffer, stringLengths[..values.Length], dataLocation);
            ArrayPool<int>.Shared.Return(stringLengths);

            _dataIndex = dataLocation;
        }

        public void Write(int field, IReadOnlySpanEnumerator values, ReadOnlySpan<long> longValues, ReadOnlySpan<double> doubleValues)
        {
            Debug.Assert(field < _knownFields.Count);
            Debug.Assert(_knownFieldsLocations[field] == Invalid);
            Debug.Assert(values.Length == longValues.Length && values.Length == doubleValues.Length);

            if (values.Length == 0)
                return;

            int dataLocation = _dataIndex;

            // Write known field pointer.
            _knownFieldsLocations[field] = dataLocation | IsTypedMask[sizeof(int)];

            // Write the list metadata information. 
            dataLocation += VariableSizeEncoding.Write(_buffer, (byte)(IndexEntryFieldType.List | IndexEntryFieldType.Tuple), dataLocation);
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

            MemoryMarshal.Write(stringPtrTableLocation, ref dataLocation);
            dataLocation += VariableSizeEncoding.WriteMany<int>(_buffer, stringLengths[..values.Length], dataLocation);

            // Write the long values
            MemoryMarshal.Write(longPtrLocation, ref dataLocation);
            dataLocation += VariableSizeEncoding.WriteMany(_buffer, longValues, pos: dataLocation);

            _dataIndex = dataLocation;
        }

        public void Write(Slice name, ReadOnlySpan<byte> value)
        {
            if (_knownFields.TryGetValue(name, out int field))
            {
                Write(field, value);
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
            if (_knownFields.TryGetValue(name, out int field))
            {
                Write(field, value, longValue, doubleValue);
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
            for ( int i = 0; i < _knownFieldsLocations.Length; i++)
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
                    location |= IsTypedMask[offset];

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
        public readonly int Count;
        private int _currentIdx;

        private readonly ReadOnlySpan<byte> _buffer;
        private int _spanOffset;
        private int _spanTableOffset;
        private int _longOffset;
        private int _doubleOffset;
        private readonly bool IsTuple => _doubleOffset != 0;


        internal IndexEntryFieldIterator(IndexEntryFieldType type)
        {
            Debug.Assert(type == IndexEntryFieldType.Invalid);
            Type = IndexEntryFieldType.Invalid;
            Count = 0;
            _buffer = ReadOnlySpan<byte>.Empty;

            Unsafe.SkipInit(out _currentIdx);
            Unsafe.SkipInit(out _spanTableOffset);
            Unsafe.SkipInit(out _spanOffset);
            Unsafe.SkipInit(out _longOffset);
            Unsafe.SkipInit(out _doubleOffset);
        }

        public IndexEntryFieldIterator(ReadOnlySpan<byte> buffer, int offset)
        {
            _buffer = buffer;
            Type = (IndexEntryFieldType)VariableSizeEncoding.Read<byte>(_buffer, out var length, offset);
            offset += length;

            if (((byte)Type & (byte)IndexEntryFieldType.List) == 0)
                throw new FormatException("Type is not a list.");

            Count = VariableSizeEncoding.Read<ushort>(_buffer, out length, offset);

            offset += length;

            _spanTableOffset = MemoryMarshal.Read<int>(_buffer[offset..]);
            if (((byte)Type & (byte)IndexEntryFieldType.Tuple) != 0)
            {
                _longOffset = MemoryMarshal.Read<int>(_buffer[(offset+sizeof(int))..]);
                _doubleOffset = (offset + 2 * sizeof(int));
                _spanOffset = (_doubleOffset + Count * sizeof(double));
            }
            else
            {
                _doubleOffset = 0;
                _spanOffset = (offset + sizeof(int));
                _longOffset = 0;
            }

            _currentIdx = -1;
        }

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
    public readonly ref struct IndexEntryReader
    {
        private const int Invalid = unchecked((int)0xFFFF_FFFF);

        internal static ReadOnlySpan<int> IsInvalidMask => new(new int[]
        {            
            0,                          // Unused
            0xFF,                       // Byte
            0xFFFF,                     // Short
            0,                          // Unused
            unchecked((int)0xFFFF_FFFF) // Int
        });

        internal static ReadOnlySpan<int> LocationMask => new(new int[]
        {
            ~0,                          // Unused
            ~0x80,                       // Byte
            ~0x8000,                     // Short
            ~0,                          // Unused
            ~unchecked((int)0x80000000)  // Int
        });

        private readonly Span<byte> _buffer;

        public int Length => (int)MemoryMarshal.Read<uint>(_buffer);

        public IndexEntryReader(Span<byte> buffer)
        {
            _buffer = buffer;
        }


        internal static (int, bool) GetMetadataFieldLocation(Span<byte> buffer, int field)
        {
            ref var header = ref MemoryMarshal.AsRef<IndexEntryHeader>(buffer);            
            
            ushort knownFieldsCount = (ushort)(header.KnownFieldCount >> 2);
            byte encodeSize = (byte)(header.KnownFieldCount & 0b11);

            int locationOffset = buffer.Length - (knownFieldsCount * encodeSize) + field * encodeSize;

            int offset;
            bool isTyped;
            switch (encodeSize)
            {
                case 4:
                    offset = (int)Unsafe.ReadUnaligned<uint>(ref buffer[locationOffset]);
                    if (offset == IsInvalidMask[4]) goto Fail;
                    isTyped = (offset & IndexEntryWriter.IsTypedMask[4]) != 0;
                    offset &= LocationMask[4];
                    break;
                case 2:
                    offset = Unsafe.ReadUnaligned<ushort>(ref buffer[locationOffset]);
                    if (offset == IsInvalidMask[2]) goto Fail;
                    isTyped = (offset & IndexEntryWriter.IsTypedMask[2]) != 0;
                    offset &= LocationMask[2];                    
                    break;
                case 1:
                    offset = Unsafe.ReadUnaligned<byte>(ref buffer[locationOffset]);
                    if (offset == IsInvalidMask[1]) goto Fail;
                    isTyped = (offset & IndexEntryWriter.IsTypedMask[1]) != 0;
                    offset &= LocationMask[1];
                    break;
                default:
                    goto Fail;
            }

            return (offset , isTyped);

            Fail: return (Invalid, false);
        }

        public bool Read<T>(int field, out T value) where T : unmanaged
        {
            var (intOffset, isTyped) = GetMetadataFieldLocation(_buffer, field);
            if (intOffset == Invalid)
                goto Fail;

            if (isTyped)
            {
                byte type = VariableSizeEncoding.Read<byte>(_buffer, out var length, intOffset);
                intOffset += length;

                if ((type & (byte)IndexEntryFieldType.Tuple) != 0)
                {
                    var lResult = VariableSizeEncoding.Read<long>(_buffer, out length, intOffset);
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
                throw new NotSupportedException($"The type {nameof(T)} is unsupported.");
            }

            Fail: Unsafe.SkipInit(out value);
            return false;
        }

        public IndexEntryFieldType GetFieldType(int field)
        {
            var (intOffset, isTyped) = GetMetadataFieldLocation(_buffer, field);
            if (intOffset == Invalid)
                return IndexEntryFieldType.Invalid;

            if (isTyped)
            {
                return (IndexEntryFieldType)VariableSizeEncoding.Read<byte>(_buffer, out var length, intOffset);
            }

            return IndexEntryFieldType.None;
        }

        public IndexEntryFieldIterator ReadMany(int field)
        {
            var (intOffset, isTyped) = GetMetadataFieldLocation(_buffer, field);
            if (intOffset == Invalid)
                return new IndexEntryFieldIterator(IndexEntryFieldType.Invalid);

            if (!isTyped)
                throw new ArgumentException($"Field with index number '{field}' is untyped.");

            return new IndexEntryFieldIterator(_buffer, intOffset);
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

            iterator = new IndexEntryFieldIterator(_buffer, intOffset);
            return true;
        }

        public bool Read(int field, out ReadOnlySpan<byte> value, int elementIdx = 0)
        {
            var (intOffset, isTyped) = GetMetadataFieldLocation(_buffer, field);
            if (intOffset == Invalid)
            {
                value = ReadOnlySpan<byte>.Empty;
                return false;
            }

            int stringLength = 0;

            if (isTyped)
            {
                byte type = VariableSizeEncoding.Read<byte>(_buffer, out var length, intOffset);
                intOffset += length;
                if ((type & (byte)IndexEntryFieldType.List) != 0)
                {
                    int totalSize = VariableSizeEncoding.Read<ushort>(_buffer, out length, intOffset);
                    if (elementIdx >= totalSize)
                    {
                        value = ReadOnlySpan<byte>.Empty;
                        return false;
                    }
                    
                    intOffset += length;
                    var spanTableOffset = MemoryMarshal.Read<int>(_buffer[intOffset..]);
                    if ((type & (byte)IndexEntryFieldType.Tuple) != 0)
                    {
                        intOffset += 2 * sizeof(int) + totalSize * sizeof(double);
                    }
                    else
                    {
                        intOffset += sizeof(int);
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
                else if ((type & (byte)IndexEntryFieldType.Tuple) != 0)
                {
                    VariableSizeEncoding.Read<long>(_buffer, out length, intOffset); // Skip
                    intOffset += length;
                    Unsafe.ReadUnaligned<double>(ref _buffer[intOffset]);
                    intOffset += sizeof(double);

                    stringLength = VariableSizeEncoding.Read<int>(_buffer, out int readOffset, intOffset);
                    intOffset += readOffset;
                }
            }
            else
            {
                stringLength = VariableSizeEncoding.Read<int>(_buffer, out int readOffset, intOffset);
                intOffset += readOffset;
            }

            value = _buffer.Slice(intOffset, stringLength);
            return true;
        }

        public bool Read(int field, out long longValue, out double doubleValue, out ReadOnlySpan<byte> sequenceValue)
        {
            var (intOffset, isTyped) = GetMetadataFieldLocation(_buffer, field);
            if (intOffset == Invalid)
                goto Fail;

            if (isTyped)
            {
                byte type = VariableSizeEncoding.Read<byte>(_buffer, out var length, intOffset);
                intOffset += length;
                
                if ((type & (byte)IndexEntryFieldType.Tuple) != 0)
                {
                    int stringLength;
                    if ((type & (byte)IndexEntryFieldType.List) != 0)
                    {
                        int totalElements = VariableSizeEncoding.Read<ushort>(_buffer, out length, intOffset);
                        intOffset += length;

                        var spanOffset = intOffset + 2 * sizeof(int) + totalElements * sizeof(double);

                        var spanTableOffset = MemoryMarshal.Read<int>(_buffer[intOffset..]);
                        intOffset += sizeof(int);
                        var longTableOffset = MemoryMarshal.Read<int>(_buffer[intOffset..]);
                        intOffset += sizeof(int);

                        doubleValue = Unsafe.ReadUnaligned<double>(ref _buffer[intOffset]);
                        longValue = VariableSizeEncoding.Read<long>(_buffer, out length, longTableOffset); // Read

                        stringLength = VariableSizeEncoding.Read<ushort>(_buffer, out _, spanTableOffset); // Read

                        // Jump to the string location
                        intOffset = spanOffset;
                    }
                    else
                    {
                        longValue = VariableSizeEncoding.Read<long>(_buffer, out length, intOffset); // Read
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

            Fail:  Unsafe.SkipInit(out longValue);
            Unsafe.SkipInit(out doubleValue);
            sequenceValue = ReadOnlySpan<byte>.Empty;
            return false;
        }


        public string DebugDump(Dictionary<Slice, int> knownFields)
        {
            string result = string.Empty;
            foreach (var (name, field) in knownFields)
            {
                var type = GetFieldType(field);
                if (type.HasFlag(IndexEntryFieldType.None))
                {
                    Read(field, out var value);
                    result += $"{name}: {Encodings.Utf8.GetString(value)}{Environment.NewLine}";
                }
                else if (type.HasFlag(IndexEntryFieldType.Invalid))
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
    }
}
