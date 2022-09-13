using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Compression;
using Sparrow.Server.Compression;
using Sparrow.Utils;

namespace Corax;

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
public ref struct IndexEntryReader
{
    private const int Invalid = unchecked((int)0xFFFF_FFFF);
    private readonly Span<byte> _buffer;
    private int _lastFieldAccessed;
    private int _lastFieldAccessedOffset;
    private bool _lastFieldAccessedIsTyped;

    public int Length => (int)MemoryMarshal.Read<uint>(_buffer);

    public Span<byte> Buffer => _buffer;

    public IndexEntryReader(Span<byte> buffer)
    {
        _buffer = buffer;
        _lastFieldAccessed = -1;
        Unsafe.SkipInit(out _lastFieldAccessedOffset);
        Unsafe.SkipInit(out _lastFieldAccessedIsTyped);
    }
    public readonly ref struct FieldReader
    {
        private readonly IndexEntryReader _parent;
        private readonly IndexEntryFieldType _type;
        private readonly bool _isTyped;
        private readonly int _offset;

        public FieldReader(IndexEntryReader parent, IndexEntryFieldType type, bool isTyped, int offset)
        {
            _parent = parent;
            _type = type;
            _isTyped = isTyped;
            _offset = offset;
        }


        /// <summary>
        ///  Map binary format into IndexEntryFieldIterator
        /// </summary>
        /// <returns>Returns true when binary format is acceptable by IndexEntryFieldIterator. Otherwise false.</returns>
        public bool TryReadMany(out IndexEntryFieldIterator iterator)
        {
            if (_isTyped == false ||
                _type.HasFlag(IndexEntryFieldType.List) == false ||
                _type.HasFlag(IndexEntryFieldType.SpatialPointList))
            {
                iterator = default;
                return false;
            }

            iterator = new IndexEntryFieldIterator(_parent._buffer, _offset);
            return iterator.IsValid;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryReadTuple(out long longValue, out double doubleValue, out Span<byte> sequenceValue)
        {
            bool result = Read(out var type, out longValue, out doubleValue, out sequenceValue);

            // When we dont ask about the type, we dont usually care about the empty lists either.
            // The behavior in those cases is that trying to access an element by index when the list is empty
            // should return false (as in failure). 
            if (type.HasFlag(IndexEntryFieldType.Empty))
                return false;

            return result;
        }

        public bool Read(out IndexEntryFieldType type, out long longValue, out double doubleValue, out Span<byte> sequenceValue)
        {
            if (_type == IndexEntryFieldType.Invalid || _isTyped == false)
                goto Fail;

            type = _type;
            if (type == IndexEntryFieldType.Null)
                goto NullOrEmpty;

            // The read method here will work either if we have a list or a single tuple as long as the type is correct.
            // This has a long history and it is been done for consistency. All internal primitives handling lists like 
            // Sets at the tree levels have the semantic of accessing the first element of the list in case of single
            // reads. Handling lists of elements at the Corax level is an explicit action. The rationale is that the
            // setup costs for handling multiple elements are usually much higher than to access the first element, where
            // data layout is optimized for. 
            if (type.HasFlag(IndexEntryFieldType.Tuple) == false)
                goto Fail;

            if (type.HasFlag(IndexEntryFieldType.HasNulls))
            {
                if (type.HasFlag(IndexEntryFieldType.List))
                    type = IndexEntryFieldType.HasNulls | IndexEntryFieldType.List;
                else
                    type = IndexEntryFieldType.HasNulls;
                goto NullOrEmpty;
            }

            if (type.HasFlag(IndexEntryFieldType.Empty))
            {
                if (type.HasFlag(IndexEntryFieldType.List))
                    type = IndexEntryFieldType.Empty | IndexEntryFieldType.List;
                else
                    type = IndexEntryFieldType.Empty;
                goto NullOrEmpty;
            }

            var buffer = _parent._buffer;
            var intOffset = _offset + sizeof(IndexEntryFieldType);
            longValue = VariableSizeEncoding.Read<long>(buffer, out int length, intOffset); // Read
            intOffset += length;
            doubleValue = Unsafe.ReadUnaligned<double>(ref buffer[intOffset]);
            intOffset += sizeof(double);

            int stringLength = VariableSizeEncoding.Read<ushort>(buffer, out length, intOffset);
            intOffset += length;


            sequenceValue = buffer.Slice(intOffset, stringLength);
            return true;

            Fail:
            Unsafe.SkipInit(out longValue);
            Unsafe.SkipInit(out doubleValue);
            sequenceValue = Span<byte>.Empty;
            type = IndexEntryFieldType.Invalid;
            return false;

            NullOrEmpty:
            Unsafe.SkipInit(out longValue);
            Unsafe.SkipInit(out doubleValue);
            sequenceValue = Span<byte>.Empty;
            return true;
        }

        /// <summary>
        ///  Map binary format into IndexEntryFieldIterator
        /// </summary>
        /// <returns>Returns IndexEntryFieldIterator</returns>
        public IndexEntryFieldIterator ReadMany()
        {
            Span<byte> buffer = _parent._buffer;
            if (_offset == Invalid)
                return new IndexEntryFieldIterator(IndexEntryFieldType.Invalid);
            if (_isTyped == false)
                throw new ArgumentException($"Field cannot be untyped.");

            var type = (IndexEntryFieldType)VariableSizeEncoding.Read<ushort>(buffer, out var length, _offset);
            if (type.HasFlag(IndexEntryFieldType.SpatialPointList))
                throw new NotSupportedException(
                    $"{IndexEntryFieldType.SpatialPointList} is not supported inside {nameof(ReadMany)} Please call {nameof(ReadManySpatialPoint)} or {nameof(TryReadManySpatialPoint)}.");

            return new IndexEntryFieldIterator(buffer, _offset);
        }

        /// <summary>
        /// Read unmanaged field entry from buffer.
        /// To get coordinates from Spatial entry you've to set T as (double Latitude, double Longitude)
        /// </summary>
        public bool Read<T>(out IndexEntryFieldType type, out T value) where T : unmanaged
        {
            if (_offset == Invalid || _isTyped == false)
                goto Fail;

            return ReadFromOffset(_offset, out type, out value);
        
            Fail:
            Unsafe.SkipInit(out value);
            type = IndexEntryFieldType.Invalid;
            return false;
        }

        private bool ReadFromOffset<T>(int intOffset, out IndexEntryFieldType type, out T value) where T : unmanaged
        {
            var buffer = _parent._buffer;
            type = (IndexEntryFieldType)VariableSizeEncoding.Read<ushort>(buffer, out _, _offset);

            if (type == IndexEntryFieldType.Null)
                goto IsNull;

            intOffset += sizeof(IndexEntryFieldType);

            if ((type & IndexEntryFieldType.Tuple) != 0)
            {
                var lResult = VariableSizeEncoding.Read<long>(buffer, out int length, intOffset);
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
                    value = (T)(object)Unsafe.ReadUnaligned<double>(ref buffer[intOffset]);
                    return true;
                }

                if (typeof(T) == typeof(double))
                {
                    var dResult = Unsafe.ReadUnaligned<double>(ref buffer[intOffset]);
                    value = (T)(object)(float)dResult;
                    return true;
                }

                throw new NotSupportedException($"The type {nameof(T)} is unsupported.");
            }

            if (type.HasFlag(IndexEntryFieldType.SpatialPoint))
            {
                //<type><lat><long><amount_of_geohashes><pointer_to_string_length_table><geohash>
                if (typeof(T) == typeof((double, double)))
                {
                    var latitude = Unsafe.ReadUnaligned<double>(ref buffer[intOffset]);
                    intOffset += sizeof(double);
                    var longitude = Unsafe.ReadUnaligned<double>(ref buffer[intOffset]);
                    value = (T)(object)(latitude, longitude);
                    return true;
                }
            }

            throw new NotSupportedException($"The type {nameof(T)} is unsupported.");

            IsNull:
            Unsafe.SkipInit(out value);
            type = IndexEntryFieldType.Null;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Read(out Span<byte> value, int elementIdx = 0)
        {
            bool result = Read(out IndexEntryFieldType type, out value, elementIdx);

            // When we dont ask about the type, we dont usually care about the empty lists either.
            // The behavior in those cases is that trying to access an element by index when the list is empty
            // should return false (as in failure). 
            if (type.HasFlag(IndexEntryFieldType.Empty))
                return false;

            return result;
        }

        
        public bool Read<T>(out T value) where T : unmanaged
        {
            return Read(out var _, out value);
        }


        public bool Read(out IndexEntryFieldType type, out Span<byte> value, int elementIdx = 0)
        {
            if(_offset == Invalid)
                goto Fail;
            
            int stringLength = 0;
            var buffer = _parent._buffer;
            var intOffset = _offset;
            if (_isTyped == false)
            {
                stringLength = VariableSizeEncoding.Read<int>(buffer, out int readOffset, intOffset);
                intOffset += readOffset;
                type = IndexEntryFieldType.Simple;
                goto ReturnSuccessful;
            }

            intOffset += +sizeof(IndexEntryFieldType);
            type = _type;
            if (type == IndexEntryFieldType.Null)
            {
                if (elementIdx == 0)
                    goto IsNull;
                else
                    goto FailNull;
            }

            if (type.HasFlag(IndexEntryFieldType.Empty))
            {
                goto EmptyList;
            }

            if (type.HasFlag(IndexEntryFieldType.List))
            {
                int totalElements = VariableSizeEncoding.Read<ushort>(buffer, out int length, intOffset);
                if (elementIdx >= totalElements)
                    goto Fail;

                intOffset += length;
                var spanTableOffset = Unsafe.ReadUnaligned<int>(ref buffer[intOffset]);
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
                    if (PtrBitVector.GetBitInSpan(buffer.Slice(spanTableOffset), elementIdx) == true)
                        goto HasNull;

                    int nullBitStreamSize = totalElements / (sizeof(byte) * 8) + (totalElements % (sizeof(byte) * 8) == 0 ? 0 : 1);
                    spanTableOffset += nullBitStreamSize; // Point after the null table.                             
                }

                // Skip over the number of entries and jump to the string location.
                for (int i = 0; i < elementIdx; i++)
                {
                    stringLength = VariableSizeEncoding.Read<int>(buffer, out length, spanTableOffset);
                    intOffset += stringLength;
                    spanTableOffset += length;
                }

                stringLength = VariableSizeEncoding.Read<int>(buffer, out length, spanTableOffset);
            }
            else if ((type & IndexEntryFieldType.Raw) != 0)
            {
                if (type.HasFlag(IndexEntryFieldType.HasNulls))
                {
                    var spanTableOffset = Unsafe.ReadUnaligned<int>(ref buffer[intOffset]);
                    if (PtrBitVector.GetBitInSpan(buffer.Slice(spanTableOffset), elementIdx) == true)
                        goto HasNull;
                }

                stringLength = VariableSizeEncoding.Read<int>(buffer, out int readOffset, intOffset);
                intOffset += readOffset;
                type = IndexEntryFieldType.Raw;
            }
            else if ((type & IndexEntryFieldType.Tuple) != 0)
            {
                if (type.HasFlag(IndexEntryFieldType.HasNulls))
                {
                    var spanTableOffset = Unsafe.ReadUnaligned<int>(ref buffer[intOffset]);
                    if (PtrBitVector.GetBitInSpan(buffer.Slice(spanTableOffset), elementIdx) == true)
                        goto HasNull;
                }

                VariableSizeEncoding.Read<long>(buffer, out int length, intOffset); // Skip
                intOffset += length;
                Unsafe.ReadUnaligned<double>(ref buffer[intOffset]);
                intOffset += sizeof(double);

                stringLength = VariableSizeEncoding.Read<int>(buffer, out int readOffset, intOffset);
                intOffset += readOffset;
            }
            else if ((type & IndexEntryFieldType.SpatialPoint) != 0)
            {
                intOffset += 2 * sizeof(double);
                stringLength = VariableSizeEncoding.Read<byte>(buffer, out var length, intOffset);
                intOffset += length;
            }


            ReturnSuccessful:
            value = buffer.Slice(intOffset, stringLength);
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



        /// <summary>
        ///  Try map index entry field into SpatialPointList
        /// </summary>
        /// <returns> True is successful - otherwise false</returns>
        public bool TryReadManySpatialPoint(out SpatialPointFieldIterator iterator)
        {
            if (_isTyped == false || _offset == Invalid)
                goto Failed;

            if (_type.HasFlag(IndexEntryFieldType.SpatialPointList))
            {
                iterator = new SpatialPointFieldIterator(_parent._buffer, _offset);
                return true;
            }

            Failed:
            iterator = default;
            return false;
        }
    }

    public DynamicFieldEnumerator GetEnumerator() => new(this);

    public ref struct DynamicFieldEnumerator
    {
        private readonly IndexEntryReader _parent;
        private int _remaining;
        private int _position;
        
        public Span<byte> CurrentFieldName;
        public int CurrentValueOffset;
        public IndexEntryFieldType CurrentFieldType => Unsafe.ReadUnaligned<IndexEntryFieldType>(ref _parent._buffer[CurrentValueOffset]);

        public DynamicFieldEnumerator(IndexEntryReader parent)
        {
            _parent = parent;
            var buffer = _parent._buffer;
            ref var header = ref MemoryMarshal.AsRef<IndexEntryHeader>(buffer);
            _position = checked((int)header.DynamicTable);
            _remaining = VariableSizeEncoding.Read<int>(buffer, out var offset, _position);
            _position += offset;
            CurrentFieldName = default;
            CurrentValueOffset = default;
        }

        public bool MoveNext()
        {
            if (_remaining == 0)
            {
                CurrentFieldName = default;
                return false;
            }

            _remaining--;
            
            var buffer = _parent._buffer;
            int dynamicEntryOffset = VariableSizeEncoding.Read<int>(buffer, out var offset, _position);
            _position += offset;
            var len = VariableSizeEncoding.Read<int>(buffer, out offset, dynamicEntryOffset);
            CurrentFieldName = buffer[(dynamicEntryOffset + offset)..(dynamicEntryOffset + offset + len)];
            CurrentValueOffset = dynamicEntryOffset + offset + len;
            return true;
        }
    }

    private bool ReadDynamicValueOffset(ReadOnlySpan<byte> name, out int valueOffset)
    {
        var it = new DynamicFieldEnumerator(this);
        while (it.MoveNext())
        {
            if (it.CurrentFieldName.SequenceEqual(name))
            {
                valueOffset = it.CurrentValueOffset;
                return true;
            }
        }
        valueOffset = default;
        return false;
    }


    public IndexEntryFieldType GetFieldType(ReadOnlySpan<byte> name, out int intOffset)
    {
        if (ReadDynamicValueOffset(name, out intOffset))
        { 
            var type = Unsafe.ReadUnaligned<IndexEntryFieldType>(ref _buffer[intOffset]);
            intOffset += Unsafe.SizeOf<IndexEntryFieldType>();
            return type;
        }
        return IndexEntryFieldType.Invalid;
    }

    public FieldReader GetReaderFor(ReadOnlySpan<byte> name)
    {
        if (ReadDynamicValueOffset(name, out var intOffset))
        {
            var type = (IndexEntryFieldType)VariableSizeEncoding.Read<ushort>(_buffer, out _, intOffset);
            return new FieldReader(this, type, true, intOffset );
        }

        return new FieldReader(this, IndexEntryFieldType.Invalid, false, Invalid);
    }

    [Pure]
    public FieldReader GetReaderFor(int field)
    {
        var (intOffset, isTyped) = GetMetadataFieldLocation(_buffer, field);
        IndexEntryFieldType type = IndexEntryFieldType.Invalid;
        if (intOffset != Invalid)
        {
            type = isTyped ? 
                Unsafe.ReadUnaligned<IndexEntryFieldType>(ref _buffer[intOffset]) : 
                IndexEntryFieldType.Simple;
        }

        return new FieldReader(this, type, isTyped, intOffset);
    }

    public IndexEntryFieldType GetFieldType(int field, out int intOffset)
    {
        (intOffset, var isTyped) = GetMetadataFieldLocation(_buffer, field);
        if (intOffset == Invalid)
            return IndexEntryFieldType.Invalid;

        if (isTyped)
        {
            var type = Unsafe.ReadUnaligned<IndexEntryFieldType>(ref _buffer[intOffset]);
            intOffset += Unsafe.SizeOf<IndexEntryFieldType>();
            return type;
        }

        return IndexEntryFieldType.Simple;
    }

    

    //<type:byte><amount_of_items:int><geohashLevel:int><geohash_ptr:int>
    //<longitudes_ptr:int><latitudes_list:double[]><longtitudes_list:double[]><geohashes_list:bytes[]>
    public SpatialPointFieldIterator ReadManySpatialPoint(int field)
    {
        var (intOffset, isTyped) = GetMetadataFieldLocation(_buffer, field);
        if (intOffset == Invalid || isTyped == false)
            return new SpatialPointFieldIterator();

        return new SpatialPointFieldIterator(_buffer, intOffset);
    }


    
    private (int offset, bool isTyped) GetMetadataFieldLocation(Span<byte> buffer, int field)
    {
        if (field == _lastFieldAccessed)
        {
            return (_lastFieldAccessedOffset, _lastFieldAccessedIsTyped);
        }
        else
        {
            (_lastFieldAccessedOffset, _lastFieldAccessedIsTyped) = GetMetadataFieldLocationUnlikely(buffer, field);
            _lastFieldAccessed = field;
            return (_lastFieldAccessedOffset, _lastFieldAccessedIsTyped);
        }
    }

    public static ReadOnlySpan<byte> TableEncodingLookupTable => new byte[] { 0, 1, 2, 4 };

    private static ReadOnlySpan<byte> ByteKnownFieldMaskShiftLookupTable => new byte[]
    {
        0,
        (sizeof(byte) - 1) * 8,
        (sizeof(short) - 1) * 8,
        (sizeof(int) - 1) * 8,
    };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (int offset, bool isTyped) GetMetadataFieldLocationUnlikely(Span<byte> buffer, int field)
    {
        ref var header = ref MemoryMarshal.AsRef<IndexEntryHeader>(buffer);

        ushort knownFieldsCount = (ushort)(header.KnownFieldCount >> 2);
        IndexEntryTableEncoding encoding = (IndexEntryTableEncoding)(header.KnownFieldCount & 0b11);

        int encodeSize = TableEncodingLookupTable[(int)encoding];
        Debug.Assert(header.Length == (int)header.Length);
        int locationOffset = (int)header.Length - (knownFieldsCount * encodeSize) + field * encodeSize;

        int offset;
        switch (encoding)
        {
            case IndexEntryTableEncoding.OneByte:
            {
                offset = Unsafe.ReadUnaligned<byte>(ref buffer[locationOffset]);
                if (offset == 0xFF)
                    goto Fail;
                goto End;
            }
            case IndexEntryTableEncoding.TwoBytes:
            {
                offset = Unsafe.ReadUnaligned<ushort>(ref buffer[locationOffset]);
                if (offset == 0xFFFF)
                    goto Fail;
                goto End;
            }
            case IndexEntryTableEncoding.FourBytes:
            {
                offset = (int)Unsafe.ReadUnaligned<uint>(ref buffer[locationOffset]);
                if (offset == unchecked((int)0xFFFF_FFFF))
                    goto Fail;
                goto End;
            }
        }
        
        Fail:
        return (Invalid, false);

        End:
        int mask = 0x80 << ByteKnownFieldMaskShiftLookupTable[(int)encoding];
        bool isTyped = (offset & mask) != 0;
        offset &= ~mask;
        
        return (offset, isTyped);
    }

    public string DebugDump(IndexFieldsMapping knownFields)
    {
        string result = string.Empty;
        foreach (var (name, field) in knownFields.Select(x => (x.FieldName, x.FieldId)))
        {
            var reader = GetReaderFor(field);
            var type = GetFieldType(field, out _);
            if (type is IndexEntryFieldType.Simple or IndexEntryFieldType.Tuple)
            {
                reader.Read(out var value);
                result += $"{name}: {Encodings.Utf8.GetString(value)}{Environment.NewLine}";
            }
            else if (type == IndexEntryFieldType.Invalid)
            {
                result += $"{name}: null{Environment.NewLine}";
            }
            else if (type is IndexEntryFieldType.List or IndexEntryFieldType.TupleList)
            {
                var iterator = reader.ReadMany();

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
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadTuple(int field, out long longValue, out double doubleValue, out Span<byte> sequenceValue)
    {
        bool result = Read(field, out var type, out longValue, out doubleValue, out sequenceValue);

        // When we dont ask about the type, we dont usually care about the empty lists either.
        // The behavior in those cases is that trying to access an element by index when the list is empty
        // should return false (as in failure). 
        if (type.HasFlag(IndexEntryFieldType.Empty))
            return false;

        return result;
    }

    public bool Read(int field, out IndexEntryFieldType type, out long longValue, out double doubleValue, out Span<byte> sequenceValue)
    {
        var (intOffset, isTyped) = GetMetadataFieldLocation(_buffer, field);
        if (intOffset == Invalid || isTyped == false)
            goto Fail;

        type = GetFieldType(field, out intOffset);
        if (type == IndexEntryFieldType.Null)
            goto NullOrEmpty;

        // The read method here will work either if we have a list or a single tuple as long as the type is correct.
        // This has a long history and it is been done for consistency. All internal primitives handling lists like 
        // Sets at the tree levels have the semantic of accessing the first element of the list in case of single
        // reads. Handling lists of elements at the Corax level is an explicit action. The rationale is that the
        // setup costs for handling multiple elements are usually much higher than to access the first element, where
        // data layout is optimized for. 
        if (type.HasFlag(IndexEntryFieldType.Tuple) == false)
            goto Fail;

        if (type.HasFlag(IndexEntryFieldType.HasNulls))
        {
            if (type.HasFlag(IndexEntryFieldType.List))
                type = IndexEntryFieldType.HasNulls | IndexEntryFieldType.List;
            else
                type = IndexEntryFieldType.HasNulls;
            goto NullOrEmpty;
        }

        if (type.HasFlag(IndexEntryFieldType.Empty))
        {
            if (type.HasFlag(IndexEntryFieldType.List))
                type = IndexEntryFieldType.Empty | IndexEntryFieldType.List;
            else
                type = IndexEntryFieldType.Empty;
            goto NullOrEmpty;
        }

        longValue = VariableSizeEncoding.Read<long>(_buffer, out int length, intOffset); // Read
        intOffset += length;
        doubleValue = Unsafe.ReadUnaligned<double>(ref _buffer[intOffset]);
        intOffset += sizeof(double);

        int stringLength = VariableSizeEncoding.Read<ushort>(_buffer, out length, intOffset);
        intOffset += length;


        sequenceValue = _buffer.Slice(intOffset, stringLength);
        return true;


    Fail:
        Unsafe.SkipInit(out longValue);
        Unsafe.SkipInit(out doubleValue);
        sequenceValue = Span<byte>.Empty;
        type = IndexEntryFieldType.Invalid;
        return false;

    NullOrEmpty:
        Unsafe.SkipInit(out longValue);
        Unsafe.SkipInit(out doubleValue);
        sequenceValue = Span<byte>.Empty;
        return true;
    }   

}
