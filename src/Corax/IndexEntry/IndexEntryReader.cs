using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Fields;
using Corax.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server.Compression;

namespace Corax.IndexEntry;

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
public unsafe readonly ref struct IndexEntryReader
{
    private const int Invalid = unchecked((int)0xFFFF_FFFF);
    private readonly Span<byte> _buffer;

    public int Length => (int)MemoryMarshal.Read<uint>(_buffer);

    public IndexEntryReader(Span<byte> buffer)
    {
        _buffer = buffer;
    }


    /// <summary>
    /// Read unmanaged field entry from buffer.
    /// To get coordinates from Spatial entry you've to set T as (double Latitude, double Longitude)
    /// </summary>
    public bool Read<T>(int field, out IndexEntryFieldType type, out T value) where T : unmanaged
    {
        var (intOffset, isTyped) = GetMetadataFieldLocation(_buffer, field);
        if (intOffset == Invalid || isTyped == false)
            goto Fail;


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

        if (type.HasFlag(IndexEntryFieldType.SpatialPoint))
        {
            //<type><lat><long><amount_of_geohashes><pointer_to_string_length_table><geohash>
            if (typeof(T) == typeof((double, double)))
            {
                var latitude = Unsafe.ReadUnaligned<double>(ref _buffer[intOffset]);
                intOffset += sizeof(double);
                var longitude = Unsafe.ReadUnaligned<double>(ref _buffer[intOffset]);
                value = (T)(object)(latitude, longitude);
                return true;
            }
        }

        throw new NotSupportedException($"The type {nameof(T)} is unsupported.");

    Fail:
        Unsafe.SkipInit(out value);
        type = IndexEntryFieldType.Invalid;
        return false;
    IsNull:
        Unsafe.SkipInit(out value);
        type = IndexEntryFieldType.Null;
        return true;
    }

    /// <summary>
    /// Read unmanaged field entry from buffer.
    /// To get coordinates from Spatial entry you've to set T as (double Latitude, double Longitude)
    /// </summary>
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
            var type = Unsafe.ReadUnaligned<IndexEntryFieldType>(ref _buffer[intOffset]);
            intOffset += Unsafe.SizeOf<IndexEntryFieldType>();
            return type;
        }

        return IndexEntryFieldType.Simple;
    }

    /// <summary>
    ///  Try map index entry field into SpatialPointList
    /// </summary>
    /// <returns> True is successful - otherwise false</returns>
    public bool TryReadManySpatialPoint(int field, out SpatialPointFieldIterator iterator)
    {
        var (intOffset, isTyped) = GetMetadataFieldLocation(_buffer, field);

        if (isTyped == false || intOffset == Invalid)
            goto Failed;


        var type = (IndexEntryFieldType)VariableSizeEncoding.Read<ushort>(_buffer, out var length, intOffset);
        if (type.HasFlag(IndexEntryFieldType.SpatialPointList))
        {
            iterator = new SpatialPointFieldIterator(_buffer, intOffset);
            return true;
        }

    Failed:
        iterator = default;
        return false;
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

    /// <summary>
    ///  Map binary format into IndexEntryFieldIterator
    /// </summary>
    /// <returns>Returns IndexEntryFieldIterator</returns>
    public IndexEntryFieldIterator ReadMany(int field)
    {
        var (intOffset, isTyped) = GetMetadataFieldLocation(_buffer, field);

        if (intOffset == Invalid)
            return new IndexEntryFieldIterator(IndexEntryFieldType.Invalid);

        if (isTyped == false)
            throw new ArgumentException($"Field with index number '{field}' is untyped.");


        var type = (IndexEntryFieldType)VariableSizeEncoding.Read<byte>(_buffer, out var length, intOffset);
        if (type.HasFlag(IndexEntryFieldType.SpatialPointList))
            throw new NotSupportedException($"{IndexEntryFieldType.SpatialPointList} is not supported inside {nameof(ReadMany)} Please call {nameof(ReadManySpatialPoint)} or {nameof(TryReadManySpatialPoint)}.");

        return new IndexEntryFieldIterator(_buffer, intOffset);
    }


    /// <summary>
    ///  Map binary format into IndexEntryFieldIterator
    /// </summary>
    /// <returns>Returns true when binary format is acceptable by IndexEntryFieldIterator. Otherwise false.</returns>
    public bool TryReadMany(int field, out IndexEntryFieldIterator iterator)
    {
        var (intOffset, isTyped) = GetMetadataFieldLocation(_buffer, field);
        if (intOffset == Invalid)
            goto Failed;

        if (isTyped == false)
            goto Failed;

        var type = (IndexEntryFieldType)VariableSizeEncoding.Read<ushort>(_buffer, out var length, intOffset);

        if (type.HasFlag(IndexEntryFieldType.List) == false || type.HasFlag(IndexEntryFieldType.SpatialPointList))
            goto Failed;

        iterator = new IndexEntryFieldIterator(_buffer, intOffset);
        return iterator.IsValid;

    Failed:
        iterator = default;
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Read(int field, out Span<byte> value, int elementIdx = 0)
    {
        bool result = Read(field, out IndexEntryFieldType type, out value, elementIdx);

        // When we dont ask about the type, we dont usually care about the empty lists either.
        // The behavior in those cases is that trying to access an element by index when the list is empty
        // should return false (as in failure). 
        if (type.HasFlag(IndexEntryFieldType.Empty))
            return false;

        return result;
    }

    public bool Read(int field, out IndexEntryFieldType type, out Span<byte> value, int elementIdx = 0)
    {
        var (intOffset, isTyped) = GetMetadataFieldLocation(_buffer, field);
        if (intOffset == Invalid)
            goto Fail;

        int stringLength = 0;

        if (isTyped == false)
        {
            stringLength = VariableSizeEncoding.Read<int>(_buffer, out int readOffset, intOffset);
            intOffset += readOffset;
            type = IndexEntryFieldType.Simple;
            goto ReturnSuccessful;
        }

        type = GetFieldType(field, out intOffset);

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

                int nullBitStreamSize = totalElements / (sizeof(byte) * 8) + (totalElements % (sizeof(byte) * 8) == 0 ? 0 : 1);
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
        else if ((type & IndexEntryFieldType.SpatialPoint) != 0)
        {
            intOffset += 2 * sizeof(double);
            stringLength = VariableSizeEncoding.Read<byte>(_buffer, out var length, intOffset);
            intOffset += length;
        }


    ReturnSuccessful:
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
        
        if( type.HasFlag(IndexEntryFieldType.Empty))
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (int offset, bool isTyped) GetMetadataFieldLocation(Span<byte> buffer, int field)
    {
        ref var header = ref MemoryMarshal.AsRef<IndexEntryHeader>(buffer);

        ushort knownFieldsCount = (ushort)(header.KnownFieldCount >> 2);
        IndexEntryTableEncoding encoding = (IndexEntryTableEncoding)(header.KnownFieldCount & 0b11);
        int encodeSize = encoding switch
        {
            IndexEntryTableEncoding.OneByte => 1,
            IndexEntryTableEncoding.TwoBytes => 2,
            IndexEntryTableEncoding.FourBytes => 4,
            _ => throw new InvalidOperationException()
        };

        int locationOffset = buffer.Length - (knownFieldsCount * encodeSize) + field * encodeSize;

        int offset;
        bool isTyped;

        if (encoding == IndexEntryTableEncoding.OneByte)
        {
            offset = Unsafe.ReadUnaligned<byte>(ref buffer[locationOffset]);
            if (offset == 0xFF)
                goto Fail;
            isTyped = (offset & 0x80) != 0;
            offset &= ~0x80;
            goto End;
        }

        if (encoding == IndexEntryTableEncoding.TwoBytes)
        {
            offset = Unsafe.ReadUnaligned<ushort>(ref buffer[locationOffset]);
            if (offset == 0xFFFF)
                goto Fail;
            isTyped = (offset & 0x8000) != 0;
            offset &= ~0x8000;
            goto End;
        }

        if (encoding == IndexEntryTableEncoding.FourBytes)
        {
            offset = (int)Unsafe.ReadUnaligned<uint>(ref buffer[locationOffset]);
            if (offset == unchecked((int)0xFFFF_FFFF))
                goto Fail;
            isTyped = (offset & Constants.IndexWriter.IntKnownFieldMask) != 0;
            offset &= ~Constants.IndexWriter.IntKnownFieldMask;
            goto End;
        }

    Fail:
        return (Invalid, false);

    End:
        return (offset, isTyped);
    }

    public string DebugDump(IndexFieldsMapping knownFields)
    {
        string result = string.Empty;
        foreach (var (name, field) in knownFields.Select(x => (x.FieldName, x.FieldId)))
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
}
