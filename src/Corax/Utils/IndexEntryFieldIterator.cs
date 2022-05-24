using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Binary;
using Sparrow.Server.Compression;

namespace Corax;

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
