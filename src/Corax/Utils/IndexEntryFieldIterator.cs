using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Binary;
using Sparrow.Compression;

namespace Corax
{
    public ref struct IndexEntryFieldIterator
    {
        private const int Invalid = -1;
        
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
        private int _stringLength;
        
        internal IndexEntryFieldIterator(IndexEntryFieldType type)
        {
            Debug.Assert(type == IndexEntryFieldType.Invalid);
            Type = IndexEntryFieldType.Invalid;
            Count = 0;
            _buffer = ReadOnlySpan<byte>.Empty;
            IsValid = false;
            _currentIdx = Invalid;

            _spanOffset = 0;
            _nullTableOffset = 0;
            _spanTableOffset = 0;
            _spanOffset = 0;
            _longOffset = 0;
            _doubleOffset = 0;
            _stringLength = Invalid;
        }

        public IndexEntryFieldIterator(ReadOnlySpan<byte> buffer, int offset)
        {
            _buffer = buffer;

            Type = MemoryMarshal.Read<IndexEntryFieldType>(buffer.Slice(offset));
            offset += sizeof(IndexEntryFieldType);

            if (Type.HasFlag(IndexEntryFieldType.List) == false)
            {
                IsValid = false;
                Count = 0;
                _currentIdx = Invalid;
                _stringLength = Invalid;
                _spanOffset = 0;
                _nullTableOffset = 0;
                _spanTableOffset = 0;
                _spanOffset = 0;
                _longOffset = 0;
                _doubleOffset = 0;
                return;
            }

            Count = VariableSizeEncoding.Read<ushort>(_buffer, out var length, offset);
            offset += length;

            _nullTableOffset = MemoryMarshal.Read<int>(_buffer[offset..]);
            if (Type.HasFlag(IndexEntryFieldType.Tuple) && !Type.HasFlag(IndexEntryFieldType.Empty))
            {
                _longOffset = MemoryMarshal.Read<int>(_buffer[(offset + sizeof(int))..]);
                _doubleOffset = (offset + 2 * sizeof(int)); // Skip the pointer from sequences and longs.

                if (Type.HasFlag(IndexEntryFieldType.HasNulls))
                {
                    int nullBitStreamSize = Count / (sizeof(byte) * 8) + (Count % (sizeof(byte) * 8) == 0 ? 0 : 1);
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
                    int nullBitStreamSize = Count / (sizeof(byte) * 8) + (Count % (sizeof(byte) * 8) == 0 ? 0 : 1);
                    _spanTableOffset = _nullTableOffset + nullBitStreamSize; // Point after the null table.         
                }
                else
                {
                    _spanTableOffset = _nullTableOffset;
                }

                _spanOffset = (offset + sizeof(int));
            }
            
            _stringLength = Invalid;
            _currentIdx = Invalid;
            IsValid = true;
        }

        public bool IsNull
        {
            get
            {
                if (!IsValid)
                    throw new InvalidOperationException($"Cannot call {nameof(IsNull)} on an invalid iterator.");

                if (_currentIdx < 0)
                    throw new InvalidOperationException($"Cannot call {nameof(IsNull)} before calling {nameof(ReadNext)}.");

                if (!Type.HasFlag(IndexEntryFieldType.HasNulls))
                    return false;

                unsafe
                {
                    fixed (byte* nullTablePtr = _buffer)
                    {
                        return PtrBitVector.GetBitInPointer(nullTablePtr + _nullTableOffset, _currentIdx);
                    }
                }
            }
        }

        /// <summary>
        /// Check if whole collection is empty.
        /// </summary>
        public bool IsEmptyCollection
        {
            get
            {
                if (IsValid == false)
                    throw new InvalidOperationException($"Cannot call {nameof(IsEmptyCollection)} on an invalid iterator.");

                return Type.HasFlag(IndexEntryFieldType.Empty);
            }
        }
        
        /// <summary>
        /// Check if current string is empty.
        /// </summary>
        public bool IsEmptyString
        {
            get
            {
                if (!IsValid)
                    throw new InvalidOperationException($"Cannot call {nameof(IsEmptyString)} on an invalid iterator.");
                
                if (_stringLength == Invalid)
                    _stringLength = VariableSizeEncoding.Read<int>(_buffer, out _, _spanTableOffset);

                return _stringLength == 0;
            }
        }
        
        public ReadOnlySpan<byte> Sequence
        {
            get
            {
                if (!IsValid)
                    throw new InvalidOperationException($"Cannot call {nameof(Sequence)} on an invalid iterator.");

                if (Count == 0 || _currentIdx >= Count)
                    throw new IndexOutOfRangeException();

                if (_currentIdx < 0)
                    throw new InvalidOperationException($"Cannot call {nameof(IsNull)} before calling {nameof(ReadNext)}.");

                if (_stringLength == Invalid)
                    _stringLength = VariableSizeEncoding.Read<int>(_buffer, out _, _spanTableOffset);
                
                return _buffer.Slice(_spanOffset, _stringLength);
            }
        }

        public long Long
        {
            get
            {
                if (!IsValid)
                    throw new InvalidOperationException($"Cannot call {nameof(Long)} on an invalid iterator.");
                
                if (!Type.HasFlag(IndexEntryFieldType.Tuple))
                    throw new InvalidOperationException();
                if (Count == 0 || _currentIdx >= Count)
                    throw new IndexOutOfRangeException();

                if (_currentIdx < 0)
                    throw new InvalidOperationException($"Cannot call {nameof(IsNull)} before calling {nameof(ReadNext)}.");

                return VariableSizeEncoding.Read<long>(_buffer, out _, _longOffset);
            }
        }

        public double Double
        {
            get
            {
                if (!IsValid)
                    throw new InvalidOperationException($"Cannot call {nameof(Double)} on an invalid iterator.");

                if (!Type.HasFlag(IndexEntryFieldType.Tuple))
                    throw new InvalidOperationException();
                
                if (Count == 0 || _currentIdx >= Count)
                    throw new IndexOutOfRangeException();

                if (_currentIdx < 0)
                    throw new InvalidOperationException($"Cannot call {nameof(IsNull)} before calling {nameof(ReadNext)}.");

                return Unsafe.ReadUnaligned<double>(ref MemoryMarshal.GetReference(_buffer[_doubleOffset..]));
            }
        }

        public bool ReadNext()
        {
            _currentIdx++;
            _stringLength = Invalid;
            
            if (_currentIdx >= Count)
                return false;

            if (_currentIdx > 0)
            {
                // This two have fixed size. 
                _spanOffset += VariableSizeEncoding.Read<int>(_buffer, out var length, _spanTableOffset);
                _spanTableOffset += length;

                if (Type.HasFlag(IndexEntryFieldType.Tuple))
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
}
