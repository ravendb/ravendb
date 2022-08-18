using System;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow;
using Voron.Data.BTrees;
using Voron.Impl;

namespace Voron.Data
{
    public sealed unsafe class VoronStream : Stream
    {
        public Slice Name { get; }

        private readonly Tree.ChunkDetails[] _chunksDetails;
        private readonly long[] _chunksOffsets;
        private long _position;
        private int _index;
        private LowLevelTransaction _llt;
        
        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => false;
        public override long Length { get; }

        public override string ToString()
        {
            return Name.ToString();
        }

        public VoronStream(Slice name, Tree.ChunkDetails[] chunksDetails, LowLevelTransaction llt)
        {
            Name = name;

            _chunksDetails = chunksDetails;
            _chunksOffsets = new long[_chunksDetails.Length];

            _index = 0;
            _llt = llt;
            _lastPage = default(Page);
            _position = 0;

            for (int index = 0; index < _chunksDetails.Length; index++)
            {
                ref Tree.ChunkDetails cd = ref _chunksDetails[index];
                _chunksOffsets[index] = Length;
                Length += cd.ChunkSize;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpdateCurrentTransaction(Transaction tx)
        {
            if (tx != null)
            {
                if (_llt == tx.LowLevelTransaction)
                    return;

                _llt = tx.LowLevelTransaction;
                _lastPage = default(Page);
                return;
            }

            ThrowTransactionIsNull();            
        }

        private void ThrowTransactionIsNull()
        {
            throw new ArgumentNullException("tx");
        }

        public override long Position
        {
            get
            {
                return _position;
            }
            set
            {
                if (value >= Length)
                {
                    //Out of stream
                    _index = _chunksDetails.Length - 1;
                    _position = Length;
                    return;
                }

                var search = Array.BinarySearch(_chunksOffsets, value);
                
                if (search >= 0)
                {
                    //The index of the specified value in the specified array, if value is found; otherwise, a negative number
                 
                    //Ideally hit the 0th element of chunk.
                    _position = value;
                    _index = search;
                }
                else
                {
                    //If value is not found and value is less than one or more elements in array, the negative number returned is the bitwise complement of the index of the first element that is larger than value.
                    //If value is not found and value is greater than all elements in array, the negative number returned is the bitwise complement of (the index of the last element plus 1). If this method is called with a non-sorted array, the return value can be incorrect and a negative number could be returned, even if value is present in array.
                    search = ~search;
                    
                    //LessOrEqualZero should not be here. 0 means it should be handled above (it is a 0th element of first chunk
                    //If it is negative then offset from Seek() is invalid (because it should move ptr to right, not to left)
                    if (search <= 0)
                        ThrowWhenValueIsEqualOrLessZero(value);

                    _position = value;
                    _index = search - 1;
                }
            }
        }

        private Page _lastPage;

        public override int ReadByte()
        {
            var chunk = _chunksDetails[_index];

            if (_position - _chunksOffsets[_index] == chunk.ChunkSize)
            {
                if (_index == _chunksDetails.Length - 1)
                    return -1;

                _index++;
                chunk = _chunksDetails[_index];

                if (chunk.ChunkSize == 0)
                    return -1;
            }

            if (!_lastPage.IsValid || _lastPage.PageNumber != chunk.PageNumber)
            {
                _lastPage = _llt.GetPage(chunk.PageNumber);
            }
            
            var pos = _position - _chunksOffsets[_index];
            _position++; //move ptr
            return _lastPage.DataPointer[pos];
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var len = _chunksDetails[_index].ChunkSize;

            if (_position == _chunksOffsets[_index] + len)
            {
                if (_index == _chunksDetails.Length - 1)
                    return 0;

                _index++;
                
                len = _chunksDetails[_index].ChunkSize;

                if (len == 0)
                    return 0;
            }
            
            //0th element of array + len(chunk size ) == 0th element of next chunk
            var countSizeLeft = _chunksOffsets[_index] + len - _position;
            if (count > countSizeLeft)
                count = (int)countSizeLeft;

            ref Tree.ChunkDetails chunk = ref _chunksDetails[_index];
            if (!_lastPage.IsValid || _lastPage.PageNumber != chunk.PageNumber)
            {
                _lastPage = _llt.GetPage(chunk.PageNumber);
            }

            var pos = _position - _chunksOffsets[_index];
            fixed (byte* dst = buffer)
            {
                Memory.Copy(dst + offset, _lastPage.DataPointer + pos, count);
            }

            //move ptr
            _position += count;
            return count;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    Position = offset;
                    break;

                case SeekOrigin.Current:
                    Position += offset;
                    break;

                case SeekOrigin.End:
                    Position = Length + offset;
                    break;
            }

            return Position;
        }

        public override void Flush()
        {
            throw new NotSupportedException("The method or operation is not supported by VoronStream.");
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException("The method or operation is not supported by VoronStream.");
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException("The method or operation is not supported by VoronStream.");
        }

        private static void ThrowWhenValueIsEqualOrLessZero(long position)
        {
            throw new ArgumentException($"Position {position} is not possible inside {nameof(VoronStream)}.");
        }
    }
}
