﻿using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Sparrow;
using Voron.Data.BTrees;
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Data
{
    public unsafe class VoronStream : Stream
    {
        public Slice Name { get; }

        private readonly Tree.ChunkDetails[] _chunksDetails;
        private readonly int[] _positions;
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
            _positions = new int[_chunksDetails.Length];
            _index = 0;
            _llt = llt;
            foreach (var cd in _chunksDetails)
            {
                Length += cd.ChunkSize;
            }
        }

        public void UpdateCurrentTransaction(Transaction tx)
        {
            if (tx == null)
                throw new ArgumentNullException(nameof(tx));
            _llt = tx.LowLevelTransaction;
        }

        public override long Position
        {
            get
            {
                long pos = 0;
                // ReSharper disable once ForCanBeConvertedToForeach
                for (int i = 0; i < _positions.Length; i++)
                {
                    pos += _positions[i];
                }
                return pos;
            }
            set
            {
                long pos = 0;
                _index = _positions.Length - 1;
                for (int i = 0; i < _positions.Length; i++)
                {
                    if (pos + _chunksDetails[i].ChunkSize > value)
                    {
                        _positions[i] = (int)((value - pos) % _chunksDetails[i].ChunkSize);
                        _index = i;
                        break;
                    }
                    _positions[i] = _chunksDetails[i].ChunkSize;
                    pos += _chunksDetails[i].ChunkSize;
                }
                for (int i = _index + 1; i < _positions.Length; i++)
                {
                    _positions[i] = 0;
                }
            }
        }

        public override int ReadByte()
        {
            var pos = _positions[_index];
            var len = _chunksDetails[_index].ChunkSize;

            if (pos == len)
            {
                if (_index == _chunksDetails.Length - 1)
                    return -1;

                _index++;
            }

            var pagerRef = new LowLevelTransaction.PagerRef();
            var page = _llt.GetPage(_chunksDetails[_index].PageNumber, pagerRef);
            pagerRef.Pager.EnsureMapped(_llt, pagerRef.PagerPageNumber, pagerRef.Pager.GetNumberOfOverflowPages(page.OverflowSize));

            return page.DataPointer[_positions[_index]++];
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var pos = _positions[_index];
            var len = _chunksDetails[_index].ChunkSize;

            if (pos == len)
            {
                if (_index == _chunksDetails.Length - 1)
                    return 0;

                _index++;

                pos = _positions[_index];
                len = _chunksDetails[_index].ChunkSize;
            }

            if (count > len - pos)
                count = len - pos;

            var pagerRef = new LowLevelTransaction.PagerRef();
            var page = _llt.GetPage(_chunksDetails[_index].PageNumber, pagerRef);
            pagerRef.Pager.EnsureMapped(_llt, pagerRef.PagerPageNumber, pagerRef.Pager.GetNumberOfOverflowPages(page.OverflowSize));

            fixed (byte* dst = buffer)
            {
                Memory.CopyInline(dst + offset, page.DataPointer + pos, count);
            }

            _positions[_index] += count;

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
    }
}