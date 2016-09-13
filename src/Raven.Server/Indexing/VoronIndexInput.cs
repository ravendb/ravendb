using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using Lucene.Net.Store;
using Raven.Abstractions.Extensions;
using Raven.Server.Utils;
using Sparrow;
using Voron;
using Voron.Impl;

namespace Raven.Server.Indexing
{
    public unsafe class VoronIndexInput : IndexInput
    {
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        private bool _isOriginal = true;

        private readonly long _originalTransactionId;
        private readonly ThreadLocal<Transaction> _currentTransaction;

        private readonly string _name;

        private MmapStream[] _streams;
        private byte*[] _basePtrs;
        private int[] _chunkSizes;
        private CombinedReadingStream _stream;

        public VoronIndexInput(ThreadLocal<Transaction> transaction, string name)
        {
            _name = name;
            _originalTransactionId = transaction.Value.LowLevelTransaction.Id;
            _currentTransaction = transaction;

            OpenInternal();
        }

        private void OpenInternal()
        {
            var fileTree = _currentTransaction.Value.ReadTree(_name);

            if (fileTree == null)
                throw new FileNotFoundException("Could not find index input", _name);

            var numberOfChunks = fileTree.State.NumberOfEntries;

            _basePtrs = new byte*[numberOfChunks];
            _chunkSizes = new int[numberOfChunks];
            _streams = new MmapStream[numberOfChunks];

            int index = 0;

            using (var it = fileTree.Iterate(prefetch: false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    throw new InvalidDataException("Could not seek to any chunk of this file");

                do
                {
                    var readResult = fileTree.Read(it.CurrentKey);

                    _basePtrs[index] = readResult.Reader.Base;
                    _chunkSizes[index] = readResult.Reader.Length;
                    _streams[index] = new MmapStream(readResult.Reader.Base, readResult.Reader.Length);

                    index++;
                } while (it.MoveNext());
            }
            
            if (numberOfChunks != index)
                throw new InvalidDataException($"Read invalid number of file chunks. Expected {numberOfChunks}, read {index}.");

            _stream = new CombinedReadingStream(_streams);
        }

        public override object Clone()
        {
            AssertNotDisposed();

            var clone = (VoronIndexInput)base.Clone();
            GC.SuppressFinalize(clone);
            clone._isOriginal = false;

            if (clone._originalTransactionId != clone._currentTransaction.Value.LowLevelTransaction.Id)
                clone.OpenInternal();
            else
            {
                for (int i = 0; i < _streams.Length; i++)
                {
                    clone._streams[i] = new MmapStream(clone._basePtrs[i], clone._chunkSizes[i])
                    {
                        Position = _streams[i].Position
                    };
                }

                clone._stream = new CombinedReadingStream(clone._streams);
            }

            return clone;
        }

        public override byte ReadByte()
        {
            AssertNotDisposed();

            var readByte = _stream.ReadByte();
            if (readByte == -1)
                throw new EndOfStreamException();
            return (byte)readByte;
        }

        public override void ReadBytes(byte[] b, int offset, int len)
        {
            AssertNotDisposed();

            _stream.ReadEntireBlock(b, offset, len);
        }

        public override void Seek(long pos)
        {
            AssertNotDisposed();

            _stream.Seek(pos, SeekOrigin.Begin);
        }

        protected override void Dispose(bool disposing)
        {
            GC.SuppressFinalize(this);

            if (_isOriginal == false)
                return;

            _cts.Cancel();
        }

        public override long Length()
        {
            return _stream.Length;
        }

        public override long FilePointer
        {
            get
            {
                AssertNotDisposed();

                return _stream.Position;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AssertNotDisposed()
        {
            if (_cts.IsCancellationRequested)
                throw new ObjectDisposedException("VoronIndexInput");
        }
    }
}