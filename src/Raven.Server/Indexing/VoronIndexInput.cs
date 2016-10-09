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
using Voron.Util;

namespace Raven.Server.Indexing
{
    public unsafe class VoronIndexInput : IndexInput
    {
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        private readonly Transaction _originalTransaction;
        private readonly AsyncLocal<Transaction> _currentTransaction;

        private readonly string _name;
        private ChunkedMmapStream _stream;

        private bool _isOriginal = true;
        private PtrSize[] _ptrs;

        public VoronIndexInput(AsyncLocal<Transaction> transaction, string name)
        {
            _name = name;
            _originalTransaction = transaction.Value;
            _currentTransaction = transaction;

            OpenInternal();
        }

        private void OpenInternal()
        {
            var fileTree = _currentTransaction.Value.ReadTree(_name);

            if (fileTree == null)
                throw new FileNotFoundException("Could not find index input", _name);

            var numberOfChunks = fileTree.State.NumberOfEntries;

            _ptrs = new PtrSize[numberOfChunks];

            int index = 0;

            using (var it = fileTree.Iterate(prefetch: false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    throw new InvalidDataException("Could not seek to any chunk of this file");

                do
                {
                    var readResult = fileTree.Read(it.CurrentKey);

                    _ptrs[index] = PtrSize.Create(readResult.Reader.Base, readResult.Reader.Length);

                    index++;
                } while (it.MoveNext());
            }
            
            if (numberOfChunks != index)
                throw new InvalidDataException($"Read invalid number of file chunks. Expected {numberOfChunks}, read {index}.");

            _stream = new ChunkedMmapStream(_ptrs, VoronIndexOutput.MaxFileChunkSize);
        }

        public override object Clone()
        {
            AssertNotDisposed();

            var clone = (VoronIndexInput)base.Clone();
            GC.SuppressFinalize(clone);
            clone._isOriginal = false;

            if (clone._originalTransaction != clone._currentTransaction.Value)
            {
                clone.OpenInternal();
                clone._stream.Position = _stream.Position;
            }
            else
            {
                clone._stream = new ChunkedMmapStream(_ptrs, VoronIndexOutput.MaxFileChunkSize)
                {
                    Position = _stream.Position
                };
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

        public override void ReadBytes(byte[] buffer, int offset, int len)
        {
            AssertNotDisposed();

            _stream.ReadEntireBlock(buffer, offset, len);
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
            if(_currentTransaction.Value == null)
                throw new ObjectDisposedException("No Transaction in thread");
            if (_currentTransaction.Value.LowLevelTransaction.IsDisposed)
                throw new ObjectDisposedException("No Transaction in thread");
        }
    }
}