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
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Impl;
using Voron.Util;

namespace Raven.Server.Indexing
{
    public unsafe class VoronIndexInput : IndexInput
    {
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        private readonly AsyncLocal<Transaction> _currentTransaction;

        private readonly string _name;
        private ChunkedSparseMmapStream _stream;

        private bool _isOriginal = true;

        public VoronIndexInput(AsyncLocal<Transaction> transaction, string name)
        {
            _name = name;
            _currentTransaction = transaction;

            OpenInternal();
        }

        private void OpenInternal()
        {
            var fileTree = _currentTransaction.Value.ReadTree("Files");
            if (fileTree == null)
                throw new FileNotFoundException("Could not find index input", _name);

            Slice fileName;
            using (Slice.From(_currentTransaction.Value.Allocator, _name, out fileName))
            {
                _stream = fileTree.ReadStream(fileName);
                if (_stream == null)
                    throw new FileNotFoundException("Could not find index input", _name);
            }          
        }

        public override object Clone()
        {
            ThrowIfDisposed();
            ThrowIfCancellationRequested();

            var clone = (VoronIndexInput)base.Clone();
            GC.SuppressFinalize(clone);
            clone._isOriginal = false;

            clone.OpenInternal();
            clone._stream.Position = _stream.Position;

            return clone;
        }

        public override byte ReadByte()
        {
            ThrowIfDisposed();
            ThrowIfCancellationRequested();

            _stream.UpdateCurrentTransaction(_currentTransaction.Value);
            var readByte = _stream.ReadByte();
            if (readByte == -1)
                throw new EndOfStreamException();

            return (byte)readByte;
        }

        public override void ReadBytes(byte[] buffer, int offset, int len)
        {
            ThrowIfDisposed();
            ThrowIfCancellationRequested();

            _stream.UpdateCurrentTransaction(_currentTransaction.Value);
            _stream.ReadEntireBlock(buffer, offset, len);
        }

        public override void Seek(long pos)
        {
            ThrowIfDisposed();
            ThrowIfCancellationRequested();

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
                ThrowIfDisposed();
                ThrowIfCancellationRequested();
                return _stream.Position;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ThrowIfDisposed()
        {
            if(_currentTransaction.Value == null)
                throw new ObjectDisposedException("No Transaction in thread");
            if (_currentTransaction.Value.LowLevelTransaction.IsDisposed)
                throw new ObjectDisposedException("No Transaction in thread");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ThrowIfCancellationRequested()
        {
            if (_cts.IsCancellationRequested)
                throw new OperationCanceledException("VoronIndexInput");
        }
                    
    }
}