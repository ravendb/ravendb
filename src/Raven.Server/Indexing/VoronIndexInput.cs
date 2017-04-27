using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using Lucene.Net.Store;
using Raven.Client.Extensions.Streams;
using Voron;
using Voron.Data;
using Voron.Impl;

namespace Raven.Server.Indexing
{
    public class VoronIndexInput : IndexInput
    {
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        private readonly string _name;
        private VoronStream _stream;

        private bool _isOriginal = true;

        public VoronIndexInput(string name, Transaction transaction)
        {
            _name = name;

            OpenInternal(transaction);
        }

        public override string ToString()
        {
            return _name;
        }

        private void OpenInternal(Transaction transaction)
        {
            var fileTree = transaction.ReadTree("Files");
            if (fileTree == null)
                throw new FileNotFoundException("Could not find index input", _name);

            Slice fileName;
            using (Slice.From(transaction.Allocator, _name, out fileName))
            {
                _stream = fileTree.ReadStream(fileName);
                if (_stream == null)
                    throw new FileNotFoundException("Could not find index input", _name);
            }
        }

        public override object Clone(IState s)
        {
            var state = s as VoronState;
            if (state == null)
            {
                ThrowStateNullException();
                return null;
            }

            ThrowIfDisposed(state);

            var clone = (VoronIndexInput)base.Clone(s);
            GC.SuppressFinalize(clone);
            clone._isOriginal = false;

            clone.OpenInternal(state.Transaction);
            clone._stream.Position = _stream.Position;

            return clone;
        }

        public override byte ReadByte(IState s)
        {
            var state = s as VoronState;
            if (state == null)
            {
                ThrowStateNullException();
                return byte.MinValue;
            }

            ThrowIfDisposed(state);

            _stream.UpdateCurrentTransaction(state.Transaction);
            var readByte = _stream.ReadByte();
            if (readByte == -1)
                throw new EndOfStreamException();

            return (byte)readByte;
        }

        public override void ReadBytes(byte[] buffer, int offset, int len, IState s)
        {
            var state = s as VoronState;
            if (state == null)
            {
                ThrowStateNullException();
                return;
            }

            ThrowIfDisposed(state);

            _stream.UpdateCurrentTransaction(state.Transaction);
            _stream.ReadEntireBlock(buffer, offset, len);
        }

        public override void Seek(long pos, IState s)
        {
            var state = s as VoronState;
            if (state == null)
            {
                ThrowStateNullException();
                return;
            }

            ThrowIfDisposed(state);

            _stream.Seek(pos, SeekOrigin.Begin);
        }

        protected override void Dispose(bool disposing)
        {
            GC.SuppressFinalize(this);

            if (_isOriginal == false)
                return;

            _cts.Cancel();
            _cts.Dispose();
        }

        public override long Length(IState s)
        {
            var state = s as VoronState;
            if (state == null)
            {
                ThrowStateNullException();
                return 0;
            }

            return _stream.Length;
        }

        public override long FilePointer(IState s)
        {
            var state = s as VoronState;
            if (state == null)
            {
                ThrowStateNullException();
                return 0;
            }

            ThrowIfDisposed(state);

            return _stream.Position;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ThrowIfDisposed(VoronState state)
        {
            if (state == null)
            {
                ThrowDisposed();
                return; // never hit
            }
            if (state.Transaction.LowLevelTransaction.IsDisposed)
                ThrowTransactionDisposed();
            if (_cts.IsCancellationRequested)
                ThrowCancelled();
        }

        private static void ThrowTransactionDisposed()
        {
            throw new ObjectDisposedException("No Transaction in thread");
        }

        private static void ThrowDisposed()
        {
            throw new ObjectDisposedException("No Transaction in thread");
        }

        private static void ThrowCancelled()
        {
            throw new OperationCanceledException("VoronIndexInput");
        }

        private static void ThrowStateNullException()
        {
            throw new ArgumentNullException("State");
        }
    }
}