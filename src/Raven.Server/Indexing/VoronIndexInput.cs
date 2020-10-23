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

        private readonly LuceneVoronDirectory _directory;
        private readonly string _name;
        private readonly string _tree;
        private VoronStream _stream;

        private bool _isOriginal = true;

        public VoronIndexInput(LuceneVoronDirectory directory, string name, Transaction transaction, string tree)
        {
            _directory = directory;
            _name = name;
            _tree = tree;

            OpenInternal(transaction);
        }

        public override string ToString()
        {
            return _name;
        }

        private void OpenInternal(Transaction transaction)
        {
            if (transaction.IsWriteTransaction == false)
            {
                if (transaction.LowLevelTransaction.ImmutableExternalState is IndexTransactionCache cache)
                {
                    if (cache.DirectoriesByName.TryGetValue(_directory.Name, out var files))
                    {
                        if (files.ChunksByName.TryGetValue(_name, out var details))
                        {
                            // we don't dispose here explicitly, the fileName needs to be
                            // alive as long as the transaction is
                            Slice.From(transaction.Allocator, _name, out Slice fileName);
                            _stream = new VoronStream(fileName, details, transaction.LowLevelTransaction);
                            return;
                        }
                    }
                }
            }

            var fileTree = transaction.ReadTree(_tree);
            if (fileTree == null)
                throw new FileNotFoundException($"Could not find '{_tree}' tree for index input", _name);

            using (Slice.From(transaction.Allocator, _name, out Slice fileName))
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
            if (!(s is VoronState state))
            {
                ThrowStateNullException();
                return byte.MinValue;
            }

            ThrowIfDisposed(state);

            _stream.UpdateCurrentTransaction(state.Transaction);
            var readByte = _stream.ReadByte();
            if (readByte == -1)
                ThrowEndOfStreamException();

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
            if (pos > _stream.Length)
                ThrowInvalidSeekPosition(pos);

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

        private void ThrowEndOfStreamException()
        {
            throw new EndOfStreamException($"Input name: {_name}. Current position: {_stream.Position}, length: {_stream.Length}");
        }

        private void ThrowInvalidSeekPosition(long pos)
        {
            throw new InvalidOperationException($"Cannot set stream position to {pos} because the length of '{_name}' stream is {_stream.Length}");
        }
    }
}
