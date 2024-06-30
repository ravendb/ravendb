using System;
using System.Diagnostics.CodeAnalysis;
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
    public sealed class VoronIndexInput : IndexInput
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

            _stream = OpenVoronStream(transaction, _directory, _name, _tree);
        }

        public override string ToString()
        {
            return _name;
        }

        internal static VoronStream OpenVoronStream(Transaction transaction, LuceneVoronDirectory directory, string name, string tree)
        {
            if (transaction.IsWriteTransaction == false)
            {
                if (transaction.LowLevelTransaction.CurrentStateRecord.ClientState is IndexTransactionCache cache)
                {
                    if (cache.DirectoriesByName.TryGetValue(directory.Name, out var files))
                    {
                        if (files.ChunksByName.TryGetValue(name, out var details))
                        {
                            // we don't dispose here explicitly, the fileName needs to be
                            // alive as long as the transaction is
                            Slice.From(transaction.Allocator, name, out Slice fileName);
                            return new VoronStream(fileName, details, transaction.LowLevelTransaction);
                        }
                    }
                }
            }

            var fileTree = transaction.ReadTree(tree);
            if (fileTree == null)
                throw new FileNotFoundException($"Could not find '{tree}' tree for index input", name);

            using (Slice.From(transaction.Allocator, name, out Slice fileName))
            {
                var stream = fileTree.ReadStream(fileName);
                if (stream == null)
                    throw new FileNotFoundException("Could not find index input", name);

                return stream;
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

            clone._stream = OpenVoronStream(state.Transaction, clone._directory, clone._name, clone._tree);
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

            if (state.Transaction.LowLevelTransaction.IsValid == false)
                ThrowTransactionDisposed();
            if (_cts.IsCancellationRequested)
                ThrowCancelled();
        }

        [DoesNotReturn]
        private static void ThrowTransactionDisposed()
        {
            throw new ObjectDisposedException("No Transaction in thread");
        }

        [DoesNotReturn]
        private static void ThrowDisposed()
        {
            throw new ObjectDisposedException("No Transaction in thread");
        }

        [DoesNotReturn]
        private static void ThrowCancelled()
        {
            throw new OperationCanceledException("VoronIndexInput");
        }

        [DoesNotReturn]
        private static void ThrowStateNullException()
        {
            throw new ArgumentNullException("State");
        }

        [DoesNotReturn]
        private void ThrowEndOfStreamException()
        {
            throw new EndOfStreamException($"Input name: {_name}. Current position: {_stream.Position}, length: {_stream.Length}");
        }

        [DoesNotReturn]
        private void ThrowInvalidSeekPosition(long pos)
        {
            throw new InvalidOperationException($"Cannot set stream position to {pos} because the length of '{_name}' stream is {_stream.Length}");
        }
    }
}
