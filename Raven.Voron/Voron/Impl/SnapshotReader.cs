using System.Collections.Generic;
using Voron.Util;

namespace Voron.Impl
{
    using System;
    using System.IO;
    using System.Runtime.CompilerServices;
    using Voron.Trees;

    public class SnapshotReader : IDisposable
    {
        private bool _disposed;
        private LinkedList<IIterator> _openedIterators; 
        public SnapshotReader(Transaction tx)
        {
            Transaction = tx;
        }

        public Transaction Transaction { get; private set; }
        public bool HasOpenedIterators { get { return _openedIterators != null && _openedIterators.Count > 0; } }

        public ReadResult Read(string treeName, Slice key, WriteBatch writeBatch = null)
        {
            if (_disposed)
                throw new ObjectDisposedException("SnapshotReader");
            Tree tree = null;

            if (writeBatch != null && writeBatch.IsEmpty == false)
            {
                WriteBatch.InBatchValue result;
                if (writeBatch.TryGetValue(treeName, key, out result))
                {
                    if (!result.Version.HasValue)
                        tree = GetTree(treeName);

                    switch (result.OperationType)
                    {
                        case WriteBatch.BatchOperationType.Add:
                        {
                            var reader = new ValueReader(result.Stream);
                            return new ReadResult(reader, result.Version.HasValue ? (ushort) (result.Version.Value + 1) : tree.ReadVersion(key));
                        }
                        case WriteBatch.BatchOperationType.Delete:
                            return null;
                    }
                }
            }

            if (tree == null)
                tree = GetTree(treeName);

            return tree.Read(key);
        }

        public StructReadResult<T> ReadStruct<T>(string treeName, Slice key, StructureSchema<T> schema, WriteBatch writeBatch = null)
        {
            if (_disposed)
                throw new ObjectDisposedException("SnapshotReader");
            Tree tree = null;

            if (writeBatch != null && writeBatch.IsEmpty == false)
            {
                WriteBatch.InBatchValue result;
                if (writeBatch.TryGetValue(treeName, key, out result))
                {
                    if (!result.Version.HasValue)
                        tree = GetTree(treeName);

                    switch (result.OperationType)
                    {
                        case WriteBatch.BatchOperationType.AddStruct:
                            return new StructReadResult<T>(new StructureReader<T>((Structure<T>) result.Struct, schema),  result.Version.HasValue ? (ushort)(result.Version.Value + 1) : tree.ReadVersion(key));
                        case WriteBatch.BatchOperationType.Delete:
                            return null;
                    }
                }
            }

            if (tree == null)
                tree = GetTree(treeName);

            return tree.ReadStruct(key, schema);
        }

        public int GetDataSize(string treeName, Slice key)
        {
            if (_disposed)
                throw new ObjectDisposedException("SnapshotReader");
            var tree = GetTree(treeName);
            return tree.GetDataSize(key);
        }

        public bool Contains(string treeName, Slice key, out ushort? version, WriteBatch writeBatch = null)
        {
            if (_disposed)
                throw new ObjectDisposedException("SnapshotReader");
            if (writeBatch != null && writeBatch.IsEmpty == false)
            {
                WriteBatch.InBatchValue result;
                if (writeBatch.TryGetValue(treeName, key, out result))
                {
                    version = result.Version;

                    switch (result.OperationType)
                    {
                        case WriteBatch.BatchOperationType.Add:
                            return true;
                        case WriteBatch.BatchOperationType.AddStruct:
                            return true;
                        case WriteBatch.BatchOperationType.Delete:
                            return false;
                        default:
                            throw new ArgumentOutOfRangeException(result.OperationType.ToString());
                    }
                }
            }

            var tree = GetTree(treeName);
            var readVersion = tree.ReadVersion(key);

            var exists = readVersion > 0;

            version = exists ? (ushort?)readVersion : null;

            return exists;
        }

        public ushort ReadVersion(string treeName, Slice key, WriteBatch writeBatch = null)
        {
            if (_disposed)
                throw new ObjectDisposedException("SnapshotReader");
            if (writeBatch != null)
            {
                WriteBatch.InBatchValue result;
                if (writeBatch.TryGetValue(treeName, key, out result) && result.Version.HasValue)
                {
                    switch (result.OperationType)
                    {
                        case WriteBatch.BatchOperationType.Add:
                        case WriteBatch.BatchOperationType.AddStruct:
                        case WriteBatch.BatchOperationType.Delete:
                            return (ushort)(result.Version.Value + 1);
                    }
                }
            }

            var tree = GetTree(treeName);
            return tree.ReadVersion(key);
        }

        public IIterator Iterate(string treeName)
        {
            var tree = GetTree(treeName);
            var treeIterator = tree.Iterate();
            RegisterOpenedIterator(treeIterator);
            return treeIterator;
        }

        private void RegisterOpenedIterator(IIterator it)
        {
            if (_openedIterators == null)
                _openedIterators = new LinkedList<IIterator>();
            _openedIterators.AddLast(it);
            it.OnDispoal += RemoveFromTrackedIterators;
        }

        private void RemoveFromTrackedIterators(IIterator treeIterator)
        {
            if (_openedIterators == null)
                return; // should never happen, except during disposal
            // this is O(N), but we don't expect to have many concurrent iterators, and the 
            // memory utilization is important, so we use a linked list.
            _openedIterators.Remove(treeIterator);
        }

        public void Dispose()
        {
            _disposed = true;
            Transaction.Dispose();
            
            if (_openedIterators != null)
            {
                var copy = _openedIterators;
                _openedIterators = null;
                foreach (var openedIterator in copy)
                {
                    openedIterator.Dispose();
                }
            }
        }

        public IIterator MultiRead(string treeName, Slice key)
        {
            var tree = GetTree(treeName);
            var it = tree.MultiRead(key);
            RegisterOpenedIterator(it);
            return it;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Tree GetTree(string treeName)
        {
            if (_disposed)
                throw new ObjectDisposedException("SnapshotReader");
            var tree = treeName == null ? Transaction.Root : Transaction.Environment.CreateTree(Transaction, treeName);
            return tree;
        }
    }
}
