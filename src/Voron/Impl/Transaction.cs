using Sparrow;
using System;
using System.Collections.Generic;

using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Global;

namespace Voron.Impl
{
    public unsafe class Transaction : IDisposable
    {
        private Dictionary<Tuple<Tree, Slice>, Tree> _multiValueTrees;
        private readonly LowLevelTransaction _lowLevelTransaction;

        public LowLevelTransaction LowLevelTransaction
        {
            get { return _lowLevelTransaction; }
        }

        private readonly Dictionary<string, Tree> _trees = new Dictionary<string, Tree>();
        private readonly Dictionary<string, Table> _tables = new Dictionary<string, Table>();

        public Transaction(LowLevelTransaction lowLevelTransaction)
        {
            _lowLevelTransaction = lowLevelTransaction;
        }

        public ByteStringContext Allocator
        {
            get { return _lowLevelTransaction.Allocator; }
        }

        public Tree ReadTree(string treeName, RootObjectType type = RootObjectType.VariableSizeTree)
        {
            Tree tree;
            if (_trees.TryGetValue(treeName, out tree))
                return tree;

            Slice treeNameSlice = Slice.From(this.Allocator, treeName, ByteStringType.Immutable);

            var header = (TreeRootHeader*)_lowLevelTransaction.RootObjects.DirectRead(treeNameSlice);
            if (header != null)
            {
                if (header->RootObjectType != type)
                    throw new InvalidOperationException($"Tried to opened {treeName} as a { type }, but it is actually a " + header->RootObjectType);

                tree = Tree.Open(_lowLevelTransaction, this, header, type);
                tree.Name = treeName;
                _trees.Add(treeName, tree);
                return tree;
            }

            _trees.Add(treeName, null);
            return null;
        }

        public IEnumerable<Tree> Trees
        {
            get { return _trees.Values; }
        }

        public void Commit()
        {
            if (_lowLevelTransaction.Flags != (TransactionFlags.ReadWrite))
                return; // nothing to do

            PrepareForCommit();
            _lowLevelTransaction.Commit();
        }

        public Table OpenTable(TableSchema schema, string name)
        {
            Table openTable;
            if (_tables.TryGetValue(name, out openTable))
                return openTable;

            openTable = new Table(schema, name, this, 1);
            _tables[name] = openTable;

            return openTable;
        }

        internal void PrepareForCommit()
        {
            if (_multiValueTrees != null)
            {
                foreach (var multiValueTree in _multiValueTrees)
                {
                    var parentTree = multiValueTree.Key.Item1;
                    var key = multiValueTree.Key.Item2;
                    var childTree = multiValueTree.Value;

                    var trh = (TreeRootHeader*)parentTree.DirectAdd(key, sizeof(TreeRootHeader), TreeNodeFlags.MultiValuePageRef);
                    childTree.State.CopyTo(trh);
                }
            }

            foreach (var tree in Trees)
            {
                if (tree == null)
                    continue;

                tree.State.InWriteTransaction = false;
                var treeState = tree.State;
                if (treeState.IsModified)
                {
                    var treePtr = (TreeRootHeader*)_lowLevelTransaction.RootObjects.DirectAdd(tree.Name, sizeof(TreeRootHeader));
                    treeState.CopyTo(treePtr);
                }
            }

            foreach (var participant in _tables.Values)
            {
                participant.PrepareForCommit();
            }
        }

        internal void AddMultiValueTree(Tree tree, Slice key, Tree mvTree)
        {
            if (_multiValueTrees == null)
                _multiValueTrees = new Dictionary<Tuple<Tree, Slice>, Tree>(new TreeAndSliceComparer());
            mvTree.IsMultiValueTree = true;
            _multiValueTrees.Add(Tuple.Create(tree, key.Clone(_lowLevelTransaction.Allocator, ByteStringType.Immutable)), mvTree);
        }

        internal bool TryGetMultiValueTree(Tree tree, Slice key, out Tree mvTree)
        {
            mvTree = null;
            if (_multiValueTrees == null)
                return false;
            return _multiValueTrees.TryGetValue(Tuple.Create(tree, key), out mvTree);
        }

        internal bool TryRemoveMultiValueTree(Tree parentTree, Slice key)
        {
            var keyToRemove = Tuple.Create(parentTree, key);
            if (_multiValueTrees == null || !_multiValueTrees.ContainsKey(keyToRemove))
                return false;

            return _multiValueTrees.Remove(keyToRemove);
        }


        internal void AddTree(string name, Tree tree)
        {
            Tree value;
            if (_trees.TryGetValue(name, out value) && value != null)
            {
                throw new InvalidOperationException("Tree already exists: " + name);
            }
            _trees[name] = tree;
        }



        public void DeleteTree(string name)
        {
            if (_lowLevelTransaction.Flags == (TransactionFlags.ReadWrite) == false)
                throw new ArgumentException("Cannot create a new newRootTree with a read only transaction");

            Tree tree = ReadTree(name);
            if (tree == null)
                return;

            foreach (var page in tree.AllPages())
            {
                _lowLevelTransaction.FreePage(page);
            }

            _lowLevelTransaction.RootObjects.Delete(name);

            if (_multiValueTrees != null)
            {
                var toRemove = new List<Tuple<Tree, Slice>>();

                foreach (var valueTree in _multiValueTrees)
                {
                    var multiTree = valueTree.Key.Item1;

                    if (multiTree.Name == name)
                    {
                        toRemove.Add(valueTree.Key);
                    }
                }

                foreach (var recordToRemove in toRemove)
                {
                    _multiValueTrees.Remove(recordToRemove);
                }
            }

            _trees.Remove(name);
        }

        public void RenameTree(string fromName, string toName)
        {
            if (_lowLevelTransaction.Flags == (TransactionFlags.ReadWrite) == false)
                throw new ArgumentException("Cannot rename a new tree with a read only transaction");

            if (toName.Equals(Constants.RootTreeName, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Cannot create a tree with reserved name: " + toName);

            if (ReadTree(toName) != null)
                throw new ArgumentException("Cannot rename a tree with the name of an existing tree: " + toName);

            Tree fromTree = ReadTree(fromName);
            if (fromTree == null)
                throw new ArgumentException("Tree " + fromName + " does not exists");

            Slice key = Slice.From(this.Allocator, toName, ByteStringType.Immutable);

            _lowLevelTransaction.RootObjects.Delete(fromName);
            var ptr = _lowLevelTransaction.RootObjects.DirectAdd(key, sizeof(TreeRootHeader));
            fromTree.State.CopyTo((TreeRootHeader*)ptr);
            fromTree.Name = toName;
            fromTree.State.IsModified = true;

            _trees.Remove(fromName);
            _trees.Remove(toName);

            AddTree(toName, fromTree);
        }

        public Tree CreateTree(string name, RootObjectType type = RootObjectType.VariableSizeTree)
        {
            Tree tree = ReadTree(name, type);
            if (tree != null)
                return tree;

            if (_lowLevelTransaction.Flags == (TransactionFlags.ReadWrite) == false)
                throw new InvalidOperationException("No such tree: '" + name + "' and cannot create trees in read transactions");

            Slice key = Slice.From(this.Allocator, name, ByteStringType.Immutable);

            tree = Tree.Create(_lowLevelTransaction, this);
            tree.Name = name;
            tree.State.RootObjectType = type;

            var space = (TreeRootHeader*) _lowLevelTransaction.RootObjects.DirectAdd(key, sizeof(TreeRootHeader));
            tree.State.CopyTo(space);

            tree.State.IsModified = true;
            AddTree(name, tree);

            return tree;
        }


        public void Dispose()
        {
            _lowLevelTransaction?.Dispose();
        }

        public FixedSizeTree FixedTreeFor(Slice treeName)
        {
            var valueSize = FixedSizeTree.GetValueSize(LowLevelTransaction, LowLevelTransaction.RootObjects, treeName);
            return FixedTreeFor(treeName, valueSize);
        }

        public FixedSizeTree FixedTreeFor(Slice treeName, ushort valSize)
        {
            return new FixedSizeTree(LowLevelTransaction, LowLevelTransaction.RootObjects, treeName, valSize);
        }

        public RootObjectType GetRootObjectType(Slice name)
        {
            var val = (RootHeader*) _lowLevelTransaction.RootObjects.DirectRead(name);
            if (val == null)
                return RootObjectType.None;

            return val->RootObjectType;
        }
    }

    public static class TransactionLegacyExtensions
    {
        public static TreePage GetReadOnlyTreePage(this LowLevelTransaction tx, long pageNumber)
        {
            return tx.GetPage(pageNumber).ToTreePage();
        }

        public static FixedSizeTreePage GetReadOnlyFixedSizeTreePage(this LowLevelTransaction tx, long pageNumber)
        {
            return tx.GetPage(pageNumber).ToFixedSizeTreePage();
        }
    }
}
