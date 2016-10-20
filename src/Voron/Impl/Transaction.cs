using Sparrow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
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

        public LowLevelTransaction LowLevelTransaction => _lowLevelTransaction;

        public ByteStringContext Allocator => _lowLevelTransaction.Allocator;

        private Dictionary<Slice, Table> _tables;

        private Dictionary<Slice, Tree> _trees;

        private Dictionary<Slice, FixedSizeTree> _globalFixedSizeTree;

        public IEnumerable<Tree> Trees => _trees == null ? Enumerable.Empty<Tree>() : _trees.Values;

        public Transaction(LowLevelTransaction lowLevelTransaction)
        {
            _lowLevelTransaction = lowLevelTransaction;
        }

        private void EnsureTrees()
        {
            if (_trees != null)
                return;
            _trees = new Dictionary<Slice, Tree>(SliceComparer.Instance);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public Tree ReadTree(string treeName, RootObjectType type = RootObjectType.VariableSizeTree)
        {
            Slice treeNameSlice;
            Slice.From(Allocator, treeName, ByteStringType.Immutable, out treeNameSlice);
            return ReadTree(treeNameSlice, type);
        }

        public Tree ReadTree(Slice treeName, RootObjectType type = RootObjectType.VariableSizeTree)
        {
            EnsureTrees();

            Tree tree;
            if (_trees.TryGetValue(treeName, out tree))
                return tree;

            TreeRootHeader* header = (TreeRootHeader*)_lowLevelTransaction.RootObjects.DirectRead(treeName);
            if (header != null)
            {
                if (header->RootObjectType != type)
                    throw new InvalidOperationException($"Tried to opened {treeName} as a {type}, but it is actually a " + header->RootObjectType);

                tree = Tree.Open(_lowLevelTransaction, this, header, type);
                tree.Name = treeName;
                _trees.Add(treeName, tree);
                return tree;
            }

            _trees.Add(treeName, null);
            return null;
        }

        public void Commit()
        {
            if (_lowLevelTransaction.Flags != TransactionFlags.ReadWrite)
                return; // nothing to do

            PrepareForCommit();
            _lowLevelTransaction.Commit();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public Table OpenTable(TableSchema schema, string name)
        {
            Slice nameSlice;
            Slice.From(Allocator, name, ByteStringType.Immutable, out nameSlice);
            return OpenTable(schema, nameSlice);
        }

        public Table OpenTable(TableSchema schema, Slice name)
        {
            if(_tables == null)
                _tables = new Dictionary<Slice, Table>(SliceComparer.Instance);

            Table openTable;
            if (_tables.TryGetValue(name, out openTable))
                return openTable;

            openTable = new Table(schema, name, this, 1);
            _tables.Add(name, openTable);

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

            if (_tables != null)
            {
                foreach (var participant in _tables.Values)
                {
                    participant.PrepareForCommit();
                }
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


        [MethodImpl(MethodImplOptions.NoInlining)]
        internal void AddTree(string name, Tree tree)
        {
            Slice treeName;
            Slice.From(Allocator, name, ByteStringType.Immutable, out treeName);
            AddTree(treeName, tree);
        }

        internal void AddTree(Slice name, Tree tree)
        {
            EnsureTrees();

            Tree value;
            if (_trees.TryGetValue(name, out value) && value != null)
            {
                throw new InvalidOperationException("Tree already exists: " + name);
            }

            // Either we haven't added this tree, or we added it as null (meaning it didn't exist)
            _trees[name] = tree;
        }


        [MethodImpl(MethodImplOptions.NoInlining)]
        public void DeleteTree(string name)
        {
            Slice nameSlice;
            Slice.From(Allocator, name, ByteStringType.Immutable, out nameSlice);
            DeleteTree(nameSlice);
        }

        public void DeleteTree(Slice name)
        {
            if (_lowLevelTransaction.Flags == TransactionFlags.ReadWrite == false)
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

                    if (SliceComparer.Equals(multiTree.Name, name))
                    {
                        toRemove.Add(valueTree.Key);
                    }
                }

                foreach (var recordToRemove in toRemove)
                {
                    _multiValueTrees.Remove(recordToRemove);
                }
            }
            // already created in ReadTree
            _trees.Remove(name);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void RenameTree(string fromName, string toName)
        {
            Slice fromNameSlice;
            Slice toNameSlice;
            Slice.From(Allocator, fromName, ByteStringType.Immutable, out fromNameSlice);
            Slice.From(Allocator, toName, ByteStringType.Immutable, out toNameSlice);
            RenameTree(fromNameSlice, toNameSlice);
        }

        public void RenameTree(Slice fromName, Slice toName)
        {
            if (_lowLevelTransaction.Flags == TransactionFlags.ReadWrite == false)
                throw new ArgumentException("Cannot rename a new tree with a read only transaction");

            if (SliceComparer.Equals(toName, Constants.RootTreeNameSlice))
                throw new InvalidOperationException("Cannot create a tree with reserved name: " + toName);

            if (ReadTree(toName) != null)
                throw new ArgumentException("Cannot rename a tree with the name of an existing tree: " + toName);

            Tree fromTree = ReadTree(fromName);
            if (fromTree == null)
                throw new ArgumentException("Tree " + fromName + " does not exists");

            _lowLevelTransaction.RootObjects.Delete(fromName);

            var ptr = _lowLevelTransaction.RootObjects.DirectAdd(toName, sizeof(TreeRootHeader));
            fromTree.State.CopyTo((TreeRootHeader*) ptr);
            fromTree.Name = toName;
            fromTree.State.IsModified = true;

            // _trees already ensrued already created in ReadTree
            _trees.Remove(fromName);
            _trees.Remove(toName);

            AddTree(toName, fromTree);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public Tree CreateTree(string name, RootObjectType type = RootObjectType.VariableSizeTree)
        {
            Slice treeNameSlice;
            Slice.From(Allocator, name, ByteStringType.Immutable, out treeNameSlice);
            return CreateTree(treeNameSlice, type);
        }

        public Tree CreateTree(Slice name, RootObjectType type = RootObjectType.VariableSizeTree)
        {
            Tree tree = ReadTree(name, type);
            if (tree != null)
                return tree;

            if (_lowLevelTransaction.Flags == TransactionFlags.ReadWrite == false)
                throw new InvalidOperationException("No such tree: '" + name +
                                                    "' and cannot create trees in read transactions");

            tree = Tree.Create(_lowLevelTransaction, this);
            tree.Name = name;
            tree.State.RootObjectType = type;

            var space = (TreeRootHeader*)_lowLevelTransaction.RootObjects.DirectAdd(name, sizeof(TreeRootHeader));
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
            var val = (RootHeader*)_lowLevelTransaction.RootObjects.DirectRead(name);
            if (val == null)
                return RootObjectType.None;

            return val->RootObjectType;
        }

        public FixedSizeTree GetGlobalFixedSizeTree(Slice name, ushort valSize)
        {
            if (_globalFixedSizeTree == null)
                _globalFixedSizeTree = new Dictionary<Slice, FixedSizeTree>(SliceComparer.Instance);

            FixedSizeTree tree;
            if (_globalFixedSizeTree.TryGetValue(name, out tree) == false)
            {
                tree = new FixedSizeTree(LowLevelTransaction, LowLevelTransaction.RootObjects, name, valSize);
                _globalFixedSizeTree[tree.Name] = tree;
            }
            return tree;
        }
    }
}
