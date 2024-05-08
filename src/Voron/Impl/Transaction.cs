using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Data.RawData;
using Voron.Data.PostingLists;
using Constants = Voron.Global.Constants;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using Voron.Impl.Paging;
using static Sparrow.PortableExceptions;
using static Voron.VoronExceptions;

namespace Voron.Impl
{
    public sealed unsafe class Transaction : IDisposable, IDisposableQueryable
    {
        public LowLevelTransaction LowLevelTransaction
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _lowLevelTransaction; }
        }

        public object Owner;

        public ByteStringContext Allocator => _lowLevelTransaction.Allocator;

        private Dictionary<long, Container.TransactionState> _containers;
        
        private Dictionary<Slice, PostingList> _postingLists;
        
        private Dictionary<TableKey, Table> _tables;
        private Dictionary<Slice, TableSchemaStatsReference> _tableSchemaStats;

        private Dictionary<Slice, Tree> _trees;

        private Dictionary<Slice, FixedSizeTree> _globalFixedSizeTree;

        public IEnumerable<Tree> Trees => _trees?.Values ?? Enumerable.Empty<Tree>();
        
        public IEnumerable<Table> Tables => _tables?.Values ?? Enumerable.Empty<Table>();

        public bool IsWriteTransaction => _lowLevelTransaction.Flags == TransactionFlags.ReadWrite;

        public event Action<Transaction> OnBeforeCommit;

        internal Dictionary<long, ByteString> CachedDecompressedBuffersByStorageId =>
            _cachedDecompressedBuffersByStorageId ??= new Dictionary<long, ByteString>();

        private LowLevelTransaction _lowLevelTransaction;
        private Dictionary<Tuple<Tree, Slice>, Tree> _multiValueTrees;
        private Dictionary<long, ByteString> _cachedDecompressedBuffersByStorageId;

        public Transaction(LowLevelTransaction lowLevelTransaction)
        {
            _lowLevelTransaction = lowLevelTransaction;
            _lowLevelTransaction.Transaction = this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureTrees()
        {
            if (_trees != null)
                return;
            _trees = new Dictionary<Slice, Tree>(SliceStructComparer.Instance);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public Tree ReadTree(string treeName, RootObjectType type = RootObjectType.VariableSizeTree, bool isIndexTree = false, NewPageAllocator newPageAllocator = null)
        {
            Slice.From(Allocator, treeName, ByteStringType.Immutable, out var treeNameSlice);
            return ReadTree(treeNameSlice, type, isIndexTree, newPageAllocator);
        }

        public Tree ReadTree(Slice treeName, RootObjectType type = RootObjectType.VariableSizeTree, bool isIndexTree = false, NewPageAllocator newPageAllocator = null)
        {
            EnsureTrees();

            if (_trees.TryGetValue(treeName, out var tree))
            {
                if (tree == null)
                    return null;

                if (newPageAllocator == null)
                    return tree;

                if (tree.HasNewPageAllocator == false)
                    tree.SetNewPageAllocator(newPageAllocator);

                return tree;
            }

            var header = (TreeRootHeader*)_lowLevelTransaction.RootObjects.DirectRead(treeName);
            if (header != null)
            {
                ThrowIf<InvalidOperationException>(header->RootObjectType != type, $"Tried to open {treeName} as a {type}, but it is actually a {header->RootObjectType}");
                    
                tree = Tree.Open(_lowLevelTransaction, this, treeName, *header, isIndexTree, newPageAllocator);

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

        public Transaction BeginAsyncCommitAndStartNewTransaction(TransactionPersistentContext persistentContext)
        {
            ThrowIfReadOnly(_lowLevelTransaction, "Cannot call begin async commit on read tx");

            PrepareForCommit();
            
            var tx = _lowLevelTransaction.BeginAsyncCommitAndStartNewTransaction(persistentContext);
            return new Transaction(tx);
        }


        public void EndAsyncCommit()
        {
            _lowLevelTransaction.EndAsyncCommit();
        }

        public long OpenContainer(string name)
        {
            using (Slice.From(Allocator, name, ByteStringType.Immutable, out Slice nameSlice))
            {
                return OpenContainer(nameSlice);
            }
        }

        public Lookup<TKey> LookupFor<TKey>(string name) where TKey : struct, ILookupKey
        {
            using (Slice.From(Allocator, name, ByteStringType.Immutable, out Slice nameSlice))
            {
                return LookupFor<TKey>(nameSlice);
            }
        }
        
        public Lookup<TKey> LookupFor<TKey>(Slice name) where TKey : struct, ILookupKey
        {
            return LowLevelTransaction.RootObjects.LookupFor<TKey>(name);
        }

        public bool TryGetLookupFor<TKey>(Slice name, out Lookup<TKey> lookup) where TKey : struct, ILookupKey
        {
            return LowLevelTransaction.RootObjects.TryGetLookupFor(name, out lookup);
        }
        
        public long OpenContainer(Slice name)
        {
            var exists = LowLevelTransaction.RootObjects.DirectRead(name);
            if (exists != null)
            {
                return ((ContainerRootHeader*)exists)->ContainerId;
            }
            var id = Container.Create(LowLevelTransaction);

            using (LowLevelTransaction.RootObjects.DirectAdd(name, sizeof(ContainerRootHeader), out var ptr))
                *((ContainerRootHeader*)ptr) = new ContainerRootHeader
                {
                    RootObjectType = RootObjectType.Container,
                    ContainerId = id
                };
            
            
            return id;
        }

        public PostingList OpenPostingList(string name)
        {
            using (Slice.From(Allocator, name, ByteStringType.Immutable, out Slice nameSlice))
            {
                return OpenPostingList(nameSlice);
            }
        }

        public PostingList OpenPostingList(Slice name)
        {
            _postingLists ??= new Dictionary<Slice, PostingList>(SliceStructComparer.Instance);
            if (_postingLists.TryGetValue(name, out var set))
                return set;

            var clonedName = name.Clone(Allocator);
            
            var existing = LowLevelTransaction.RootObjects.Read(name);
            if (existing == null)
            {
                var state = new PostingListState();
                PostingList.Create(this.LowLevelTransaction, ref state);
                using (LowLevelTransaction.RootObjects.DirectAdd(name, sizeof(PostingListState), out var p))
                {
                    Unsafe.Copy(p, ref state);
                }
                existing = LowLevelTransaction.RootObjects.Read(name);
            }
 
            set = new PostingList(LowLevelTransaction, clonedName,
                MemoryMarshal.AsRef<PostingListState>(existing.Reader.AsSpan())
            );
            _postingLists[clonedName] = set;
            return set;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public Table OpenTable(TableSchema schema, string name)
        {
            using (Slice.From(Allocator, name, ByteStringType.Immutable, out Slice nameSlice))
            {
                return OpenTable(schema, nameSlice);
            }
        }

        public Table OpenTable(TableSchema schema, Slice name)
        {
            _tables ??= new Dictionary<TableKey, Table>();

            var key = new TableKey(name, schema.Compressed);
            if (_tables.TryGetValue(key, out Table value))
                return value;

            var clonedName = name.Clone(Allocator);
            key = new TableKey(clonedName, schema.Compressed);

            var tableTree = ReadTree(clonedName, RootObjectType.Table);

            if (tableTree == null)
                return null;

            _tableSchemaStats ??= new Dictionary<Slice, TableSchemaStatsReference>(SliceComparer.Instance);

            if (_tableSchemaStats.TryGetValue(clonedName, out var tableStatsRef) == false)
            {
                var stats = (TableSchemaStats*)tableTree.DirectRead(TableSchema.StatsSlice);
                if (stats == null)
                    throw new InvalidDataException($"Cannot find stats value for table {name}");

                _tableSchemaStats[clonedName] = tableStatsRef = new TableSchemaStatsReference()
                {
                    NumberOfEntries = stats->NumberOfEntries,
                    OverflowPageCount = stats->OverflowPageCount
                };
            }

            value = new Table(schema, clonedName, this, tableTree, tableStatsRef, schema.TableType);
            _tables[key] = value;
            return value;
        }

        internal void PrepareForCommit()
        {
            OnBeforeCommit?.Invoke(this);

            if (_multiValueTrees != null)
            {
                foreach (var multiValueTree in _multiValueTrees)
                {
                    var parentTree = multiValueTree.Key.Item1;
                    var key = multiValueTree.Key.Item2;
                    var childTree = multiValueTree.Value;

                    using (parentTree.DirectAdd(key, sizeof(TreeRootHeader), TreeNodeFlags.MultiValuePageRef, out byte* ptr))
                        childTree.State.CopyTo((TreeRootHeader*)ptr);
                }
            }
            
            if (_postingLists != null)
            {
                foreach (PostingList set in _postingLists.Values)
                {
                    set.PrepareForCommit();
                    using (_lowLevelTransaction.RootObjects.DirectAdd(set.Name, sizeof(PostingListState), out byte* ptr))
                    {
                        Span<byte> span = new Span<byte>(ptr, sizeof(PostingListState));
                        ref var savedState = ref MemoryMarshal.AsRef<PostingListState>(span);
                        savedState = set.State;
                    }
                }
            }

            if (_containers != null)
            {
                foreach (var (containerId, containerState) in _containers)
                {
                    containerState.PrepareForCommit(this);
                }
            }

            foreach (var tree in Trees)
            {
                if (tree == null)
                    continue;

                tree.PrepareForCommit();

                var treeState = tree.State;
                if (treeState.IsModified)
                {
                    using (_lowLevelTransaction.RootObjects.DirectAdd(tree.Name, sizeof(TreeRootHeader), out byte* ptr))
                        treeState.CopyTo((TreeRootHeader*)ptr);
                }
            }

            if (_tables != null)
            {
                foreach (var participant in _tables.Values)
                {
                    participant.PrepareForCommit();
                }
            }

            _lowLevelTransaction.PrepareForCommit();
        }

        internal void AddMultiValueTree(Tree tree, Slice key, Tree mvTree)
        {
            _multiValueTrees ??= new Dictionary<Tuple<Tree, Slice>, Tree>(new TreeAndSliceComparer());

            mvTree.IsMultiValueTree = true;
            _multiValueTrees.Add(Tuple.Create(tree, key.Clone(_lowLevelTransaction.Allocator, ByteStringType.Immutable)), mvTree);
        }

        internal bool TryGetMultiValueTree(Tree tree, Slice key, out Tree mvTree)
        {
            mvTree = null;
            return _multiValueTrees != null && _multiValueTrees.TryGetValue(Tuple.Create(tree, key), out mvTree);
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
            Slice.From(Allocator, name, ByteStringType.Immutable, out Slice treeName);
            AddTree(treeName, tree);
        }

        internal void AddTree(Slice name, Tree tree)
        {
            EnsureTrees();

            if (_trees.TryGetValue(name, out Tree value) && value != null)
            {
                throw new InvalidOperationException("Tree already exists: " + name);
            }

            // Either we haven't added this tree, or we added it as null (meaning it didn't exist)
            _trees[name] = tree;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void DeleteTree(string name)
        {
            Slice.From(Allocator, name, ByteStringType.Immutable, out Slice nameSlice);
            DeleteTree(nameSlice);
        }

        public void DeleteFixedTree(string name)
        {
            var tree = FixedTreeFor(name);

            DeleteFixedTree(tree);
        }

        public void DeleteFixedTree(FixedSizeTree tree, bool isInRoot = true)
        {
            while (true)
            {
                using (var it = tree.Iterate())
                {
                    if (it.Seek(long.MinValue) == false)
                        break;

                    var currentKey = it.CurrentKey;
                    tree.Delete(currentKey);
                }
            }

            if (isInRoot)
                _lowLevelTransaction.RootObjects.Delete(tree.Name);
        }

        public void DeleteTree(Slice name)
        {
            ThrowIfReadOnly(_lowLevelTransaction, "Cannot create a new newRootTree with a read only transaction");

            Tree tree = ReadTree(name);
            if (tree == null)
                return;

            DeleteTree(tree);

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

        private void DeleteTree(Tree tree, bool isInRoot = true)
        {
            foreach (var page in tree.AllPages())
            {
                _lowLevelTransaction.FreePage(page);
            }

            if (isInRoot)
                _lowLevelTransaction.RootObjects.Delete(tree.Name);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void RenameTree(string fromName, string toName)
        {
            Slice.From(Allocator, fromName, ByteStringType.Immutable, out Slice fromNameSlice);
            Slice.From(Allocator, toName, ByteStringType.Immutable, out Slice toNameSlice);
            RenameTree(fromNameSlice, toNameSlice);
        }

        public void RenameTree(Slice fromName, Slice toName)
        {
            ThrowIfReadOnly(_lowLevelTransaction, "Cannot rename a new tree with a read only transaction");

            if (SliceComparer.Equals(toName, Constants.RootTreeNameSlice))
                Throw<InvalidOperationException>($"Cannot create a tree with reserved name: {toName}");

            var existingTree = ReadTree(toName);
            ThrowIfNotNull<ArgumentException>(existingTree,() => $"Cannot rename a tree with the name of an existing tree: {toName}");

            Tree fromTree = ReadTree(fromName);
            ThrowIfNull<ArgumentException>(fromTree, () => $"Tree {fromName} does not exists");

            _lowLevelTransaction.RootObjects.Delete(fromName);

            using (_lowLevelTransaction.RootObjects.DirectAdd(toName, sizeof(TreeRootHeader), out byte* ptr))
                fromTree.State.CopyTo((TreeRootHeader*)ptr);

            fromTree.Rename(toName);

            // _trees already ensured created in ReadTree
            _trees.Remove(fromName);
            _trees.Remove(toName);

            AddTree(toName, fromTree);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public Tree CreateTree(string name, RootObjectType type = RootObjectType.VariableSizeTree, TreeFlags flags = TreeFlags.None, bool isIndexTree = false, NewPageAllocator newPageAllocator = null)
        {
            Slice.From(Allocator, name, ByteStringType.Immutable, out var treeNameSlice);
            return CreateTree(treeNameSlice, type, flags, isIndexTree, newPageAllocator);
        }

        public Tree CreateTree(Slice name, RootObjectType type = RootObjectType.VariableSizeTree, TreeFlags flags = TreeFlags.None, bool isIndexTree = false, NewPageAllocator newPageAllocator = null)
        {
            Tree tree = ReadTree(name, type, isIndexTree, newPageAllocator);
            if (tree != null)
                return tree;

            ThrowIfReadOnly(_lowLevelTransaction, $"No such tree: '{name}' and cannot create trees in read transactions");

            tree = Tree.Create(_lowLevelTransaction, this, name, flags, type, isIndexTree, newPageAllocator);
            ref var state = ref tree.State.Modify();
            state.RootObjectType = type;

            using (_lowLevelTransaction.RootObjects.DirectAdd(name, sizeof(TreeRootHeader), out byte* ptr))
                tree.State.CopyTo((TreeRootHeader*)ptr);

            AddTree(name, tree);

            return tree;
        }

        bool IDisposableQueryable.IsDisposed => _lowLevelTransaction == null || _lowLevelTransaction.IsDisposed;
        
        public void Dispose()
        {
            _lowLevelTransaction?.Dispose();
            _lowLevelTransaction = null;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public CompactTree CompactTreeFor(string treeName)
        {
            using var _ = Slice.From(Allocator, treeName, ByteStringType.Immutable, out var treeNameSlice);
            return CompactTreeFor(treeNameSlice);
        }

        public bool TryGetCompactTreeFor(Slice treeName, out CompactTree tree)
        {
            return _lowLevelTransaction.RootObjects.TryGetCompactTreeFor(treeName, out tree);
        }
        
        public CompactTree CompactTreeFor(Slice treeName)
        {
            return _lowLevelTransaction.RootObjects.CompactTreeFor(treeName);
        }

        public FixedSizeTree FixedTreeFor(string treeName)
        {
            Slice.From(Allocator, treeName, ByteStringType.Immutable, out var treeNameSlice);
            return FixedTreeFor(treeNameSlice);
        }

        public FixedSizeTree FixedTreeFor(string treeName, ushort valSize)
        {
            Slice.From(Allocator, treeName, ByteStringType.Immutable, out var treeNameSlice);
            return FixedTreeFor(treeNameSlice, valSize);
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
            return val == null ? RootObjectType.None : val->RootObjectType;
        }

        public FixedSizeTree GetGlobalFixedSizeTree(Slice name, ushort valSize, bool isIndexTree = false, NewPageAllocator newPageAllocator = null)
        {
            _globalFixedSizeTree ??= new Dictionary<Slice, FixedSizeTree>(SliceStructComparer.Instance);

            if (_globalFixedSizeTree.TryGetValue(name, out FixedSizeTree tree) == false)
            {
                tree = new FixedSizeTree(LowLevelTransaction, LowLevelTransaction.RootObjects, name, valSize, isIndexTree: isIndexTree, newPageAllocator: newPageAllocator);
                _globalFixedSizeTree[tree.Name] = tree;
            }
            else if (newPageAllocator != null && tree.HasNewPageAllocator == false)
            {
                tree.SetNewPageAllocator(newPageAllocator);
            }

            return tree;
        }

        [Conditional("DEBUG")]
        public static void DebugDisposeReaderAfterTransaction(Transaction tx, BlittableJsonReaderObject reader)
        {
            if (reader == null)
                return;

            Debug.Assert(tx != null);
            // this method is called to ensure that after the transaction is completed, all the readers are disposed
            // so we won't have read-after-tx use scenario, which can in rare case corrupt memory. This is a debug
            // helper that is used across the board, but it is meant to assert stuff during debug only
            tx.LowLevelTransaction.OnDispose += state => reader.Dispose();
        }

        public void DeleteTable(string name)
        {
            var tableTree = ReadTree(name, RootObjectType.Table);
            if (tableTree == null)
                return;

            var writtenSchemaData = tableTree.DirectRead(TableSchema.SchemasSlice);
            var writtenSchemaDataSize = tableTree.GetDataSize(TableSchema.SchemasSlice);
            var schema = TableSchema.ReadFrom(Allocator, writtenSchemaData, writtenSchemaDataSize);

            var table = OpenTable(schema, name);

            // delete table data

            table.DeleteByPrimaryKey(Slices.BeforeAllKeys, x =>
            {
                if (schema.Key.IsGlobal)
                {
                    return table.IsOwned(x.Reader.Id);
                }

                return true;
            });

            if (schema.Key.IsGlobal == false)
            {
                var pkTree = table.GetTree(schema.Key);

                DeleteTree(pkTree, isInRoot: false);

                tableTree.Delete(pkTree.Name);
            }

            // index trees should be already removed but just in case let's go over them and ensure they're really deleted

            foreach (var indexDef in schema.Indexes.Values)
            {
                if (indexDef.IsGlobal) // must not delete global indexes
                    continue;

                if (tableTree.Read(indexDef.Name) == null)
                    continue;

                var indexTree = table.GetTree(indexDef);

                DeleteTree(indexTree, isInRoot: false);

                tableTree.Delete(indexTree.Name);
            }

            foreach (var indexDef in schema.FixedSizeIndexes.Values)
            {
                if (indexDef.IsGlobal)  // must not delete global indexes
                    continue;

                if (tableTree.Read(indexDef.Name) == null)
                    continue;

                var index = table.GetFixedSizeTree(indexDef);

                DeleteFixedTree(index, isInRoot: false);

                tableTree.Delete(index.Name);
            }

            // raw data sections

            table.ActiveDataSmallSection.FreeRawDataSectionPages();

            if (tableTree.Read(TableSchema.ActiveCandidateSectionSlice) != null)
            {
                using (var it = table.ActiveCandidateSection.Iterate())
                {
                    if (it.Seek(long.MinValue))
                    {
                        var sectionPageNumber = it.CurrentKey;
                        var section = new ActiveRawDataSmallSection(this, sectionPageNumber);

                        section.FreeRawDataSectionPages();
                    }
                }

                DeleteFixedTree(table.ActiveCandidateSection, isInRoot: false);
            }

            if (tableTree.Read(TableSchema.InactiveSectionSlice) != null)
                DeleteFixedTree(table.InactiveSections, isInRoot: false);

            DeleteTree(name);

            using (Slice.From(Allocator, name, ByteStringType.Immutable, out var nameSlice))
            {
                _tables.Remove(new TableKey(nameSlice, schema.Compressed));
            }
        }

        public void ForgetAbout(in long storageId)
        {
            if (_cachedDecompressedBuffersByStorageId == null)
                return;

            if (_cachedDecompressedBuffersByStorageId.Remove(storageId, out var t))
            {
                Allocator.Release(ref t);
                _lowLevelTransaction.DecompressedBufferBytes -= t.Length;
            }
        }

        public void Forget(Slice name)
        {
            LowLevelTransaction.RootObjects.Forget(name);
        }

        public Container.TransactionState GetContainerState(long containerId)
        {
            _containers ??= new Dictionary<long, Container.TransactionState>();
            if (_containers.TryGetValue(containerId, out var state))
                return state;
            state = new Container.TransactionState(containerId);
            _containers[containerId] = state;
            return state;
        }

        private readonly struct TableKey(Slice tableName, bool compressed)
        {
            private readonly Slice _tableName = tableName;
            private readonly bool _compressed = compressed;

            private bool Equals(TableKey other) =>
                SliceComparer.Equals(_tableName, other._tableName) && _compressed == other._compressed;

            public override bool Equals(object obj) =>
                obj is TableKey other && Equals(other);

            public override int GetHashCode() =>
                HashCode.Combine(_tableName.GetHashCode(), _compressed);
        }
    }
}
