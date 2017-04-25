using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Sparrow;
using Sparrow.Collections;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.RawData;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Data.Tables
{
    public unsafe class Table : IDisposable
    {
        private readonly TableSchema _schema;
        private readonly Transaction _tx;
        private readonly Tree _tableTree;

        private ActiveRawDataSmallSection _activeDataSmallSection;
        private FixedSizeTree _fstKey;
        private FixedSizeTree _inactiveSections;
        private FixedSizeTree _activeCandidateSection;

        private readonly FastDictionary<Slice, Tree, SliceStructComparer> _treesBySliceCache = new FastDictionary<Slice, Tree, SliceStructComparer>(SliceStructComparer.Instance);
        private readonly FastDictionary<Slice, FastDictionary<Slice, FixedSizeTree, SliceStructComparer>, SliceStructComparer> _fixedSizeTreeCache = new FastDictionary<Slice, FastDictionary<Slice, FixedSizeTree, SliceStructComparer>, SliceStructComparer>(SliceStructComparer.Instance);

        public readonly Slice Name;

        public long NumberOfEntries { get; private set; }

        private long _overflowPageCount;
        private readonly NewPageAllocator _tablePageAllocator;
        private readonly NewPageAllocator _globalPageAllocator;

        public FixedSizeTree FixedSizeKey => _fstKey ?? (_fstKey = GetFixedSizeTree(_tableTree, _schema.Key.Name, sizeof(long)));

        public FixedSizeTree InactiveSections => _inactiveSections ?? (_inactiveSections = GetFixedSizeTree(_tableTree, TableSchema.InactiveSectionSlice, 0));

        public FixedSizeTree ActiveCandidateSection => _activeCandidateSection ?? (_activeCandidateSection = GetFixedSizeTree(_tableTree, TableSchema.ActiveCandidateSectionSlice, 0));

        public ActiveRawDataSmallSection ActiveDataSmallSection
        {
            get
            {
                if (_activeDataSmallSection == null)
                {
                    var readResult = _tableTree.Read(TableSchema.ActiveSectionSlice);
                    if (readResult == null)
                        throw new InvalidDataException($"Could not find active sections for {Name}");

                    long pageNumber = readResult.Reader.ReadLittleEndianInt64();

                    _activeDataSmallSection = new ActiveRawDataSmallSection(_tx.LowLevelTransaction, pageNumber);
                    _activeDataSmallSection.DataMoved += OnDataMoved;
                }
                return _activeDataSmallSection;
            }
        }

        private void OnDataMoved(long previousId, long newId, byte* data, int size)
        {
            var tvr = new TableValueReader(data, size);
            DeleteValueFromIndex(previousId, ref tvr);
            InsertIndexValuesFor(newId, ref tvr);
        }

        /// <summary>
        /// Tables should not be loaded using this function. The proper way to
        /// do this is to use the OpenTable method in the Transaction class.
        /// Using this constructor WILL NOT register the Table for commit in
        /// the Transaction, and hence changes WILL NOT be commited.
        /// </summary>
        public Table(TableSchema schema, Slice name, Transaction tx, Tree tableTree, bool doSchemaValidation = false)
        {
            Name = name;

            _schema = schema;
            _tx = tx;

            _tableTree = tableTree;
            if (_tableTree == null)
                throw new ArgumentNullException(nameof(tableTree), "Cannot open table " + Name);

            var stats = (TableSchemaStats*)_tableTree.DirectRead(TableSchema.StatsSlice);
            if (stats == null)
                throw new InvalidDataException($"Cannot find stats value for table {name}");

            NumberOfEntries = stats->NumberOfEntries;
            _overflowPageCount = stats->OverflowPageCount;
            _tablePageAllocator = new NewPageAllocator(_tx.LowLevelTransaction, _tableTree);
            _globalPageAllocator = new NewPageAllocator(_tx.LowLevelTransaction, _tx.LowLevelTransaction.RootObjects);

            if (doSchemaValidation)
            {
                byte* writtenSchemaData = _tableTree.DirectRead(TableSchema.SchemasSlice);
                int writtenSchemaDataSize = _tableTree.GetDataSize(TableSchema.SchemasSlice);
                var actualSchema = TableSchema.ReadFrom(tx.Allocator, writtenSchemaData, writtenSchemaDataSize);
                actualSchema.Validate(schema);
            }
        }

        /// <summary>
        /// this overload is meant to be used for global reads only, when want to use
        /// a global index to find data, without touching the actual table.
        /// </summary>
        public Table(TableSchema schema, Transaction tx)
        {
            _schema = schema;
            _tx = tx;
        }

        public bool ReadByKey(Slice key, out TableValueReader reader)
        {
            long id;
            if (TryFindIdFromPrimaryKey(key, out id) == false)
            {
                reader = default(TableValueReader);
                return false;
            }

            int size;
            var rawData = DirectRead(id, out size);
            reader = new TableValueReader(id, rawData, size);
            return true;
        }

        public bool VerifyKeyExists(Slice key)
        {
            long id;
            return TryFindIdFromPrimaryKey(key, out id);
        }


        private bool TryFindIdFromPrimaryKey(Slice key, out long id)
        {
            id = -1;
            var pkTree = GetTree(_schema.Key);
            var readResult = pkTree?.Read(key);
            if (readResult == null)
                return false;

            id = readResult.Reader.ReadLittleEndianInt64();
            return true;
        }

        public byte* DirectRead(long id, out int size)
        {
            var posInPage = id % Constants.Storage.PageSize;
            if (posInPage == 0) // large
            {
                var page = _tx.LowLevelTransaction.GetPage(id / Constants.Storage.PageSize);
                size = page.OverflowSize;

                return page.Pointer + PageHeader.SizeOf;
            }

            // here we rely on the fact that RawDataSmallSection can
            // read any RawDataSmallSection piece of data, not just something that
            // it exists in its own section, but anything from other sections as well
            return RawDataSection.DirectRead(_tx.LowLevelTransaction, id, out size);
        }

        public long Update(long id, TableValueBuilder builder)
        {
            // The ids returned from this function MUST NOT be stored outside of the transaction.
            // These are merely for manipulation within the same transaction, and WILL CHANGE afterwards.
            int size = builder.Size;

            // first, try to fit in place, either in small or large sections
            var prevIsSmall = id % Constants.Storage.PageSize != 0;
            if (size + sizeof(RawDataSection.RawDataEntrySizes) < RawDataSection.MaxItemSize)
            {
                // We must read before we call TryWriteDirect, because it will modify the size
                int oldDataSize;
                var oldData = DirectRead(id, out oldDataSize);

                AssertNoReferenceToOldData(builder, oldData, oldDataSize);

                byte* pos;
                if (prevIsSmall && ActiveDataSmallSection.TryWriteDirect(id, size, out pos))
                {
                    var tvr = new TableValueReader(oldData, oldDataSize);
                    UpdateValuesFromIndex(id,
                        ref tvr,
                        builder);

                    builder.CopyTo(pos);
                    return id;
                }
            }
            else if (prevIsSmall == false)
            {
                var pageNumber = id / Constants.Storage.PageSize;
                var page = _tx.LowLevelTransaction.GetPage(pageNumber);
                var existingNumberOfPages = _tx.LowLevelTransaction.DataPager.GetNumberOfOverflowPages(page.OverflowSize);
                var newNumberOfPages = _tx.LowLevelTransaction.DataPager.GetNumberOfOverflowPages(size);

                if (existingNumberOfPages == newNumberOfPages)
                {
                    page = _tx.LowLevelTransaction.ModifyPage(pageNumber);


                    var pos = page.Pointer + PageHeader.SizeOf;

                    var tvr = new TableValueReader(pos, page.OverflowSize);

                    AssertNoReferenceToOldData(builder, pos, size);

                    UpdateValuesFromIndex(id, ref tvr, builder);

                    // MemoryCopy into final position.
                    page.OverflowSize = size;

                    builder.CopyTo(pos);

                    return id;
                }
            }

            // can't fit in place, will just delete & insert instead
            Delete(id);
            return Insert(builder);
        }

        [Conditional("DEBUG")]
        private void AssertNoReferenceToOldData(TableValueBuilder builder, byte* oldData, int oldDataSize)
        {
            for (int i = 0; i < builder.Count; i++)
            {
                Slice slice;
                using (builder.SliceFromLocation(_tx.Allocator, i, out slice))
                {
                    if (slice.Content.Ptr >= oldData &&
                        slice.Content.Ptr < oldData + oldDataSize)
                    {
                        throw new InvalidOperationException(
                            "Invalid attempt to update data with the source equals to the range we are modifying. This is not permitted since it can cause data corruption when table defrag happens");
                    }
                }
            }
        }

        public void Delete(long id)
        {
            int size;
            var ptr = DirectRead(id, out size);
            if (ptr == null)
                return;

            var tvr = new TableValueReader(ptr, size);
            DeleteValueFromIndex(id, ref tvr);

            var largeValue = (id % Constants.Storage.PageSize) == 0;
            if (largeValue)
            {
                var page = _tx.LowLevelTransaction.GetPage(id / Constants.Storage.PageSize);
                var numberOfPages = _tx.LowLevelTransaction.DataPager.GetNumberOfOverflowPages(page.OverflowSize);
                _overflowPageCount -= numberOfPages;

                for (int i = 0; i < numberOfPages; i++)
                {
                    _tx.LowLevelTransaction.FreePage(page.PageNumber + i);
                }
            }

            NumberOfEntries--;

            byte* updatePtr;
            using (_tableTree.DirectAdd(TableSchema.StatsSlice, sizeof(TableSchemaStats), out updatePtr))
            {
                var stats = (TableSchemaStats*)updatePtr;

                stats->NumberOfEntries = NumberOfEntries;
                stats->OverflowPageCount = _overflowPageCount;
            }

            if (largeValue)
                return;

            var density = ActiveDataSmallSection.Free(id);
            if (ActiveDataSmallSection.Contains(id) || density > 0.5)
                return;

            var sectionPageNumber = ActiveDataSmallSection.GetSectionPageNumber(id);
            if (density > 0.15)
            {
                ActiveCandidateSection.Add(sectionPageNumber);
                return;
            }

            // move all the data to the current active section (maybe creating a new one
            // if this is busy)

            // if this is in the active candidate list, remove it so it cannot be reused if the current
            // active is full and need a new one
            ActiveCandidateSection.Delete(sectionPageNumber);

            var idsInSection = ActiveDataSmallSection.GetAllIdsInSectionContaining(id);
            foreach (var idToMove in idsInSection)
            {
                int itemSize;
                var pos = ActiveDataSmallSection.DirectRead(idToMove, out itemSize);
                var newId = AllocateFromSmallActiveSection(itemSize);

                OnDataMoved(idToMove, newId, pos, itemSize);

                byte* writePos;
                if (ActiveDataSmallSection.TryWriteDirect(newId, itemSize, out writePos) == false)
                    throw new InvalidDataException($"Cannot write to newly allocated size in {Name} during delete");

                Memory.Copy(writePos, pos, itemSize);
            }

            ActiveDataSmallSection.DeleteSection(sectionPageNumber);
        }


        private void DeleteValueFromIndex(long id, ref TableValueReader value)
        {
            if (_schema.Key != null)
            {
                Slice keySlice;
                using (_schema.Key.GetSlice(_tx.Allocator, ref value, out keySlice))
                {
                    var pkTree = GetTree(_schema.Key);
                    pkTree.Delete(keySlice);
                }
            }

            foreach (var indexDef in _schema.Indexes.Values)
            {
                // For now we wont create secondary indexes on Compact trees.
                var indexTree = GetTree(indexDef);
                Slice val;
                using (indexDef.GetSlice(_tx.Allocator, ref value, out val))
                {
                    var fst = GetFixedSizeTree(indexTree, val.Clone(_tx.Allocator), 0);
                    fst.Delete(id);
                }
            }

            foreach (var indexDef in _schema.FixedSizeIndexes.Values)
            {
                var index = GetFixedSizeTree(indexDef);
                var key = indexDef.GetValue(ref value);
                index.Delete(key);
            }
        }


        public long Insert(TableValueBuilder builder)
        {
            // Any changes done to this method should be reproduced in the Insert below, as they're used when compacting.
            // The ids returned from this function MUST NOT be stored outside of the transaction.
            // These are merely for manipulation within the same transaction, and WILL CHANGE afterwards.

            int size = builder.Size;

            byte* pos;
            long id;

            if (size + sizeof(RawDataSection.RawDataEntrySizes) < RawDataSection.MaxItemSize)
            {
                id = AllocateFromSmallActiveSection(size);

                if (ActiveDataSmallSection.TryWriteDirect(id, size, out pos) == false)
                    throw new InvalidOperationException(
                        $"After successfully allocating {size:#,#;;0} bytes, failed to write them on {Name}");

                // Memory Copy into final position.
                builder.CopyTo(pos);
            }
            else
            {
                var numberOfOverflowPages = _tx.LowLevelTransaction.DataPager.GetNumberOfOverflowPages(size);
                var page = _tx.LowLevelTransaction.AllocatePage(numberOfOverflowPages);
                _overflowPageCount += numberOfOverflowPages;

                page.Flags = PageFlags.Overflow | PageFlags.RawData;
                page.OverflowSize = size;

                pos = page.Pointer + PageHeader.SizeOf;

                builder.CopyTo(pos);
                
                id = page.PageNumber * Constants.Storage.PageSize;
            }

            var tvr = new TableValueReader(pos, size);
            InsertIndexValuesFor(id, ref tvr);

            NumberOfEntries++;

            byte* ptr;
            using (_tableTree.DirectAdd(TableSchema.StatsSlice, sizeof(TableSchemaStats), out ptr))
            {
                var stats = (TableSchemaStats*)ptr;

                stats->NumberOfEntries = NumberOfEntries;
                stats->OverflowPageCount = _overflowPageCount;
            }

            return id;
        }

        private void UpdateValuesFromIndex(long id, ref TableValueReader oldVer, TableValueBuilder newVer)
        {
            Slice idAsSlice;
            using (Slice.External(_tx.Allocator, (byte*)&id, sizeof(long), out idAsSlice))
            {
                if (_schema.Key != null)
                {
                    Slice oldKeySlice;
                    Slice newKeySlice;
                    using (_schema.Key.GetSlice(_tx.Allocator, ref oldVer, out oldKeySlice))
                    using (_schema.Key.GetSlice(_tx.Allocator, newVer, out newKeySlice))
                    {
                        if (SliceComparer.AreEqual(oldKeySlice, newKeySlice) == false)
                        {
                            var pkTree = GetTree(_schema.Key);
                            pkTree.Delete(oldKeySlice);
                            pkTree.Add(newKeySlice, idAsSlice);
                        }
                    }
                }

                foreach (var indexDef in _schema.Indexes.Values)
                {
                    // For now we wont create secondary indexes on Compact trees.
                    Slice oldVal;
                    Slice newVal;
                    using (indexDef.GetSlice(_tx.Allocator, ref oldVer, out oldVal))
                    using (indexDef.GetSlice(_tx.Allocator, newVer, out newVal))
                    {
                        if (SliceComparer.AreEqual(oldVal, newVal) == false)
                        {
                            var indexTree = GetTree(indexDef);
                            var fst = GetFixedSizeTree(indexTree, oldVal.Clone(_tx.Allocator), 0);
                            fst.Delete(id);
                            fst = GetFixedSizeTree(indexTree, newVal.Clone(_tx.Allocator), 0);
                            fst.Add(id);
                        }
                    }
                }

                foreach (var indexDef in _schema.FixedSizeIndexes.Values)
                {
                    var index = GetFixedSizeTree(indexDef);
                    var oldKey = indexDef.GetValue(ref oldVer);
                    var newKey = indexDef.GetValue(_tx.Allocator, newVer);
                    if (oldKey != newKey)
                    {
                        index.Delete(oldKey);
                        index.Add(newKey, idAsSlice);
                    }
                }
            }
        }


        internal long Insert(ref TableValueReader reader)
        {
            // The ids returned from this function MUST NOT be stored outside of the transaction.
            // These are merely for manipulation within the same transaction, and WILL CHANGE afterwards.

            int size = reader.Size;

            byte* pos;
            long id;
            if (size + sizeof(RawDataSection.RawDataEntrySizes) < RawDataSection.MaxItemSize)
            {
                id = AllocateFromSmallActiveSection(size);

                if (ActiveDataSmallSection.TryWriteDirect(id, size, out pos) == false)
                    throw new InvalidOperationException($"After successfully allocating {size:#,#;;0} bytes, failed to write them on {Name}");
            }
            else
            {
                var numberOfOverflowPages = _tx.LowLevelTransaction.DataPager.GetNumberOfOverflowPages(size);
                var page = _tx.LowLevelTransaction.AllocatePage(numberOfOverflowPages);
                _overflowPageCount += numberOfOverflowPages;

                page.Flags = PageFlags.Overflow | PageFlags.RawData;
                page.OverflowSize = size;

                pos = page.Pointer + PageHeader.SizeOf;

                id = page.PageNumber * Constants.Storage.PageSize;
            }

            // Memory copy into final position.
            Memory.Copy(pos, reader.Pointer, reader.Size);

            var tvr = new TableValueReader(pos, size);
            InsertIndexValuesFor(id, ref tvr);

            NumberOfEntries++;

            byte* ptr;
            using (_tableTree.DirectAdd(TableSchema.StatsSlice, sizeof(TableSchemaStats), out ptr))
            {
                var stats = (TableSchemaStats*)ptr;

                stats->NumberOfEntries = NumberOfEntries;
                stats->OverflowPageCount = _overflowPageCount;
            }

            return id;
        }

        private void InsertIndexValuesFor(long id, ref TableValueReader value)
        {
            var pk = _schema.Key;
            Slice idAsSlice;
            using (Slice.External(_tx.Allocator, (byte*)&id, sizeof(long), out idAsSlice))
            {
                if (pk != null)
                {
                    Slice pkVal;
                    using (pk.GetSlice(_tx.Allocator, ref value, out pkVal))
                    {
                        var pkIndex = GetTree(pk);

                        pkIndex.Add(pkVal, idAsSlice);
                    }
                }

                foreach (var indexDef in _schema.Indexes.Values)
                {
                    // For now we wont create secondary indexes on Compact trees.
                    Slice val;
                    using (indexDef.GetSlice(_tx.Allocator, ref value, out val))
                    {
                        var indexTree = GetTree(indexDef);
                        var index = GetFixedSizeTree(indexTree, val, 0);
                        index.Add(id);
                    }
                }

                foreach (var indexDef in _schema.FixedSizeIndexes.Values)
                {
                    var index = GetFixedSizeTree(indexDef);
                    long key = indexDef.GetValue(ref value);
                    index.Add(key, idAsSlice);
                }
            }
        }

        private FixedSizeTree GetFixedSizeTree(TableSchema.FixedSizeSchemaIndexDef indexDef)
        {
            if (indexDef.IsGlobal)
                return _tx.GetGlobalFixedSizeTree(indexDef.Name, sizeof(long), newPageAllocator: _globalPageAllocator);

            var tableTree = _tx.ReadTree(Name);
            return GetFixedSizeTree(tableTree, indexDef.Name, sizeof(long));
        }

        private FixedSizeTree GetFixedSizeTree(Tree parent, Slice name, ushort valSize)
        {
            FastDictionary<Slice, FixedSizeTree, SliceStructComparer> cache;

            if (_fixedSizeTreeCache.TryGetValue(parent.Name, out cache) == false)
            {
                cache = new FastDictionary<Slice, FixedSizeTree, SliceStructComparer>(SliceStructComparer.Instance);
                _fixedSizeTreeCache[parent.Name] = cache;
            }

            FixedSizeTree tree;
            if (cache.TryGetValue(name, out tree) == false)
            {
                var fixedSizeTree = new FixedSizeTree(_tx.LowLevelTransaction, parent, name, valSize, newPageAllocator: _tablePageAllocator);
                return cache[fixedSizeTree.Name] = fixedSizeTree;
            }

            return tree;
        }

        private long AllocateFromSmallActiveSection(int size)
        {
            long id;
            if (ActiveDataSmallSection.TryAllocate(size, out id) == false)
            {
                InactiveSections.Add(_activeDataSmallSection.PageNumber);

                using (var it = ActiveCandidateSection.Iterate())
                {
                    if (it.Seek(long.MinValue))
                    {
                        do
                        {
                            var sectionPageNumber = it.CurrentKey;
                            _activeDataSmallSection = new ActiveRawDataSmallSection(_tx.LowLevelTransaction,
                                sectionPageNumber);
                            _activeDataSmallSection.DataMoved += OnDataMoved;
                            if (_activeDataSmallSection.TryAllocate(size, out id))
                            {
                                ActiveCandidateSection.Delete(sectionPageNumber);
                                return id;
                            }
                        } while (it.MoveNext());

                    }
                }

                ushort maxSectionSizeInPages =
                    _tx.LowLevelTransaction.Environment.Options.RunningOn32Bits
                        ? (ushort)((1 * Constants.Size.Megabyte) / Constants.Storage.PageSize)
                        : (ushort)((32 * Constants.Size.Megabyte) / Constants.Storage.PageSize);

                var newNumberOfPages = Math.Min(maxSectionSizeInPages,
                    (ushort)(ActiveDataSmallSection.NumberOfPages * 2));

                _activeDataSmallSection = ActiveRawDataSmallSection.Create(_tx.LowLevelTransaction, Name, newNumberOfPages);
                _activeDataSmallSection.DataMoved += OnDataMoved;
                Slice pageNumber;
                var val = _activeDataSmallSection.PageNumber;
                using (Slice.External(_tx.Allocator, (byte*)&val, sizeof(long), out pageNumber))
                {
                    _tableTree.Add(TableSchema.ActiveSectionSlice, pageNumber);
                }

                var allocationResult = _activeDataSmallSection.TryAllocate(size, out id);

                Debug.Assert(allocationResult);
            }
            return id;
        }

        internal Tree GetTree(Slice name)
        {
            Tree tree;
            if (_treesBySliceCache.TryGetValue(name, out tree))
                return tree;

            var treeHeader = _tableTree.DirectRead(name);
            if (treeHeader == null)
                throw new InvalidOperationException($"Cannot find tree {name} in table {Name}");

            tree = Tree.Open(_tx.LowLevelTransaction, _tx, name, (TreeRootHeader*)treeHeader, newPageAllocator: _tablePageAllocator);
            _treesBySliceCache[name] = tree;

            return tree;
        }

        internal Tree GetTree(TableSchema.SchemaIndexDef idx)
        {
            if (idx.IsGlobal)
                return _tx.ReadTree(idx.Name, newPageAllocator: _globalPageAllocator);
            return GetTree(idx.Name);
        }

        public bool DeleteByKey(Slice key)
        {
            var pkTree = GetTree(_schema.Key);

            var readResult = pkTree.Read(key);
            if (readResult == null)
                return false;

            // This is an implementation detail. We read the absolute location pointer (absolute offset on the file)
            long id = readResult.Reader.ReadLittleEndianInt64();

            // And delete the element accordingly.
            Delete(id);

            return true;
        }

        private IEnumerable<TableValueHolder> GetSecondaryIndexForValue(Tree tree, Slice value)
        {
            try
            {
                var fstIndex = GetFixedSizeTree(tree, value, 0);
                using (var it = fstIndex.Iterate())
                {
                    if (it.Seek(long.MinValue) == false)
                        yield break;

                    var result = new TableValueHolder();
                    do
                    {
                        ReadById(it.CurrentKey, out result.Reader);
                        yield return result;
                    } while (it.MoveNext());
                }
            }
            finally
            {
                value.Release(_tx.Allocator);
            }
        }

        private IEnumerable<TableValueHolder> GetBackwardSecondaryIndexForValue(Tree tree, Slice value)
        {
            try
            {
                var fstIndex = GetFixedSizeTree(tree, value, 0);
                using (var it = fstIndex.Iterate())
                {
                    it.Seek(long.MaxValue);

                    var result = new TableValueHolder();
                    while (it.MovePrev())
                    {
                        ReadById(it.CurrentKey, out result.Reader);
                        yield return result;
                    }
                }
            }
            finally
            {
                value.Release(_tx.Allocator);
            }
        }

        private void ReadById(long id, out TableValueReader reader)
        {
            int size;
            var ptr = DirectRead(id, out size);
            reader = new TableValueReader(id, ptr, size);
        }

        public long GetNumberEntriesFor(TableSchema.FixedSizeSchemaIndexDef index, long afterValue, out long totalCount)
        {
            var fst = GetFixedSizeTree(index);

            totalCount = fst.NumberOfEntries;
            if (afterValue == 0 || totalCount == 0)
                return totalCount;

            long count = 0;
            using (var it = fst.Iterate())
            {
                if (it.Seek(afterValue) == false)
                    return 0;

                do
                {
                    if (it.CurrentKey == afterValue)
                        continue;

                    count++;
                } while (it.MoveNext());
            }

            return count;
        }

        public long GetNumberEntriesFor(TableSchema.FixedSizeSchemaIndexDef index)
        {
            var fst = GetFixedSizeTree(index);
            return fst.NumberOfEntries;
        }

        public IEnumerable<SeekResult> SeekForwardFrom(TableSchema.SchemaIndexDef index, Slice value, int skip, bool startsWith = false)
        {
            var tree = GetTree(index);
            using (var it = tree.Iterate(false))
            {
                if (startsWith)
                    it.SetRequiredPrefix(value);

                if (it.Seek(value) == false)
                    yield break;

                do
                {
                    foreach (var result in GetSecondaryIndexForValue(tree, it.CurrentKey.Clone(_tx.Allocator)))
                    {
                        if (skip > 0)
                        {
                            skip--;
                            continue;
                        }

                        yield return new SeekResult
                        {
                            Key = it.CurrentKey,
                            Result = result
                        };
                    }
                } while (it.MoveNext());
            }
        }

        public IEnumerable<SeekResult> SeekBackwardFrom(TableSchema.SchemaIndexDef index, Slice prefix, Slice last, int skip)
        {
            var tree = GetTree(index);
            if(tree.State.NumberOfEntries == 0)
                yield break;

            using (var it = tree.Iterate(false))
            {
                if (it.Seek(last) == false && it.Seek(Slices.AfterAllKeys) == false)
                    yield break;

                it.SetRequiredPrefix(prefix);
                if (SliceComparer.StartWith(it.CurrentKey, it.RequiredPrefix) == false)
                {
                    if (it.MovePrev() == false)
                        yield break;
                }

                do
                {
                    foreach (var result in GetBackwardSecondaryIndexForValue(tree, it.CurrentKey.Clone(_tx.Allocator)))
                    {
                        if (skip > 0)
                        {
                            skip--;
                            continue;
                        }

                        yield return new SeekResult
                        {
                            Key = it.CurrentKey,
                            Result = result
                        };
                    }
                } while (it.MovePrev());
            }
        }

        public TableValueHolder SeekOneForwardFrom(TableSchema.SchemaIndexDef index, Slice value)
        {
            var tree = GetTree(index);
            using (var it = tree.Iterate(false))
            {
                if (it.Seek(value) == false)
                    return null;

                foreach (var result in GetSecondaryIndexForValue(tree, it.CurrentKey.Clone(_tx.Allocator)))
                {
                    return result;
                }

                return null;
            }
        }

        public long GetCountOfMatchesFor(TableSchema.SchemaIndexDef index, Slice value)
        {
            var tree = GetTree(index);

            var fstIndex = GetFixedSizeTree(tree, value, 0);

            return fstIndex.NumberOfEntries;
        }

        public IEnumerable<TableValueHolder> SeekByPrimaryKeyPrefix(Slice requiredPrefix, Slice startAfter, int skip)
        {
            var isStartAfter = startAfter.Equals(Slices.Empty) == false;

            var pk = _schema.Key;
            var tree = GetTree(pk);
            using (var it = tree.Iterate(false))
            {
                it.SetRequiredPrefix(requiredPrefix);

                var seekValue = isStartAfter ? startAfter : requiredPrefix;
                if (it.Seek(seekValue) == false)
                    yield break;

                if (isStartAfter && it.MoveNext() == false)
                    yield break;

                if (it.Skip(skip) == false)
                    yield break;

                var result = new TableValueHolder();
                do
                {
                    GetTableValueReader(it, out result.Reader);
                    yield return result;
                }
                while (it.MoveNext());
            }
        }

        public IEnumerable<TableValueHolder> SeekByPrimaryKey(Slice value, int skip)
        {
            var pk = _schema.Key;
            var tree = GetTree(pk);
            using (var it = tree.Iterate(false))
            {
                if (it.Seek(value) == false)
                    yield break;

                if (it.Skip(skip) == false)
                    yield break;

                var result = new TableValueHolder();
                do
                {
                    GetTableValueReader(it, out result.Reader);
                    yield return result;
                }
                while (it.MoveNext());
            }
        }

        public bool SeekOnePrimaryKey(Slice slice, out TableValueReader reader)
        {
            Debug.Assert(slice.Options != SliceOptions.Key, "Should be called with only AfterAllKeys or BeforeAllKeys");

            var pk = _schema.Key;
            var tree = GetTree(pk);
            using (var it = tree.Iterate(false))
            {
                if (it.Seek(slice) == false)
                {
                    reader = default(TableValueReader);
                    return false;
                }

                GetTableValueReader(it, out reader);
                return true;
            }
        }

        public bool SeekOnePrimaryKeyPrefix(Slice slice, out TableValueReader reader)
        {
            Debug.Assert(slice.Options == SliceOptions.Key, "Should be called with Key only");

            var pk = _schema.Key;
            var tree = GetTree(pk);
            using (var it = tree.Iterate(false))
            {
                it.SetRequiredPrefix(slice);

                if (it.Seek(slice) == false)
                {
                    reader = default(TableValueReader);
                    return false;
                }

                GetTableValueReader(it, out reader);
                return true;
            }
        }

        public IEnumerable<TableValueHolder> SeekForwardFrom(TableSchema.FixedSizeSchemaIndexDef index, long key, int skip)
        {
            var fst = GetFixedSizeTree(index);

            using (var it = fst.Iterate())
            {
                if (it.Seek(key) == false)
                    yield break;

                if (it.Skip(skip) == false)
                    yield break;

                var result = new TableValueHolder();
                do
                {
                    GetTableValueReader(it, out result.Reader);
                    yield return result;
                } while (it.MoveNext());
            }
        }

        public TableValueHolder ReadLast(TableSchema.FixedSizeSchemaIndexDef index)
        {
            var fst = GetFixedSizeTree(index);

            using (var it = fst.Iterate())
            {
                if (it.SeekToLast() == false)
                    return null;

                var result = new TableValueHolder();
                GetTableValueReader(it, out result.Reader);
                return result;
            }
        }

        public IEnumerable<TableValueHolder> SeekBackwardFromLast(TableSchema.FixedSizeSchemaIndexDef index)
        {
            var fst = GetFixedSizeTree(index);
            using (var it = fst.Iterate())
            {
                if (it.SeekToLast() == false)
                    yield break;

                var result = new TableValueHolder();
                do
                {
                    GetTableValueReader(it, out result.Reader);
                    yield return result;
                } while (it.MovePrev());
            }
        }

        public IEnumerable<TableValueHolder> SeekBackwardFrom(TableSchema.FixedSizeSchemaIndexDef index, long key)
        {
            var fst = GetFixedSizeTree(index);
            using (var it = fst.Iterate())
            {
                if (it.Seek(key) == false)
                    yield break;

                var result = new TableValueHolder();
                do
                {
                    GetTableValueReader(it, out result.Reader);
                    yield return result;
                } while (it.MovePrev());
            }
        }

        public long CountBackwardFrom(TableSchema.FixedSizeSchemaIndexDef index, long key)
        {
            var fst = GetFixedSizeTree(index);

            using (var it = fst.Iterate())
            {
                if (it.Seek(key) == false)
                    return 0;

                long count = 0;
                do
                {
                    count++;
                } while (it.MovePrev());
                return count;
            }
        }

        private void GetTableValueReader(FixedSizeTree.IFixedSizeIterator it, out TableValueReader reader)
        {
            long id;
            Slice slice;
            using (it.Value(out slice))
                slice.CopyTo((byte*)&id);
            int size;
            var ptr = DirectRead(id, out size);
            reader = new TableValueReader(id, ptr, size);
        }


        private void GetTableValueReader(IIterator it, out TableValueReader reader)
        {
            long id = it.CreateReaderForCurrent().ReadLittleEndianInt64();
            int size;
            var ptr = DirectRead(id, out size);
            reader = new TableValueReader(id, ptr, size);
        }

        public long Set(TableValueBuilder builder)
        {
            // The ids returned from this function MUST NOT be stored outside of the transaction.
            // These are merely for manipulation within the same transaction, and WILL CHANGE afterwards.
            long id;
            Slice key;
            bool exists;

            using (builder.SliceFromLocation(_tx.Allocator, _schema.Key.StartIndex, out key))
            {
                exists = TryFindIdFromPrimaryKey(key, out id);
            }

            if (exists)
            {
                id = Update(id, builder);
                return id;
            }

            return Insert(builder);
        }

        public int DeleteBackwardFrom(TableSchema.FixedSizeSchemaIndexDef index, long value, long numberOfEntriesToDelete)
        {
            if (numberOfEntriesToDelete < 0)
                ThrowNonNegativeNumberOfEntriesToDelete();

            int deleted = 0;
            var fst = GetFixedSizeTree(index);
            // deleteing from a table can shift things around, so we delete 
            // them one at a time
            while (deleted < numberOfEntriesToDelete)
            {
                using (var it = fst.Iterate())
                {
                    if (it.Seek(long.MinValue) == false)
                        return deleted;

                    if (it.CurrentKey > value)
                        return deleted;

                    Delete(it.CreateReaderForCurrent().ReadLittleEndianInt64());
                    deleted++;
                }
            }

            return deleted;
        }

        public void DeleteByPrimaryKeyPrefix(Slice startSlice, Action<TableValueHolder> beforeDelete = null)
        {
            var pk = _schema.Key;
            var tree = GetTree(pk);
            TableValueHolder tableValueHolder = null;
            while (true)
            {
                using (var it = tree.Iterate(false))
                {
                    it.SetRequiredPrefix(startSlice);
                    if (it.Seek(it.RequiredPrefix) == false)
                        return;

                    long id = it.CreateReaderForCurrent().ReadLittleEndianInt64();

                    if (beforeDelete != null)
                    {
                        int size;
                        var ptr = DirectRead(id, out size);
                        if (tableValueHolder == null)
                            tableValueHolder = new TableValueHolder();
                        tableValueHolder.Reader = new TableValueReader(id, ptr, size);
                        beforeDelete(tableValueHolder);
                    }

                    Delete(id);
                }
            }
        }

        public long DeleteForwardFrom(TableSchema.SchemaIndexDef index, Slice value, bool startsWith, long numberOfEntriesToDelete,
            Action<TableValueHolder> beforeDelete = null)
        {
            if (numberOfEntriesToDelete < 0)
                ThrowNonNegativeNumberOfEntriesToDelete();

            int deleted = 0;
            var tree = GetTree(index);
            TableValueHolder tableValueHolder = null;
            while (deleted < numberOfEntriesToDelete)
            {
                // deleting from a table can shift things around, so we delete 
                // them one at a time
                using (var it = tree.Iterate(false))
                {
                    if (startsWith)
                        it.SetRequiredPrefix(value);
                    if (it.Seek(value) == false)
                        return deleted;

                    var fst = GetFixedSizeTree(tree, it.CurrentKey.Clone(_tx.Allocator), 0);
                    using (var fstIt = fst.Iterate())
                    {
                        if (fstIt.Seek(long.MinValue) == false)
                            break;

                        if (beforeDelete != null)
                        {
                            int size;
                            var ptr = DirectRead(fstIt.CurrentKey, out size);
                            if (tableValueHolder == null)
                                tableValueHolder = new TableValueHolder();
                            tableValueHolder.Reader = new TableValueReader(fstIt.CurrentKey, ptr, size);
                            beforeDelete(tableValueHolder);
                        }

                        Delete(fstIt.CurrentKey);
                        deleted++;
                    }
                }
            }
            return deleted;
        }

        private static void ThrowNonNegativeNumberOfEntriesToDelete()
        {
            throw new ArgumentOutOfRangeException("Number of entries should not be negative");
        }

        public void PrepareForCommit()
        {
            if (_treesBySliceCache == null)
                return;

            foreach (var item in _treesBySliceCache)
            {
                var tree = item.Value;
                if (!tree.State.IsModified)
                    continue;

                var treeName = item.Key;

                byte* ptr;
                using (_tableTree.DirectAdd(treeName, sizeof(TreeRootHeader), out ptr))
                {
                    var header = (TreeRootHeader*)ptr;
                    tree.State.CopyTo(header);
                }
            }
        }

        public void Dispose()
        {
            foreach (var item in _treesBySliceCache)
            {
                item.Value.Dispose();
            }

            foreach (var item in _fixedSizeTreeCache)
            {
                foreach (var item2 in item.Value)
                {
                    item2.Value.Dispose();
                }
            }

            _activeCandidateSection?.Dispose();
            _activeDataSmallSection?.Dispose();
            _fstKey?.Dispose();
            _inactiveSections?.Dispose();
            _tableTree?.Dispose();
        }

        public TableReport GetReport(bool calculateExactSizes)
        {
            var overflowSize = _overflowPageCount * Constants.Storage.PageSize;
            var report = new TableReport(overflowSize, overflowSize, calculateExactSizes)
            {
                Name = Name.ToString(),
                NumberOfEntries = NumberOfEntries
            };

            report.AddStructure(_tableTree, calculateExactSizes);

            if (_schema.Key != null && _schema.Key.IsGlobal == false)
            {
                var pkTree = GetTree(_schema.Key);
                report.AddIndex(pkTree, calculateExactSizes);
            }

            foreach (var index in _schema.FixedSizeIndexes)
            {
                if (index.Value.IsGlobal)
                    continue;

                var fst = GetFixedSizeTree(index.Value);
                report.AddIndex(fst, calculateExactSizes);
            }

            foreach (var index in _schema.Indexes)
            {
                if (index.Value.IsGlobal)
                    continue;

                var tree = GetTree(index.Value);
                report.AddIndex(tree, calculateExactSizes);
            }

            var activeCandidateSection = ActiveCandidateSection;
            report.AddStructure(activeCandidateSection, calculateExactSizes);

            var inactiveSections = InactiveSections;
            report.AddStructure(inactiveSections, calculateExactSizes);

            using (var it = inactiveSections.Iterate())
            {
                if (it.Seek(0))
                {
                    do
                    {
                        var inactiveSection = new RawDataSection(_tx.LowLevelTransaction, it.CurrentKey);
                        report.AddData(inactiveSection, calculateExactSizes);
                    } while (it.MoveNext());
                }
            }

            report.AddData(ActiveDataSmallSection, calculateExactSizes);

            report.AddPreAllocatedBuffers(_tablePageAllocator, calculateExactSizes);

            return report;
        }

        public struct SeekResult
        {
            public Slice Key;
            public TableValueHolder Result;
        }

        public class TableValueHolder
        {
            // we need this so we'll not have to create a new allocation
            // of TableValueReader per value
            public TableValueReader Reader;
        }

        public ReturnTableValueBuilderToCache Allocate(out TableValueBuilder builder)
        {
            var builderToCache = new ReturnTableValueBuilderToCache(_tx);
            builder = builderToCache.Builder;
            return builderToCache;
        }

        public struct ReturnTableValueBuilderToCache : IDisposable
        {
#if DEBUG
            private readonly Transaction _tx;
#endif
            private readonly TableValueBuilder _builder;

            public ReturnTableValueBuilderToCache(Transaction tx)
            {
                var environmentWriteTransactionPool = tx.LowLevelTransaction.Environment.WriteTransactionPool;
#if DEBUG
                _tx = tx;
                Debug.Assert(tx.LowLevelTransaction.Flags == TransactionFlags.ReadWrite);
                if (environmentWriteTransactionPool.BuilderUsages++ != 0)
                    throw new InvalidOperationException("Cannot use a cached table value builder when it is already in use");
#endif
                _builder = environmentWriteTransactionPool.TableValueBuilder;
            }

            public TableValueBuilder Builder => _builder;

            public void Dispose()
            {
                _builder.Reset();
#if DEBUG
                Debug.Assert(_tx.LowLevelTransaction.IsDisposed == false);
                if (_tx.LowLevelTransaction.Environment.WriteTransactionPool.BuilderUsages-- != 1)
                    throw new InvalidOperationException("Cannot use a cached table value builder when it is already removed");
#endif
            }
        }
    }
}