using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Sparrow;
using Sparrow.Server;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.RawData;
using Voron.Exceptions;
using Voron.Impl;
using Voron.Impl.Paging;
using Constants = Voron.Global.Constants;

namespace Voron.Data.Tables
{
    public unsafe class Table : IDisposable
    {
        private readonly bool _forGlobalReadsOnly;
        private readonly TableSchema _schema;
        private readonly Transaction _tx;
        private readonly Tree _tableTree;

        private ActiveRawDataSmallSection _activeDataSmallSection;
        private FixedSizeTree _inactiveSections;
        private FixedSizeTree _activeCandidateSection;

        private readonly Dictionary<Slice, Tree> _treesBySliceCache = new Dictionary<Slice, Tree>(SliceStructComparer.Instance);
        private readonly Dictionary<Slice, Dictionary<Slice, FixedSizeTree>> _fixedSizeTreeCache = new Dictionary<Slice, Dictionary<Slice, FixedSizeTree>>(SliceStructComparer.Instance);

        public readonly Slice Name;
        private readonly byte _tableType;

        public long NumberOfEntries { get; private set; }

        private long _overflowPageCount;
        private readonly NewPageAllocator _tablePageAllocator;
        private readonly NewPageAllocator _globalPageAllocator;

        public FixedSizeTree InactiveSections => _inactiveSections ?? (_inactiveSections = GetFixedSizeTree(_tableTree, TableSchema.InactiveSectionSlice, 0, isGlobal: false, isIndexTree: true));

        public FixedSizeTree ActiveCandidateSection => _activeCandidateSection ?? (_activeCandidateSection = GetFixedSizeTree(_tableTree, TableSchema.ActiveCandidateSectionSlice, 0, isGlobal: false, isIndexTree: true));

        public ActiveRawDataSmallSection ActiveDataSmallSection
        {
            get
            {
                if (_activeDataSmallSection == null)
                {
                    var readResult = _tableTree.Read(TableSchema.ActiveSectionSlice);
                    if (readResult == null)
                        throw new VoronErrorException($"Could not find active sections for {Name}");

                    long pageNumber = readResult.Reader.ReadLittleEndianInt64();

                    _activeDataSmallSection = new ActiveRawDataSmallSection(_tx.LowLevelTransaction, pageNumber);
                    _activeDataSmallSection.DataMoved += OnDataMoved;
                }
                return _activeDataSmallSection;
            }
        }

        private void OnDataMoved(long previousId, long newId, byte* data, int size)
        {
#if DEBUG
            if (IsOwned(previousId) == false || IsOwned(newId) == false)
            {
                VoronUnrecoverableErrorException.Raise(_tx.LowLevelTransaction,
                    $"Cannot move data in section because the old ({previousId}) or new ({newId}) belongs to a different owner");

            }
#endif
            var tvr = new TableValueReader(data, size);
            DeleteValueFromIndex(previousId, ref tvr);
            InsertIndexValuesFor(newId, ref tvr);
        }

        /// <summary>
        /// Tables should not be loaded using this function. The proper way to
        /// do this is to use the OpenTable method in the Transaction class.
        /// Using this constructor WILL NOT register the Table for commit in
        /// the Transaction, and hence changes WILL NOT be committed.
        /// </summary>
        public Table(TableSchema schema, Slice name, Transaction tx, Tree tableTree, byte tableType, bool doSchemaValidation = false)
        {
            Name = name;

            _schema = schema;
            _tx = tx;
            _tableType = tableType;

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
            _forGlobalReadsOnly = true;
            _tableType = 0;
        }

        public bool ReadByKey(Slice key, out TableValueReader reader)
        {
            if (TryFindIdFromPrimaryKey(key, out long id) == false)
            {
                reader = default(TableValueReader);
                return false;
            }

            var rawData = DirectRead(id, out int size);
            reader = new TableValueReader(id, rawData, size);
            return true;
        }

        public bool Read(ByteStringContext context, TableSchema.FixedSizeSchemaIndexDef index, long value, out TableValueReader reader)
        {
            var fst = GetFixedSizeTree(index);

            using (fst.Read(value, out var read))
            {
                if (read.HasValue == false)
                {
                    reader = default(TableValueReader);
                    return false;
                }

                var storageId = read.CreateReader().ReadLittleEndianInt64();

                ReadById(storageId, out reader);
                return true;
            }
        }

        public bool VerifyKeyExists(Slice key)
        {
            var pkTree = GetTree(_schema.Key);
            var readResult = pkTree?.Read(key);
            return readResult != null;
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

        public int GetAllocatedSize(long id)
        {
            var posInPage = id % Constants.Storage.PageSize;
            if (posInPage == 0) // large
            {
                var page = _tx.LowLevelTransaction.GetPage(id / Constants.Storage.PageSize);

                var allocated = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(page.OverflowSize);

                return allocated * Constants.Storage.PageSize;
            }

            // here we rely on the fact that RawDataSmallSection can
            // read any RawDataSmallSection piece of data, not just something that
            // it exists in its own section, but anything from other sections as well
            return RawDataSection.GetRawDataEntrySizeFor(_tx.LowLevelTransaction, id)->AllocatedSize;
        }

        public long Update(long id, TableValueBuilder builder, bool forceUpdate = false)
        {
            AssertWritableTable();

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
                        builder,
                        forceUpdate);

                    builder.CopyTo(pos);
                    return id;
                }
            }
            else if (prevIsSmall == false)
            {
                var pageNumber = id / Constants.Storage.PageSize;
                var page = _tx.LowLevelTransaction.GetPage(pageNumber);
                var existingNumberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(page.OverflowSize);
                var newNumberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(size);

                if (existingNumberOfPages == newNumberOfPages)
                {
                    page = _tx.LowLevelTransaction.ModifyPage(pageNumber);


                    var pos = page.Pointer + PageHeader.SizeOf;

                    var tvr = new TableValueReader(pos, page.OverflowSize);

                    AssertNoReferenceToOldData(builder, pos, size);

                    UpdateValuesFromIndex(id, ref tvr, builder, forceUpdate);

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
        private void AssertNoReferenceToThisPage(TableValueBuilder builder, long id)
        {
            if (builder == null)
                return;

            var pageNumber = id / Constants.Storage.PageSize;
            var page = _tx.LowLevelTransaction.GetPage(pageNumber);
            for (int i = 0; i < builder.Count; i++)
            {
                Slice slice;
                using (builder.SliceFromLocation(_tx.Allocator, i, out slice))
                {
                    if (slice.Content.Ptr >= page.Pointer &&
                        slice.Content.Ptr < page.Pointer + Constants.Storage.PageSize)
                    {
                        throw new InvalidOperationException(
                            "Invalid attempt to insert data with the source equals to the range we are modifying. This is not permitted since it can cause data corruption when table defrag happens");
                    }
                }
            }
        }

        [Conditional("DEBUG")]
        private void AssertNoReferenceToOldData(TableValueBuilder builder, byte* oldData, int oldDataSize)
        {
            for (int i = 0; i < builder.Count; i++)
            {
                using (builder.SliceFromLocation(_tx.Allocator, i, out Slice slice))
                {
                    if (slice.Content.Ptr >= oldData &&
                        slice.Content.Ptr < oldData + oldDataSize)
                    {
                        throw new InvalidOperationException(
                            "Invalid attempt to update data with the source equals to the range we are modifying. This is not permitted since it can cause data corruption when table defrag happens. You probably should clone your data.");
                    }
                }
            }
        }

        public bool IsOwned(long id)
        {
            var posInPage = id % Constants.Storage.PageSize;

            if (posInPage != 0)
                return ActiveDataSmallSection.IsOwned(id);

            // large value

            var page = _tx.LowLevelTransaction.GetPage(id / Constants.Storage.PageSize);
            var header = (RawDataOverflowPageHeader*)page.Pointer;

            return header->SectionOwnerHash == ActiveDataSmallSection.SectionOwnerHash;
        }

        public void Delete(long id)
        {
            AssertWritableTable();

            var ptr = DirectRead(id, out int size);
            if (ptr == null)
                return;

            var tvr = new TableValueReader(ptr, size);
            DeleteValueFromIndex(id, ref tvr);

            var largeValue = (id % Constants.Storage.PageSize) == 0;
            if (largeValue)
            {
                var page = _tx.LowLevelTransaction.GetPage(id / Constants.Storage.PageSize);
                var numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(page.OverflowSize);
                _overflowPageCount -= numberOfPages;

                for (var i = 0; i < numberOfPages; i++)
                {
                    _tx.LowLevelTransaction.FreePage(page.PageNumber + i);
                }
            }

            NumberOfEntries--;

            using (_tableTree.DirectAdd(TableSchema.StatsSlice, sizeof(TableSchemaStats), out byte* updatePtr))
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
            // need to remove it from the inactive tracking because it is going to be freed in a bit
            InactiveSections.Delete(sectionPageNumber);

            var idsInSection = ActiveDataSmallSection.GetAllIdsInSectionContaining(id);
            foreach (var idToMove in idsInSection)
            {
                var pos = ActiveDataSmallSection.DirectRead(idToMove, out int itemSize);
                var newId = AllocateFromSmallActiveSection(null, itemSize);

                OnDataMoved(idToMove, newId, pos, itemSize);

                if (ActiveDataSmallSection.TryWriteDirect(newId, itemSize, out byte* writePos) == false)
                    throw new VoronErrorException($"Cannot write to newly allocated size in {Name} during delete");

                Memory.Copy(writePos, pos, itemSize);
            }

            ActiveDataSmallSection.DeleteSection(sectionPageNumber);
        }

        private void ThrowInvalidAttemptToRemoveValueFromIndexAndNotFindingIt(long id, Slice indexDefName)
        {
            throw new VoronErrorException(
                $"Invalid index {indexDefName} on {Name}, attempted to delete value but the value from {id} wasn\'t in the index");
        }

        private void DeleteValueFromIndex(long id, ref TableValueReader value)
        {
            AssertWritableTable();

            if (_schema.Key != null)
            {
                using (_schema.Key.GetSlice(_tx.Allocator, ref value, out Slice keySlice))
                {
                    var pkTree = GetTree(_schema.Key);
                    pkTree.Delete(keySlice);
                }
            }

            foreach (var indexDef in _schema.Indexes.Values)
            {
                // For now we wont create secondary indexes on Compact trees.
                var indexTree = GetTree(indexDef);
                using (indexDef.GetSlice(_tx.Allocator, ref value, out Slice val))
                {
                    var fst = GetFixedSizeTree(indexTree, val.Clone(_tx.Allocator), 0, indexDef.IsGlobal);
                    if (fst.Delete(id).NumberOfEntriesDeleted == 0)
                    {
                        ThrowInvalidAttemptToRemoveValueFromIndexAndNotFindingIt(id, indexDef.Name);
                    }
                }
            }

            foreach (var indexDef in _schema.FixedSizeIndexes.Values)
            {
                var index = GetFixedSizeTree(indexDef);
                var key = indexDef.GetValue(ref value);
                if (index.Delete(key).NumberOfEntriesDeleted == 0)
                {
                    ThrowInvalidAttemptToRemoveValueFromIndexAndNotFindingIt(id, indexDef.Name);
                }
            }
        }

        /// <summary>
        /// Resource intensive function that validates fixed size trees in the table's schema
        /// </summary>
        public void AssertValidFixedSizeTrees()
        {
            foreach (var fsi in _schema.FixedSizeIndexes)
            {
                var fixedSizeTree = GetFixedSizeTree(fsi.Value);
                fixedSizeTree.ValidateTree_Forced();
            }
        }

        public long Insert(TableValueBuilder builder)
        {
            AssertWritableTable();

            // Any changes done to this method should be reproduced in the Insert below, as they're used when compacting.
            // The ids returned from this function MUST NOT be stored outside of the transaction.
            // These are merely for manipulation within the same transaction, and WILL CHANGE afterwards.

            var size = builder.Size;

            byte* pos;
            long id;

            if (size + sizeof(RawDataSection.RawDataEntrySizes) < RawDataSection.MaxItemSize)
            {
                id = AllocateFromSmallActiveSection(builder, size);

                if (ActiveDataSmallSection.TryWriteDirect(id, size, out pos) == false)
                    throw new VoronErrorException(
                        $"After successfully allocating {size:#,#;;0} bytes, failed to write them on {Name}");

                // Memory Copy into final position.
                builder.CopyTo(pos);
            }
            else
            {
                var numberOfOverflowPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(size);
                var page = _tx.LowLevelTransaction.AllocatePage(numberOfOverflowPages);
                _overflowPageCount += numberOfOverflowPages;

                page.Flags = PageFlags.Overflow | PageFlags.RawData;
                page.OverflowSize = size;

                ((RawDataOverflowPageHeader*)page.Pointer)->SectionOwnerHash = ActiveDataSmallSection.SectionOwnerHash;
                ((RawDataOverflowPageHeader*)page.Pointer)->TableType = _tableType;

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
        
        private void UpdateValuesFromIndex(long id, ref TableValueReader oldVer, TableValueBuilder newVer, bool forceUpdate)
        {
            AssertWritableTable();

            using (Slice.External(_tx.Allocator, (byte*)&id, sizeof(long), out Slice idAsSlice))
            {
                if (_schema.Key != null)
                {
                    using (_schema.Key.GetSlice(_tx.Allocator, ref oldVer, out Slice oldKeySlice))
                    using (_schema.Key.GetSlice(_tx.Allocator, newVer, out Slice newKeySlice))
                    {
                        if (SliceComparer.AreEqual(oldKeySlice, newKeySlice) == false ||
                            forceUpdate)
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
                        if (SliceComparer.AreEqual(oldVal, newVal) == false ||
                            forceUpdate)
                        {
                            var indexTree = GetTree(indexDef);
                            var fst = GetFixedSizeTree(indexTree, oldVal.Clone(_tx.Allocator), 0, indexDef.IsGlobal);
                            fst.Delete(id);
                            fst = GetFixedSizeTree(indexTree, newVal.Clone(_tx.Allocator), 0, indexDef.IsGlobal);
                            fst.Add(id);
                        }
                    }
                }

                foreach (var indexDef in _schema.FixedSizeIndexes.Values)
                {
                    var index = GetFixedSizeTree(indexDef);
                    var oldKey = indexDef.GetValue(ref oldVer);
                    var newKey = indexDef.GetValue(_tx.Allocator, newVer);

                    if (oldKey != newKey || forceUpdate)
                    {
                        index.Delete(oldKey);
                        if (index.Add(newKey, idAsSlice) == false)
                            ThrowInvalidDuplicateFixedSizeTreeKey(newKey, indexDef);
                    }
                }
            }
        }

        internal long Insert(ref TableValueReader reader)
        {
            AssertWritableTable();

            // The ids returned from this function MUST NOT be stored outside of the transaction.
            // These are merely for manipulation within the same transaction, and WILL CHANGE afterwards.

            int size = reader.Size;

            byte* pos;
            long id;
            if (size + sizeof(RawDataSection.RawDataEntrySizes) < RawDataSection.MaxItemSize)
            {
                id = AllocateFromSmallActiveSection(null, size);

                if (ActiveDataSmallSection.TryWriteDirect(id, size, out pos) == false)
                    throw new VoronErrorException($"After successfully allocating {size:#,#;;0} bytes, failed to write them on {Name}");
            }
            else
            {
                var numberOfOverflowPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(size);
                var page = _tx.LowLevelTransaction.AllocatePage(numberOfOverflowPages);
                _overflowPageCount += numberOfOverflowPages;

                page.Flags = PageFlags.Overflow | PageFlags.RawData;
                page.OverflowSize = size;

                ((RawDataOverflowPageHeader*)page.Pointer)->SectionOwnerHash = ActiveDataSmallSection.SectionOwnerHash;
                ((RawDataOverflowPageHeader*)page.Pointer)->TableType = _tableType;

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
            AssertWritableTable();

            var pk = _schema.Key;
            using (Slice.External(_tx.Allocator, (byte*)&id, sizeof(long), out Slice idAsSlice))
            {
                if (pk != null)
                {
                    using (pk.GetSlice(_tx.Allocator, ref value, out Slice pkVal))
                    {
                        var pkIndex = GetTree(pk);

                        using (pkIndex.DirectAdd(pkVal, idAsSlice.Size, TreeNodeFlags.Data | TreeNodeFlags.NewOnly, out var ptr))
                        {
                            idAsSlice.CopyTo(ptr);
                        }
                    }
                }

                foreach (var indexDef in _schema.Indexes.Values)
                {
                    // For now we wont create secondary indexes on Compact trees.
                    using (indexDef.GetSlice(_tx.Allocator, ref value, out Slice val))
                    {
                        var indexTree = GetTree(indexDef);
                        var index = GetFixedSizeTree(indexTree, val, 0, indexDef.IsGlobal);
                        index.Add(id);
                    }
                }

                foreach (var indexDef in _schema.FixedSizeIndexes.Values)
                {
                    var index = GetFixedSizeTree(indexDef);
                    var key = indexDef.GetValue(ref value);
                    if (index.Add(key, idAsSlice) == false)
                        ThrowInvalidDuplicateFixedSizeTreeKey(key, indexDef);
                }
            }
        }

        private void ThrowInvalidDuplicateFixedSizeTreeKey(long key, TableSchema.FixedSizeSchemaIndexDef indexDef)
        {
            throw new VoronErrorException("Attempt to add duplicate value " + key + " to " + indexDef.Name + " on " + Name);
        }

        public FixedSizeTree GetFixedSizeTree(TableSchema.FixedSizeSchemaIndexDef indexDef)
        {
            if (indexDef.IsGlobal)
                return _tx.GetGlobalFixedSizeTree(indexDef.Name, sizeof(long), isIndexTree: true, newPageAllocator: _globalPageAllocator);

            var tableTree = _tx.ReadTree(Name);
            return GetFixedSizeTree(tableTree, indexDef.Name, sizeof(long), isGlobal: false, isIndexTree: true);
        }

        internal FixedSizeTree GetFixedSizeTree(Tree parent, Slice name, ushort valSize, bool isGlobal, bool isIndexTree = false)
        {
            if (_fixedSizeTreeCache.TryGetValue(parent.Name, out Dictionary<Slice, FixedSizeTree> cache) == false)
            {
                cache = new Dictionary<Slice, FixedSizeTree>(SliceStructComparer.Instance);
                _fixedSizeTreeCache[parent.Name] = cache;
            }

            if (cache.TryGetValue(name, out FixedSizeTree tree) == false)
            {
                var allocator = isGlobal ? _globalPageAllocator : _tablePageAllocator;
                var fixedSizeTree = new FixedSizeTree(_tx.LowLevelTransaction, parent, name, valSize, isIndexTree: isIndexTree | parent.IsIndexTree, newPageAllocator: allocator);
                return cache[fixedSizeTree.Name] = fixedSizeTree;
            }

            return tree;
        }

        private long AllocateFromSmallActiveSection(TableValueBuilder builder, int size)
        {
            if (ActiveDataSmallSection.TryAllocate(size, out long id) == false)
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
                                var candidatePage = _activeDataSmallSection.PageNumber;
                                using (Slice.External(_tx.Allocator, (byte*)&candidatePage, sizeof(long), out Slice pageNumber))
                                {
                                    _tableTree.Add(TableSchema.ActiveSectionSlice, pageNumber);
                                }
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

                _activeDataSmallSection = ActiveRawDataSmallSection.Create(_tx.LowLevelTransaction, Name, _tableType, newNumberOfPages);
                _activeDataSmallSection.DataMoved += OnDataMoved;
                var val = _activeDataSmallSection.PageNumber;
                using (Slice.External(_tx.Allocator, (byte*)&val, sizeof(long), out Slice pageNumber))
                {
                    _tableTree.Add(TableSchema.ActiveSectionSlice, pageNumber);
                }

                var allocationResult = _activeDataSmallSection.TryAllocate(size, out id);

                Debug.Assert(allocationResult);
            }
            AssertNoReferenceToThisPage(builder, id);
            return id;
        }

        internal Tree GetTree(Slice name, bool isIndexTree)
        {
            if (_treesBySliceCache.TryGetValue(name, out Tree tree))
                return tree;

            var treeHeader = _tableTree.DirectRead(name);
            if (treeHeader == null)
                throw new VoronErrorException($"Cannot find tree {name} in table {Name}");

            tree = Tree.Open(_tx.LowLevelTransaction, _tx, name, (TreeRootHeader*)treeHeader, isIndexTree: isIndexTree, newPageAllocator: _tablePageAllocator);
            _treesBySliceCache[name] = tree;

            return tree;
        }

        internal Tree GetTree(TableSchema.SchemaIndexDef idx)
        {
            if (idx.IsGlobal)
                return _tx.ReadTree(idx.Name, isIndexTree: true, newPageAllocator: _globalPageAllocator);
            return GetTree(idx.Name, true);
        }

        public bool DeleteByKey(Slice key)
        {
            AssertWritableTable();

            var pkTree = GetTree(_schema.Key);

            var readResult = pkTree.Read(key);
            if (readResult == null)
                return false;

            // This is an implementation detail. We read the absolute location pointer (absolute offset on the file)
            var id = readResult.Reader.ReadLittleEndianInt64();

            // And delete the element accordingly.
            Delete(id);

            return true;
        }

        private IEnumerable<TableValueHolder> GetSecondaryIndexForValue(Tree tree, Slice value, TableSchema.SchemaIndexDef index)
        {
            try
            {
                var fstIndex = GetFixedSizeTree(tree, value, 0, index.IsGlobal);
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

        private IEnumerable<TableValueHolder> GetBackwardSecondaryIndexForValue(Tree tree, Slice value, TableSchema.SchemaIndexDef index)
        {
            try
            {
                var fstIndex = GetFixedSizeTree(tree, value, 0, index.IsGlobal);
                using (var it = fstIndex.Iterate())
                {
                    if (it.SeekToLast() == false)
                        yield break;

                    var result = new TableValueHolder();
                    do
                    {
                        ReadById(it.CurrentKey, out result.Reader);
                        yield return result;
                    } while (it.MovePrev());
                }
            }
            finally
            {
                value.Release(_tx.Allocator);
            }
        }

        private void ReadById(long id, out TableValueReader reader)
        {
            var ptr = DirectRead(id, out int size);
            reader = new TableValueReader(id, ptr, size);
        }

        public long GetNumberOfEntriesAfter(TableSchema.FixedSizeSchemaIndexDef index, long afterValue, out long totalCount)
        {
            var fst = GetFixedSizeTree(index);

            return fst.GetNumberOfEntriesAfter(afterValue, out totalCount);
        }

        public long GetNumberOfEntriesFor(TableSchema.FixedSizeSchemaIndexDef index)
        {
            var fst = GetFixedSizeTree(index);
            return fst.NumberOfEntries;
        }

        public IEnumerable<SeekResult> SeekForwardFrom(TableSchema.SchemaIndexDef index, Slice value, int skip, bool startsWith = false)
        {
            var tree = GetTree(index);
            if (tree == null)
                yield break;

            using (var it = tree.Iterate(true))
            {
                if (startsWith)
                    it.SetRequiredPrefix(value);

                if (it.Seek(value) == false)
                    yield break;

                do
                {
                    foreach (var result in GetSecondaryIndexForValue(tree, it.CurrentKey.Clone(_tx.Allocator), index))
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


        public IEnumerable<SeekResult> SeekForwardFromPrefix(TableSchema.SchemaIndexDef index, Slice start, Slice prefix, int skip)
        {
            var tree = GetTree(index);
            using (var it = tree.Iterate(true))
            {
                it.SetRequiredPrefix(prefix);

                if (it.Seek(start) == false)
                    yield break;

                do
                {
                    foreach (var result in GetSecondaryIndexForValue(tree, it.CurrentKey.Clone(_tx.Allocator), index))
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

        public TableValueHolder SeekOneForwardFromPrefix(TableSchema.SchemaIndexDef index, Slice value)
        {
            var tree = GetTree(index);
            if (tree == null)
                return null;

            using (var it = tree.Iterate(true))
            {
                it.SetRequiredPrefix(value);

                if (it.Seek(value) == false)
                    return null;

                do
                {
                    foreach (var result in GetSecondaryIndexForValue(tree, it.CurrentKey.Clone(_tx.Allocator), index))
                    {
                        return result;
                    }
                } while (it.MoveNext());
            }

            return null;
        }

        public IEnumerable<SeekResult> SeekBackwardFrom(TableSchema.SchemaIndexDef index, Slice prefix, Slice last, long skip)
        {
            var tree = GetTree(index);
            if (tree == null)
                yield break;

            using (var it = tree.Iterate(true))
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
                    foreach (var result in GetBackwardSecondaryIndexForValue(tree, it.CurrentKey.Clone(_tx.Allocator), index))
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

        public IEnumerable<SeekResult> SeekBackwardFrom(TableSchema.SchemaIndexDef index, Slice prefix, Slice last)
        {
            var tree = GetTree(index);
            if (tree == null)
                yield break;

            using (var it = tree.Iterate(true))
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
                    foreach (var result in GetBackwardSecondaryIndexForValue(tree, it.CurrentKey.Clone(_tx.Allocator), index))
                    {
                        yield return new SeekResult
                        {
                            Key = it.CurrentKey,
                            Result = result
                        };
                    }
                } while (it.MovePrev());
            }
        }

        public IEnumerable<SeekResult> SeekBackwardFrom(TableSchema.SchemaIndexDef index, Slice last)
        {
            var tree = GetTree(index);
            if (tree == null)
                yield break;

            using (var it = tree.Iterate(true))
            {
                if (it.Seek(last) == false && it.Seek(Slices.AfterAllKeys) == false)
                    yield break;

                do
                {
                    foreach (var result in GetBackwardSecondaryIndexForValue(tree, it.CurrentKey.Clone(_tx.Allocator), index))
                    {
                        yield return new SeekResult
                        {
                            Key = it.CurrentKey,
                            Result = result
                        };
                    }
                } while (it.MovePrev());
            }
        }

        public TableValueHolder SeekOneBackwardFrom(TableSchema.SchemaIndexDef index, Slice prefix, Slice last)
        {
            var tree = GetTree(index);
            if (tree == null)
                return null;

            using (var it = tree.Iterate(true))
            {
                if (it.Seek(last) == false && it.Seek(Slices.AfterAllKeys) == false)
                    return null;

                it.SetRequiredPrefix(prefix);
                if (SliceComparer.StartWith(it.CurrentKey, it.RequiredPrefix) == false)
                {
                    if (it.MovePrev() == false)
                        return null;
                }

                do
                {
                    foreach (var result in GetBackwardSecondaryIndexForValue(tree, it.CurrentKey.Clone(_tx.Allocator), index))
                    {
                        return result;
                    }
                } while (it.MovePrev());
            }

            return null;
        }

        public long GetCountOfMatchesFor(TableSchema.SchemaIndexDef index, Slice value)
        {
            var tree = GetTree(index);

            var fstIndex = GetFixedSizeTree(tree, value, 0, index.IsGlobal);

            return fstIndex.NumberOfEntries;
        }

        public IEnumerable<(Slice Key, TableValueHolder Value)> SeekByPrimaryKeyPrefix(Slice requiredPrefix, Slice startAfter, int skip)
        {
            var isStartAfter = startAfter.Equals(Slices.Empty) == false;

            var pk = _schema.Key;
            var tree = GetTree(pk);
            using (var it = tree.Iterate(true))
            {
                it.SetRequiredPrefix(requiredPrefix);

                var seekValue = isStartAfter ? startAfter : requiredPrefix;
                if (it.Seek(seekValue) == false)
                    yield break;

                if (isStartAfter && SliceComparer.Equals(it.CurrentKey, startAfter) && it.MoveNext() == false)
                    yield break;

                if (it.Skip(skip) == false)
                    yield break;

                var result = new TableValueHolder();
                do
                {
                    GetTableValueReader(it, out result.Reader);
                    yield return (it.CurrentKey, result);
                }
                while (it.MoveNext());
            }
        }

        public IEnumerable<TableValueHolder> SeekByPrimaryKey(Slice value, int skip)
        {
            var pk = _schema.Key;
            var tree = GetTree(pk);
            using (var it = tree.Iterate(true))
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
        public bool SeekOneBackwardByPrimaryKeyPrefix(Slice prefix, Slice value, out TableValueReader reader)
        {
            reader = default;
            var pk = _schema.Key;
            var tree = GetTree(pk);
            using (var it = tree.Iterate(true))
            {
                if (it.Seek(value) == false)
                {
                    if (it.Seek(Slices.AfterAllKeys) == false)
                        return false;

                    if (SliceComparer.StartWith(it.CurrentKey, prefix) == false)
                    {
                        it.SetRequiredPrefix(prefix);
                        if (it.MovePrev() == false)
                            return false;
                    }
                }
                else if (SliceComparer.AreEqual(it.CurrentKey, value) == false)
                {
                    it.SetRequiredPrefix(prefix);
                    if (it.MovePrev() == false)
                        return false;
                }

                GetTableValueReader(it, out reader);
                return true;
            }
        }

        public void DeleteByPrimaryKey(Slice value, Func<TableValueHolder, bool> deletePredicate)
        {
            AssertWritableTable();

            var pk = _schema.Key;
            var tree = GetTree(pk);
            TableValueHolder tableValueHolder = null;
            value = value.Clone(_tx.Allocator);
            try
            {
                while (true)
                {
                    using (var it = tree.Iterate(true))
                    {
                        if (it.Seek(value) == false)
                            return;

                        while (true)
                        {
                            var id = it.CreateReaderForCurrent().ReadLittleEndianInt64();
                            var ptr = DirectRead(id, out int size);
                            if (tableValueHolder == null)
                                tableValueHolder = new TableValueHolder();
                            tableValueHolder.Reader = new TableValueReader(id, ptr, size);
                            if (deletePredicate(tableValueHolder))
                            {
                                value.Release(_tx.Allocator);
                                value = it.CurrentKey.Clone(_tx.Allocator);
                                Delete(id);
                                break;
                            }

                            if (it.MoveNext() == false)
                                return;
                        }
                    }
                }
            }
            finally
            {
                value.Release(_tx.Allocator);
            }
        }


        public TableValueHolder ReadFirst(TableSchema.FixedSizeSchemaIndexDef index)
        {
            var fst = GetFixedSizeTree(index);

            using (var it = fst.Iterate())
            {
                if (it.Seek(0) == false)
                    return null;

                var result = new TableValueHolder();
                GetTableValueReader(it, out result.Reader);
                return result;
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

        public IEnumerable<TableValueHolder> SeekBackwardFromLast(TableSchema.FixedSizeSchemaIndexDef index, int skip = 0)
        {
            var fst = GetFixedSizeTree(index);
            using (var it = fst.Iterate())
            {
                if (it.SeekToLast() == false)
                    yield break;

                if (it.Skip(-skip) == false)
                    yield break;

                var result = new TableValueHolder();
                do
                {
                    GetTableValueReader(it, out result.Reader);
                    yield return result;
                } while (it.MovePrev());
            }
        }


        public IEnumerable<TableValueHolder> SeekBackwardFrom(TableSchema.FixedSizeSchemaIndexDef index, long key, int skip = 0)
        {
            var fst = GetFixedSizeTree(index);
            using (var it = fst.Iterate())
            {
                if (it.Seek(key) == false &&
                    it.SeekToLast() == false)
                    yield break;

                if (it.Skip(-skip) == false)
                    yield break;

                var result = new TableValueHolder();
                do
                {
                    GetTableValueReader(it, out result.Reader);
                    yield return result;
                } while (it.MovePrev());
            }
        }

        public bool HasEntriesGreaterThanStartAndLowerThanOrEqualToEnd(TableSchema.FixedSizeSchemaIndexDef index, long start, long end)
        {
            var fst = GetFixedSizeTree(index);

            using (var it = fst.Iterate())
            {
                if (it.Seek(start) == false)
                    return false;

                if (it.CurrentKey <= start && it.MoveNext() == false)
                    return false;

                return it.CurrentKey <= end;
            }
        }

        private void GetTableValueReader(FixedSizeTree.IFixedSizeIterator it, out TableValueReader reader)
        {
            long id;
            using (it.Value(out Slice slice))
                slice.CopyTo((byte*)&id);
            var ptr = DirectRead(id, out int size);
            reader = new TableValueReader(id, ptr, size);
        }


        private void GetTableValueReader(IIterator it, out TableValueReader reader)
        {
            var id = it.CreateReaderForCurrent().ReadLittleEndianInt64();
            var ptr = DirectRead(id, out int size);
            reader = new TableValueReader(id, ptr, size);
        }

        public bool Set(TableValueBuilder builder, bool forceUpdate = false)
        {
            AssertWritableTable();

            // The ids returned from this function MUST NOT be stored outside of the transaction.
            // These are merely for manipulation within the same transaction, and WILL CHANGE afterwards.
            long id;
            bool exists;

            using (builder.SliceFromLocation(_tx.Allocator, _schema.Key.StartIndex, out Slice key))
            {
                exists = TryFindIdFromPrimaryKey(key, out id);
            }

            if (exists)
            {
                Update(id, builder, forceUpdate);
                return false;
            }

            Insert(builder);
            return true;
        }
        
        public int DeleteBackwardFrom(TableSchema.FixedSizeSchemaIndexDef index, long value, long numberOfEntriesToDelete)
        {
            AssertWritableTable();

            if (numberOfEntriesToDelete < 0)
                ThrowNonNegativeNumberOfEntriesToDelete();

            int deleted = 0;
            var fst = GetFixedSizeTree(index);
            // deleting from a table can shift things around, so we delete 
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

        public bool DeleteByIndex(TableSchema.FixedSizeSchemaIndexDef index, long value)
        {
            AssertWritableTable();

            var fst = GetFixedSizeTree(index);

            using (var it = fst.Iterate())
            {
                if (it.Seek(value) == false)
                    return true;

                Delete(it.CreateReaderForCurrent().ReadLittleEndianInt64());
                return true;
            }
        }

        public bool DeleteByPrimaryKeyPrefix(Slice startSlice, Action<TableValueHolder> beforeDelete = null, Func<TableValueHolder, bool> shouldAbort = null)
        {
            AssertWritableTable();

            bool deleted = false;
            var pk = _schema.Key;
            var tree = GetTree(pk);
            TableValueHolder tableValueHolder = null;
            while (true)
            {
                using (var it = tree.Iterate(true))
                {
                    it.SetRequiredPrefix(startSlice);
                    if (it.Seek(it.RequiredPrefix) == false)
                        return deleted;

                    long id = it.CreateReaderForCurrent().ReadLittleEndianInt64();

                    if (beforeDelete != null || shouldAbort != null)
                    {
                        int size;
                        var ptr = DirectRead(id, out size);
                        if (tableValueHolder == null)
                            tableValueHolder = new TableValueHolder();
                        tableValueHolder.Reader = new TableValueReader(id, ptr, size);
                        if (shouldAbort?.Invoke(tableValueHolder) == true)
                        {
                            return deleted;
                        }
                        beforeDelete?.Invoke(tableValueHolder);
                    }

                    Delete(id);
                    deleted = true;
                }
            }
        }

        public long DeleteForwardFrom(TableSchema.SchemaIndexDef index, Slice value, bool startsWith, long numberOfEntriesToDelete,
            Action<TableValueHolder> beforeDelete = null, Func<TableValueHolder, bool> shouldAbort = null)
        {
            AssertWritableTable();

            if (numberOfEntriesToDelete < 0)
                ThrowNonNegativeNumberOfEntriesToDelete();

            int deleted = 0;
            var tree = GetTree(index);
            TableValueHolder tableValueHolder = null;
            while (deleted < numberOfEntriesToDelete)
            {
                // deleting from a table can shift things around, so we delete 
                // them one at a time
                using (var it = tree.Iterate(true))
                {
                    if (startsWith)
                        it.SetRequiredPrefix(value);
                    if (it.Seek(value) == false)
                        return deleted;

                    var fst = GetFixedSizeTree(tree, it.CurrentKey.Clone(_tx.Allocator), 0, index.IsGlobal);
                    using (var fstIt = fst.Iterate())
                    {
                        if (fstIt.Seek(long.MinValue) == false)
                            break;

                        if (beforeDelete != null || shouldAbort != null)
                        {
                            var ptr = DirectRead(fstIt.CurrentKey, out int size);
                            if (tableValueHolder == null)
                                tableValueHolder = new TableValueHolder();
                            tableValueHolder.Reader = new TableValueReader(fstIt.CurrentKey, ptr, size);
                            if (shouldAbort?.Invoke(tableValueHolder) == true)
                            {
                                return deleted;
                            }
                            beforeDelete?.Invoke(tableValueHolder);
                        }

                        Delete(fstIt.CurrentKey);
                        deleted++;
                    }
                }
            }
            return deleted;
        }

        public long DeleteForwardFrom(TableSchema.SchemaIndexDef index, Slice value, bool startsWith, long numberOfEntriesToDelete,
            Func<TableValueHolder, bool> beforeDelete)
        {
            AssertWritableTable();

            if (numberOfEntriesToDelete < 0)
                ThrowNonNegativeNumberOfEntriesToDelete();

            var deleted = 0;
            var tree = GetTree(index);
            TableValueHolder tableValueHolder = null;
            while (deleted < numberOfEntriesToDelete)
            {
                // deleting from a table can shift things around, so we delete 
                // them one at a time
                using (var it = tree.Iterate(true))
                {
                    if (startsWith)
                        it.SetRequiredPrefix(value);
                    if (it.Seek(value) == false)
                        return deleted;

                    var fst = GetFixedSizeTree(tree, it.CurrentKey.Clone(_tx.Allocator), 0, index.IsGlobal);
                    using (var fstIt = fst.Iterate())
                    {
                        if (fstIt.Seek(long.MinValue) == false)
                            break;

                        var ptr = DirectRead(fstIt.CurrentKey, out int size);
                        if (tableValueHolder == null)
                            tableValueHolder = new TableValueHolder();
                        tableValueHolder.Reader = new TableValueReader(fstIt.CurrentKey, ptr, size);
                        if (beforeDelete(tableValueHolder) == false)
                            return deleted;

                        Delete(fstIt.CurrentKey);
                        deleted++;
                    }
                }
            }
            return deleted;
        }

        public bool DeleteForwardUpToPrefix(Slice startSlice, long upToIndex, int numberOfEntriesToDelete)
        {
            AssertWritableTable();

            if (numberOfEntriesToDelete < 0)
                ThrowNonNegativeNumberOfEntriesToDelete();

            var deleted = 0;
            var pk = _schema.Key;
            var pkTree = GetTree(pk);
            TableValueHolder tableValueHolder = null;
            while (deleted < numberOfEntriesToDelete)
            {
                using (var it = pkTree.Iterate(true))
                {
                    it.SetRequiredPrefix(startSlice);
                    if (it.Seek(it.RequiredPrefix) == false)
                        return false;

                    var id = it.CreateReaderForCurrent().ReadLittleEndianInt64();
                    var ptr = DirectRead(id, out var size);

                    if (tableValueHolder == null)
                        tableValueHolder = new TableValueHolder();

                    tableValueHolder.Reader = new TableValueReader(id, ptr, size);
                    var currentIndex = *(long*)tableValueHolder.Reader.Read(1, out _);

                    if (currentIndex > upToIndex)
                        return false;

                    Delete(id);
                    deleted++;
                }
            }

            return true;
        }

        private static void ThrowNonNegativeNumberOfEntriesToDelete()
        {
            throw new VoronErrorException("Number of entries should not be negative");
        }

        public void PrepareForCommit()
        {
            AssertValidTable();

            AssertValidIndexes();

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

        [Conditional("DEBUG")]
        private void AssertValidIndexes()
        {
            var pk = _schema.Key;
            if (pk != null && pk.IsGlobal == false)
            {
                var tree = GetTree(pk);
                if (tree.State.NumberOfEntries != NumberOfEntries)
                    throw new InvalidDataException($"Mismatch in primary key size to table size: {tree.State.NumberOfEntries} != {NumberOfEntries}");
            }

            foreach (var fst in _schema.FixedSizeIndexes)
            {
                if (fst.Value.IsGlobal)
                    continue;

                var tree = GetFixedSizeTree(fst.Value);
                if (tree.NumberOfEntries != NumberOfEntries)
                    throw new InvalidDataException($"Mismatch in fixed sized tree {fst.Key} size to table size: {tree.NumberOfEntries} != {NumberOfEntries}");
            }
        }

        private void ThrowInconsistentItemsCountInIndexes(string indexName, long expectedSize, long actualSize)
        {
            throw new InvalidOperationException($"Inconsistent index items count detected! Index name: {indexName} expected size: {expectedSize} actual size: {actualSize}");
        }

        /// <summary>
        /// validate all globals indexes has the same number
        /// validate all local indexes has the same number as the table itself
        /// </summary>
        [Conditional("VALIDATE")]
        internal void AssertValidTable()
        {
            long globalDocsCount = -1;

            foreach (var fsi in _schema.FixedSizeIndexes)
            {
                var indexNumberOfEntries = GetFixedSizeTree(fsi.Value).NumberOfEntries;
                if (fsi.Value.IsGlobal == false)
                {
                    if (NumberOfEntries != indexNumberOfEntries)
                        ThrowInconsistentItemsCountInIndexes(fsi.Key.ToString(), NumberOfEntries, indexNumberOfEntries);

                }
                else
                {
                    if (globalDocsCount == -1)
                        globalDocsCount = indexNumberOfEntries;
                    else if (globalDocsCount != indexNumberOfEntries)
                        ThrowInconsistentItemsCountInIndexes(fsi.Key.ToString(), NumberOfEntries, indexNumberOfEntries);
                }
            }

            if (_schema.Key == null)
                return;

            var pkIndexNumberOfEntries = GetTree(_schema.Key).State.NumberOfEntries;
            if (_schema.Key.IsGlobal == false)
            {
                if (NumberOfEntries != pkIndexNumberOfEntries)
                    ThrowInconsistentItemsCountInIndexes(_schema.Key.Name.ToString(), NumberOfEntries, pkIndexNumberOfEntries);
            }
            else
            {
                if (globalDocsCount == -1)
                    globalDocsCount = pkIndexNumberOfEntries;
                else if (globalDocsCount != pkIndexNumberOfEntries)
                    ThrowInconsistentItemsCountInIndexes(_schema.Key.Name.ToString(), NumberOfEntries, pkIndexNumberOfEntries);
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
            _inactiveSections?.Dispose();
            _tableTree?.Dispose();
        }

        public TableReport GetReport(bool includeDetails)
        {
            var overflowSize = _overflowPageCount * Constants.Storage.PageSize;
            var report = new TableReport(overflowSize, overflowSize, includeDetails)
            {
                Name = Name.ToString(),
                NumberOfEntries = NumberOfEntries
            };

            report.AddStructure(_tableTree, includeDetails);

            if (_schema.Key != null && _schema.Key.IsGlobal == false)
            {
                var pkTree = GetTree(_schema.Key);
                report.AddIndex(pkTree, includeDetails);
            }

            foreach (var index in _schema.FixedSizeIndexes)
            {
                if (index.Value.IsGlobal)
                    continue;

                var fst = GetFixedSizeTree(index.Value);
                report.AddIndex(fst, includeDetails);
            }

            foreach (var index in _schema.Indexes)
            {
                if (index.Value.IsGlobal)
                    continue;

                var tree = GetTree(index.Value);
                report.AddIndex(tree, includeDetails);
            }

            var activeCandidateSection = ActiveCandidateSection;
            report.AddStructure(activeCandidateSection, includeDetails);

            var inactiveSections = InactiveSections;
            report.AddStructure(inactiveSections, includeDetails);

            using (var it = inactiveSections.Iterate())
            {
                if (it.Seek(0))
                {
                    do
                    {
                        var inactiveSection = new RawDataSection(_tx.LowLevelTransaction, it.CurrentKey);
                        report.AddData(inactiveSection, includeDetails);
                    } while (it.MoveNext());
                }
            }

            report.AddData(ActiveDataSmallSection, includeDetails);

            report.AddPreAllocatedBuffers(_tablePageAllocator, includeDetails);

            return report;
        }

        [Conditional("DEBUG")]
        private void AssertWritableTable()
        {
            if (_forGlobalReadsOnly)
                throw new InvalidOperationException("Table is meant to be used for global reads only while it attempted to modify the data");
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

            public ReturnTableValueBuilderToCache(Transaction tx)
            {
                var environmentWriteTransactionPool = tx.LowLevelTransaction.Environment.WriteTransactionPool;
#if DEBUG
                _tx = tx;
                Debug.Assert(tx.LowLevelTransaction.Flags == TransactionFlags.ReadWrite);
                if (environmentWriteTransactionPool.BuilderUsages++ != 0)
                    throw new InvalidOperationException("Cannot use a cached table value builder when it is already in use");
#endif
                Builder = environmentWriteTransactionPool.TableValueBuilder;
            }

            public TableValueBuilder Builder { get; }

            public void Dispose()
            {
                Builder.Reset();
#if DEBUG
                Debug.Assert(_tx.LowLevelTransaction.IsDisposed == false);
                if (_tx.LowLevelTransaction.Environment.WriteTransactionPool.BuilderUsages-- != 1)
                    throw new InvalidOperationException("Cannot use a cached table value builder when it is already removed");
#endif
            }
        }
    }
}
