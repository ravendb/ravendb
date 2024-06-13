using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Utils;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.RawData;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Util;
using static Voron.Data.Tables.TableSchema;
using Constants = Voron.Global.Constants;

namespace Voron.Data.Tables
{
    public sealed unsafe class Table
    {
        private readonly bool _forGlobalReadsOnly;
        private readonly TableSchema _schema;
        internal readonly Transaction _tx;
        private readonly EventHandler<InvalidOperationException> _onCorruptedDataHandler;
        private readonly Tree _tableTree;

        private ActiveRawDataSmallSection _activeDataSmallSection;
        private FixedSizeTree _inactiveSections;
        private FixedSizeTree _activeCandidateSection;

        private Dictionary<Slice, Tree> _treesBySliceCache;
        private Dictionary<Slice, Dictionary<Slice, FixedSizeTree>> _fixedSizeTreeCache;

        public readonly Slice Name;
        private readonly byte _tableType;
        private int? _currentCompressionDictionaryId;

        public long NumberOfEntries => _stats.NumberOfEntries;

        private readonly TableSchemaStatsReference _stats;
        private NewPageAllocator _tablePageAllocator;
        private NewPageAllocator _globalPageAllocator;

        public NewPageAllocator TablePageAllocator
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                _tablePageAllocator ??= new NewPageAllocator(_tx.LowLevelTransaction, _tableTree);
                return _tablePageAllocator;
            }
        }

        private NewPageAllocator GlobalPageAllocator
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                _globalPageAllocator ??= new NewPageAllocator(_tx.LowLevelTransaction, _tx.LowLevelTransaction.RootObjects);
                return _globalPageAllocator;
            }
        }

        public FixedSizeTree InactiveSections => _inactiveSections ??= GetFixedSizeTree(_tableTree, TableSchema.InactiveSectionSlice, 0, isGlobal: false, isIndexTree: true);

        public FixedSizeTree ActiveCandidateSection => _activeCandidateSection ??= GetFixedSizeTree(_tableTree, TableSchema.ActiveCandidateSectionSlice, 0, isGlobal: false, isIndexTree: true);

        internal int CurrentCompressionDictionaryId
        {
            get
            {
                return _currentCompressionDictionaryId ??=
                    _tableTree.ReadInt32(TableSchema.CurrentCompressionDictionaryIdSlice) ?? 0;
            }
            set
            {
                _currentCompressionDictionaryId = value;
                _tableTree.Add(TableSchema.CurrentCompressionDictionaryIdSlice, value);
            }
        }

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

                    _activeDataSmallSection = new ActiveRawDataSmallSection(_tx, pageNumber);
                    _activeDataSmallSection.DataMoved += OnDataMoved;
                }
                return _activeDataSmallSection;
            }
        }

        private void OnDataMoved(long previousId, long newId, byte* data, int size, bool compressed)
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

            if (compressed)
            {
                _tx.CachedDecompressedBuffersByStorageId.Remove(previousId);
            }
        }

        /// <summary>
        /// Tables should not be loaded using this function. The proper way to
        /// do this is to use the OpenTable method in the Transaction class.
        /// Using this constructor WILL NOT register the Table for commit in
        /// the Transaction, and hence changes WILL NOT be committed.
        /// </summary>
        public Table(TableSchema schema, Slice name, Transaction tx, Tree tableTree, TableSchemaStatsReference stats, byte tableType, bool doSchemaValidation = false)
        {
            Name = name;

            _schema = schema;
            _tx = tx;
            _tableType = tableType;
            _stats = stats;

            _tableTree = tableTree;
            if (_tableTree == null)
                throw new ArgumentNullException(nameof(tableTree), "Cannot open table " + Name);


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
        public Table(TableSchema schema, Transaction tx, EventHandler<InvalidOperationException> onCorruptedDataHandler = null)
        {
            _schema = schema;
            _tx = tx;
            _forGlobalReadsOnly = true;
            _tableType = 0;
            _onCorruptedDataHandler = onCorruptedDataHandler;
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

        public bool Read(ByteStringContext context, TableSchema.FixedSizeKeyIndexDef index, long value, out TableValueReader reader)
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

        private byte* DirectReadRaw(long id, out int size, out bool compressed)
        {
            var posInPage = id % Constants.Storage.PageSize;
            if (posInPage == 0) // large
            {
                var page = _tx.LowLevelTransaction.GetPage(id / Constants.Storage.PageSize);
                size = page.OverflowSize;

                byte* ptr = page.Pointer + PageHeader.SizeOf;

                compressed = (page.Flags & PageFlags.Compressed) == PageFlags.Compressed;
                return ptr;
            }

            // here we rely on the fact that RawDataSmallSection can
            // read any RawDataSmallSection piece of data, not just something that
            // it exists in its own section, but anything from other sections as well
            return RawDataSection.DirectRead(_tx.LowLevelTransaction, id, out size, out compressed);
        }

        public void DirectRead(long id, out TableValueReader tvr)
        {
            var rawData = DirectRead(id, out int size);
            tvr = new TableValueReader(id, rawData, size);
        }

        public byte* DirectRead(long id, out int size)
        {
            var result = DirectReadRaw(id, out size, out var compressed);
            if (compressed == false)
                return result;

            return DirectReadDecompress(id, result, ref size);
        }

        private byte* DirectReadDecompress(long id, byte* directRead, ref int size)
        {
            if (_tx.CachedDecompressedBuffersByStorageId.TryGetValue(id, out var t))
            {
                size = t.Length;
                return t.Ptr;
            }

            // we explicitly do *not* dispose the buffer, it lives as long as the tx
            var _ = DecompressValue(_tx, directRead, size, out ByteString buffer);
            _tx.LowLevelTransaction.DecompressedBufferBytes += buffer.Length;

            _tx.CachedDecompressedBuffersByStorageId[id] = buffer;

            size = buffer.Length;
            return buffer.Ptr;
        }

        public int GetSize(long id)
        {
            var ptr = DirectReadRaw(id, out var size, out var compressed);
            if (compressed == false)
                return size;

            if (_tx.CachedDecompressedBuffersByStorageId.TryGetValue(id, out var t))
                return t.Length;

            BlittableJsonReaderBase.ReadVariableSizeIntInReverse(ptr, size - 1, out var offset);
            int length = size - offset;

            int decompressedSize = GetDecompressedSize(new Span<byte>(ptr, length));
            return decompressedSize;
        }

        private static ReadOnlySpan<byte> LookupTable => new byte[] { 5, 6, 7, 9 };
        private static int GetDecompressedSize(Span<byte> buffer)
        {
            byte marker = buffer[4];
            int dicIdCode = marker & 3;
            bool singleElement = ((marker >> 5) & 1) == 1;
            int sizeId = marker >> 6;
            if (dicIdCode > 3)
                throw new ArgumentOutOfRangeException("DicId was: +" + dicIdCode, nameof(dicIdCode));

            var pos = (int)LookupTable[dicIdCode];
            if (singleElement == false)
                pos++;
       
            ulong decompressedSize = sizeId switch
            {
                0 => buffer[pos],
                1 => Unsafe.ReadUnaligned<ushort>(ref buffer[pos]) + 256UL,
                2 => Unsafe.ReadUnaligned<uint>(ref buffer[pos]),
                3 => Unsafe.ReadUnaligned<ulong>(ref buffer[pos]),
                _ => throw new ArgumentOutOfRangeException(nameof(sizeId))
            };
            if (decompressedSize > int.MaxValue)
                throw new ArgumentException("Decompress size cannot be " + decompressedSize, nameof(decompressedSize));
            return (int)decompressedSize;
        }

        internal static ByteStringContext<ByteStringMemoryCache>.InternalScope DecompressValue(
            Transaction tx,
            byte* ptr, int size, out ByteString buffer)
        {
            var dicId = BlittableJsonReaderBase.ReadVariableSizeIntInReverse(ptr, size - 1, out var offset);
            int length = size - offset;
            var dictionary = tx.LowLevelTransaction.Environment.CompressionDictionariesHolder
                .GetCompressionDictionaryFor(tx, dicId);

            int decompressedSize = GetDecompressedSize(new Span<byte>(ptr, length));
            var internalScope = tx.Allocator.Allocate(decompressedSize, out buffer);
            var actualSize = ZstdLib.Decompress(ptr, length, buffer.Ptr, buffer.Length, dictionary);
            if (actualSize != decompressedSize)
                throw new InvalidDataException($"Got decompressed size {actualSize} but expected {decompressedSize} in tx #{tx.LowLevelTransaction.Id}, dic id: {dictionary?.Id ?? 0}");
            return internalScope;
        }

        public (int AllocatedSize, bool IsCompressed) GetInfoFor(long id)
        {
            var posInPage = id % Constants.Storage.PageSize;
            if (posInPage == 0) // large value
            {
                var page = _tx.LowLevelTransaction.GetPage(id / Constants.Storage.PageSize);

                var allocated = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(page.OverflowSize);

                return (allocated * Constants.Storage.PageSize, page.Flags.HasFlag(PageFlags.Compressed));
            }

            // here we rely on the fact that RawDataSmallSection can
            // read any RawDataSmallSection piece of data, not just something that
            // it exists in its own section, but anything from other sections as well
            var sizes = RawDataSection.GetRawDataEntrySizeFor(_tx.LowLevelTransaction, id);
            return (sizes->AllocatedSize, sizes->IsCompressed);
        }

        public long Update(long id, TableValueBuilder builder, bool forceUpdate = false)
        {
            AssertWritableTable();

            if (_schema.Compressed)
                builder.TryCompression(this, _schema);

            int size = builder.Size;

            // We must read before we call TryWriteDirect, because it will modify the size

            var oldData = DirectReadRaw(id, out var oldDataSize, out var oldCompressed);
            AssertNoReferenceToOldData(builder, oldData, oldDataSize);

            ByteStringContext<ByteStringMemoryCache>.InternalScope oldDataDecompressedScope = default;
            if (oldCompressed)
            {
                oldDataDecompressedScope = DecompressValue(_tx, oldData, oldDataSize, out var buffer);
                oldData = buffer.Ptr;
                oldDataSize = buffer.Length;
                _tx.CachedDecompressedBuffersByStorageId?.Remove(id);
            }

            // first, try to fit in place, either in small or large sections
            var prevIsSmall = id % Constants.Storage.PageSize != 0;
            if (size + sizeof(RawDataSection.RawDataEntrySizes) < RawDataSection.MaxItemSize)
            {
                if (prevIsSmall && ActiveDataSmallSection.TryWriteDirect(id, size, builder.Compressed, out byte* pos))
                {
                    var tvr = new TableValueReader(oldData, oldDataSize);
                    UpdateValuesFromIndex(id,
                        ref tvr,
                        builder,
                        forceUpdate);
                    oldDataDecompressedScope.Dispose();

                    builder.CopyTo(pos);

                    return id;
                }
            }
            else if (prevIsSmall == false)
            {
                var pageNumber = id / Constants.Storage.PageSize;
                var page = _tx.LowLevelTransaction.GetPage(pageNumber);
                var existingNumberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(page.OverflowSize);
                var newNumberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(builder.Size);

                if (existingNumberOfPages == newNumberOfPages)
                {
                    page = _tx.LowLevelTransaction.ModifyPage(pageNumber);

                    var pos = page.Pointer + PageHeader.SizeOf;

                    AssertNoReferenceToOldData(builder, pos, builder.Size);

                    var tvr = new TableValueReader(oldData, oldDataSize);
                    UpdateValuesFromIndex(id, ref tvr, builder, forceUpdate);
                    oldDataDecompressedScope.Dispose();

                    // MemoryCopy into final position.
                    page.OverflowSize = builder.Size;

                    if (builder.Compressed)
                        page.Flags |= PageFlags.Compressed;
                    else if (page.Flags.HasFlag(PageFlags.Compressed))
                        page.Flags &= ~PageFlags.Compressed;

                    builder.CopyTo(pos);

                    return id;
                }
            }
            oldDataDecompressedScope.Dispose();

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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
            if (IsOwned(id) == false)
                ThrowNotOwned(id);

            AssertWritableTable();

            var ptr = DirectReadRaw(id, out int size, out bool compressed);

            if (compressed)
                _tx.ForgetAbout(id);

            ByteStringContext<ByteStringMemoryCache>.InternalScope decompressValue = default;

            if (compressed)
            {
                decompressValue = DecompressValue(_tx, ptr, size, out var buffer);
                ptr = buffer.Ptr;
                size = buffer.Length;
            }

            var tvr = new TableValueReader(ptr, size);
            DeleteValueFromIndex(id, ref tvr);

            decompressValue.Dispose();

            var largeValue = (id % Constants.Storage.PageSize) == 0;
            if (largeValue)
            {
                var page = _tx.LowLevelTransaction.GetPage(id / Constants.Storage.PageSize);
                var numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(page.OverflowSize);
                _stats.OverflowPageCount -= numberOfPages;

                for (var i = 0; i < numberOfPages; i++)
                {
                    _tx.LowLevelTransaction.FreePage(page.PageNumber + i);
                }
            }

            _stats.NumberOfEntries--;

            using (_tableTree.DirectAdd(TableSchema.StatsSlice, sizeof(TableSchemaStats), out byte* updatePtr))
            {
                var stats = (TableSchemaStats*)updatePtr;

                stats->NumberOfEntries = _stats.NumberOfEntries;
                stats->OverflowPageCount = _stats.OverflowPageCount;
            }

            if (largeValue)
                return;

            var rawDataSection = ActiveDataSmallSection.Free(id);
            if (ActiveDataSmallSection.Contains(id))
                return;

            var density = rawDataSection.Density;
            if (density > 0.5)
                return;

            var sectionPageNumber = RawDataSection.GetSectionPageNumber(_tx.LowLevelTransaction, id);
            if (density > 0.15)
            {
                ActiveCandidateSection.Add(sectionPageNumber);
                return;
            }

            ReleaseNearlyEmptySection(id, sectionPageNumber);
        }

        private void ReleaseNearlyEmptySection(long id, long sectionPageNumber)
        {
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
                var pos = ActiveDataSmallSection.DirectRead(idToMove, out int itemSize, out bool compressed);

                var dataPtr = pos;
                var dataSize = itemSize;
                ByteStringContext<ByteStringMemoryCache>.InternalScope decompressedScope = default;
                if (compressed)
                {
                    decompressedScope = DecompressValue(_tx, dataPtr, dataSize, out var buffer);
                    dataSize = buffer.Length;
                    dataPtr = buffer.Ptr;
                }

                Debug.Assert(itemSize + sizeof(RawDataSection.RawDataEntrySizes) < RawDataSection.MaxItemSize);
                if (ActiveDataSmallSection.TryAllocate(itemSize, out long newId) == false)
                {
                    newId = AllocateFromAnotherSection(itemSize);
                }

                if (ActiveDataSmallSection.TryWriteDirect(newId, itemSize, compressed, out byte* writePos) == false)
                    throw new VoronErrorException($"Cannot write to newly allocated size in {Name} during delete");

                Memory.Copy(writePos, pos, itemSize);

                OnDataMoved(idToMove, newId, dataPtr, dataSize, compressed);

                // avoiding try / finally or using here for perf reasons
                // if we get an error, it will get cleaned up by the context anyway
                decompressedScope.Dispose();
            }

            ActiveDataSmallSection.DeleteSection(sectionPageNumber);
        }

        [DoesNotReturn]
        private void ThrowNotOwned(long id)
        {
            if (_forTestingPurposes?.DisableDebugAssertionForThrowNotOwned == false)
                Debug.Assert(false, $"Trying to delete a value (id:{id}) from the wrong table ('{Name}')");
            throw new VoronErrorException($"Trying to delete a value (id:{id}) from the wrong table ('{Name}')");
        }

        [DoesNotReturn]
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
                using (_schema.Key.GetValue(_tx.Allocator, ref value, out Slice keySlice))
                {
                    var pkTree = GetTree(_schema.Key);
                    pkTree.Delete(keySlice);
                }
            }

            foreach (var indexDef in _schema.Indexes.Values)
            {
                // For now we wont create secondary indexes on Compact trees.
                var indexTree = GetTree(indexDef);
                using (indexDef.GetValue(_tx.Allocator, ref value, out Slice val))
                {
                    var fst = GetFixedSizeTree(indexTree, val.Clone(_tx.Allocator), 0, indexDef.IsGlobal);
                    if (fst.Delete(id).NumberOfEntriesDeleted == 0)
                    {
                        ThrowInvalidAttemptToRemoveValueFromIndexAndNotFindingIt(id, indexDef.Name);
                    }
                }
            }

            foreach (var dynamicKeyIndexDef in _schema.DynamicKeyIndexes.Values)
            {
                using (dynamicKeyIndexDef.GetValue(_tx, ref value, out Slice val))
                {
                    dynamicKeyIndexDef.OnIndexEntryChanged(_tx, val, oldValue: ref value, newValue: ref TableValueReaderUtils.EmptyReader);

                    var tree = GetTree(dynamicKeyIndexDef);
                    RemoveValueFromDynamicIndex(id, dynamicKeyIndexDef, tree, val);
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

            if (_schema.Compressed
                // we may have tried compressing in the update, so no need to repeat it
                && builder.CompressionTried == false)
                builder.TryCompression(this, _schema);

            byte* pos;
            long id;

            if (builder.Size + sizeof(RawDataSection.RawDataEntrySizes) < RawDataSection.MaxItemSize)
            {
                if (ActiveDataSmallSection.TryAllocate(builder.Size, out id) == false)
                {
                    id = AllocateFromAnotherSection(builder.Size);
                }
                AssertNoReferenceToThisPage(builder, id);

                if (ActiveDataSmallSection.TryWriteDirect(id, builder.Size, builder.Compressed, out pos) == false)
                    ThrowBadWriter(builder.Size, id, builder.Compressed);

                // Memory Copy into final position.
                builder.CopyTo(pos);
            }
            else
            {
                var page = AllocatePageForLargeValue(builder.Size, builder.Compressed);

                pos = page.DataPointer;

                builder.CopyTo(pos);

                id = page.PageNumber * Constants.Storage.PageSize;
            }

            var tvr = builder.CreateReader(pos);
            InsertIndexValuesFor(id, ref tvr);

            _stats.NumberOfEntries++;

            using (_tableTree.DirectAdd(TableSchema.StatsSlice, sizeof(TableSchemaStats), out byte* ptr))
            {
                var stats = (TableSchemaStats*)ptr;

                stats->NumberOfEntries = _stats.NumberOfEntries;
                stats->OverflowPageCount = _stats.OverflowPageCount;
            }

            return id;
        }

        private Page AllocatePageForLargeValue(int size, bool compressed)
        {
            var numberOfOverflowPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(size);
            var page = _tx.LowLevelTransaction.AllocatePage(numberOfOverflowPages);
            _stats.OverflowPageCount += numberOfOverflowPages;

            page.Flags = PageFlags.Overflow | PageFlags.RawData;
            if (compressed)
                page.Flags |= PageFlags.Compressed;

            page.OverflowSize = size;

            ((RawDataOverflowPageHeader*)page.Pointer)->SectionOwnerHash = ActiveDataSmallSection.SectionOwnerHash;
            ((RawDataOverflowPageHeader*)page.Pointer)->TableType = _tableType;
            return page;
        }

        private long AllocateFromAnotherSection(int itemSize)
        {
            InactiveSections.Add(_activeDataSmallSection.PageNumber);

            if (TryFindMatchFromCandidateSections(itemSize, out long id))
                return id;

            CreateNewActiveSection();

            if (ActiveDataSmallSection.TryAllocate(itemSize, out id) == false)
                ThrowBadAllocation(itemSize);

            return id;
        }

        [DoesNotReturn]
        private void ThrowBadWriter(int size, long id, bool compressed)
        {
            throw new VoronErrorException(
                $"After successfully allocating {new Size(size, SizeUnit.Bytes)} bytes (id: {id} compressed: {compressed}), failed to write them on {Name}");
        }

        [DoesNotReturn]
        private void ThrowBadAllocation(int size)
        {
            throw new VoronErrorException(
                $"After changing active sections, failed to allocate {new Size(size, SizeUnit.Bytes)} bytes on {Name}");
        }

        internal sealed class CompressionDictionariesHolder : IDisposable
        {
            private readonly ConcurrentDictionary<int, ZstdLib.CompressionDictionary> _compressionDictionaries = new();
            public ConcurrentDictionary<int, ZstdLib.CompressionDictionary> CompressionDictionaries => _compressionDictionaries;

            public ZstdLib.CompressionDictionary GetCompressionDictionaryFor(Transaction tx, int id)
            {
                if (_compressionDictionaries.TryGetValue(id, out var current))
                    return current;

                current = CreateCompressionDictionary(tx, id);

                var result = _compressionDictionaries.GetOrAdd(id, current);
                if (result != current)
                    current.Dispose();
                return result;
            }

            public IEnumerable<ZstdLib.CompressionDictionary> GetInStorageDictionaries(Transaction tx)
            {
                var tree = tx.ReadTree(TableSchema.CompressionDictionariesSlice);
                if (tree == null)
                    yield break;

                using (var iterator = tree.Iterate(true))
                {
                    if (iterator.Seek(Slices.BeforeAllKeys) == false)
                        yield break;

                    do
                    {
                        var id = iterator.CurrentKey.CreateReader().ReadBigEndianInt32();
                        var dict = CreateCompressionDictionary(tx, id);
                        yield return dict;
                    } while (iterator.MoveNext());
                }
            }

            private ZstdLib.CompressionDictionary CreateCompressionDictionary(Transaction tx, int id)
            {
                var dictionariesTree = tx.ReadTree(TableSchema.CompressionDictionariesSlice);
                var rev = Bits.SwapBytes(id);
                using var _ = Slice.From(tx.Allocator, (byte*)&rev, sizeof(int), out var slice);
                var readResult = dictionariesTree?.Read(slice);
                if (readResult == null)
                {
                    // we may be checking an empty section, so let's return an empty
                    // dictionary there
                    if (id == 0)
                    {
                        return new ZstdLib.CompressionDictionary(0, null, 0, 3);
                    }

                    throw new InvalidOperationException("Trying to read dictionary: " + id + " but it was not found!");
                }

                var info = (CompressionDictionaryInfo*)readResult.Reader.Base;
                var dic = new ZstdLib.CompressionDictionary(id,
                    readResult.Reader.Base + sizeof(CompressionDictionaryInfo),
                    readResult.Reader.Length - sizeof(CompressionDictionaryInfo), 3)
                {
                    ExpectedCompressionRatio = info->ExpectedCompressionRatio
                };
                return dic;
            }

            private void ClearCompressionDictionaries()
            {
                _compressionDictionaries.Clear();
            }

            public void Dispose()
            {
                foreach (var (_, dic) in _compressionDictionaries)
                {
                    dic.Dispose();
                }
            }

            public bool Remove(int id)
            {
                // Intentionally orphaning the dictionary here, we'll let the 
                // GC's finalizer to clear it up, this is a *very* rare operation.
                return _compressionDictionaries.TryRemove(id, out _);
            }

            internal TestingStuff _forTestingPurposes;

            internal TestingStuff ForTestingPurposesOnly()
            {
                if (_forTestingPurposes != null)
                    return _forTestingPurposes;

                return _forTestingPurposes = new TestingStuff(ClearCompressionDictionaries);
            }

            internal class TestingStuff
            {
                private readonly Action _clearCompressionDictionaries;

                public TestingStuff(Action clearCompressionDictionaries)
                {
                    _clearCompressionDictionaries = clearCompressionDictionaries;
                }

                public void ClearCompressionDictionaries()
                {
                    _clearCompressionDictionaries.Invoke();
                }
            }
        }

        private void UpdateValuesFromIndex(long id, ref TableValueReader oldVer, TableValueBuilder newVer, bool forceUpdate)
        {
            AssertWritableTable();

            using (Slice.External(_tx.Allocator, (byte*)&id, sizeof(long), out Slice idAsSlice))
            {
                if (_schema.Key != null)
                {
                    using (_schema.Key.GetValue(_tx.Allocator, ref oldVer, out Slice oldKeySlice))
                    using (_schema.Key.GetValue(_tx.Allocator, newVer, out Slice newKeySlice))
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
                    using (indexDef.GetValue(_tx.Allocator, ref oldVer, out Slice oldVal))
                    using (indexDef.GetValue(_tx.Allocator, newVer, out Slice newVal))
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

                foreach (var dynamicKeyIndexDef in _schema.DynamicKeyIndexes.Values)
                {
                    using (dynamicKeyIndexDef.GetValue(_tx, ref oldVer, out Slice oldVal))
                    using (dynamicKeyIndexDef.GetValue(_tx, newVer, out Slice newVal))
                    {
                        dynamicKeyIndexDef.OnIndexEntryChanged(_tx, key: newVal, oldValue: ref oldVer, newValue: newVer);

                        if (SliceComparer.AreEqual(oldVal, newVal) == false ||
                            forceUpdate)
                        {
                            var indexTree = GetTree(dynamicKeyIndexDef);
                            RemoveValueFromDynamicIndex(id, dynamicKeyIndexDef, indexTree, oldVal);

                            AddValueToDynamicIndex(id, dynamicKeyIndexDef, indexTree, newVal, TreeNodeFlags.Data);
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

        private void AddValueToDynamicIndex(long id, DynamicKeyIndexDef dynamicKeyIndexDef, Tree indexTree, Slice newVal, TreeNodeFlags flags)
        {
            if (dynamicKeyIndexDef.SupportDuplicateKeys == false)
            {
                using (indexTree.DirectAdd(newVal, sizeof(long), flags, out var ptr))
                {
                    *(long*)ptr = id;
                }
            }
            else
            {
                var index = GetFixedSizeTree(indexTree, newVal, 0, dynamicKeyIndexDef.IsGlobal);
                index.Add(id);
            }
        }

        private void RemoveValueFromDynamicIndex(long id, DynamicKeyIndexDef dynamicKeyIndexDef, Tree tree, Slice val)
        {
            if (dynamicKeyIndexDef.SupportDuplicateKeys == false)
            {
                tree.Delete(val);
            }
            else
            {
                var fst = GetFixedSizeTree(tree, val.Clone(_tx.Allocator), 0, dynamicKeyIndexDef.IsGlobal);
                if (fst.Delete(id).NumberOfEntriesDeleted == 0)
                {
                    ThrowInvalidAttemptToRemoveValueFromIndexAndNotFindingIt(id, dynamicKeyIndexDef.Name);
                }
            }
        }

        internal long Insert(ref TableValueReader reader)
        {
            AssertWritableTable();

            using var __ = Allocate(out var builder);

            byte* dataPtr = reader.Pointer;
            int dataSize = reader.Size;
            bool compressed = false;

            if (_schema.Compressed)
            {
                compressed = builder.TryCompression(this, _schema, ref dataPtr, ref dataSize);
            }

            long id;
            if (dataSize + sizeof(RawDataSection.RawDataEntrySizes) < RawDataSection.MaxItemSize)
            {
                if (ActiveDataSmallSection.TryAllocate(dataSize, out id) == false)
                {
                    id = AllocateFromAnotherSection(dataSize);
                }

                if (ActiveDataSmallSection.TryWriteDirect(id, dataSize, compressed, out var pos) == false)
                    ThrowBadWriter(dataSize, id, compressed);

                Memory.Copy(pos, dataPtr, dataSize);
            }
            else
            {
                var numberOfOverflowPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(dataSize);
                var page = _tx.LowLevelTransaction.AllocatePage(numberOfOverflowPages);
                _stats.OverflowPageCount += numberOfOverflowPages;

                page.Flags = PageFlags.Overflow | PageFlags.RawData;
                page.OverflowSize = dataSize;

                ((RawDataOverflowPageHeader*)page.Pointer)->SectionOwnerHash = ActiveDataSmallSection.SectionOwnerHash;
                ((RawDataOverflowPageHeader*)page.Pointer)->TableType = _tableType;

                if (compressed)
                {
                    page.Flags |= PageFlags.Compressed;
                }

                Memory.Copy(page.DataPointer, dataPtr, dataSize);

                id = page.PageNumber * Constants.Storage.PageSize;
            }

            InsertIndexValuesFor(id, ref reader);

            _stats.NumberOfEntries++;

            using (_tableTree.DirectAdd(TableSchema.StatsSlice, sizeof(TableSchemaStats), out byte* ptr))
            {
                var stats = (TableSchemaStats*)ptr;

                stats->NumberOfEntries = _stats.NumberOfEntries;
                stats->OverflowPageCount = _stats.OverflowPageCount;
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
                    using (pk.GetValue(_tx.Allocator, ref value, out Slice pkVal))
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
                    using (indexDef.GetValue(_tx.Allocator, ref value, out Slice val))
                    {
                        var indexTree = GetTree(indexDef);
                        var index = GetFixedSizeTree(indexTree, val, 0, indexDef.IsGlobal);
                        index.Add(id);
                    }
                }

                foreach (var dynamicKeyIndexDef in _schema.DynamicKeyIndexes.Values)
                {
                    using (dynamicKeyIndexDef.GetValue(_tx, ref value, out Slice dynamicKey))
                    {
                        dynamicKeyIndexDef.OnIndexEntryChanged(_tx, dynamicKey, oldValue: ref TableValueReaderUtils.EmptyReader, newValue: ref value);
                        
                        var dynamicIndex = GetTree(dynamicKeyIndexDef);
                        AddValueToDynamicIndex(id, dynamicKeyIndexDef, dynamicIndex, dynamicKey, TreeNodeFlags.Data | TreeNodeFlags.NewOnly);
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

        [DoesNotReturn]
        private void ThrowInvalidDuplicateFixedSizeTreeKey(long key, TableSchema.FixedSizeKeyIndexDef indexDef)
        {
            throw new VoronErrorException("Attempt to add duplicate value " + key + " to " + indexDef.Name + " on " + Name);
        }

        public FixedSizeTree GetFixedSizeTree(TableSchema.FixedSizeKeyIndexDef indexDef)
        {
            if (indexDef.IsGlobal)
            {
                return _tx.GetGlobalFixedSizeTree(indexDef.Name, sizeof(long), isIndexTree: true, newPageAllocator: GlobalPageAllocator);
            }

            var tableTree = _tx.ReadTree(Name);
            return GetFixedSizeTree(tableTree, indexDef.Name, sizeof(long), isGlobal: false, isIndexTree: true);
        }

        internal FixedSizeTree GetFixedSizeTree(Tree parent, Slice name, ushort valSize, bool isGlobal, bool isIndexTree = false)
        {
            if (_fixedSizeTreeCache == null || _fixedSizeTreeCache.TryGetValue(parent.Name, out Dictionary<Slice, FixedSizeTree> cache) == false)
            {
                cache = new Dictionary<Slice, FixedSizeTree>(SliceStructComparer.Instance);
                
                _fixedSizeTreeCache ??= new Dictionary<Slice, Dictionary<Slice, FixedSizeTree>>(SliceStructComparer.Instance);
                _fixedSizeTreeCache[parent.Name] = cache;
            }

            if (cache.TryGetValue(name, out FixedSizeTree tree) == false)
            {
                NewPageAllocator allocator = isGlobal ? GlobalPageAllocator : TablePageAllocator;
                var fixedSizeTree = new FixedSizeTree(_tx.LowLevelTransaction, parent, name, valSize, isIndexTree: isIndexTree | parent.IsIndexTree, newPageAllocator: allocator);
                return cache[fixedSizeTree.Name] = fixedSizeTree;
            }

            return tree;
        }

        private void CreateNewActiveSection()
        {
            ushort maxSectionSizeInPages =
                _tx.LowLevelTransaction.Environment.Options.RunningOn32Bits
                    ? (ushort)((1 * Constants.Size.Megabyte) / Constants.Storage.PageSize)
                    : (ushort)((32 * Constants.Size.Megabyte) / Constants.Storage.PageSize);

            var newNumberOfPages = Math.Min(maxSectionSizeInPages,
                (ushort)(ActiveDataSmallSection.NumberOfPages * 2));

            _activeDataSmallSection = ActiveRawDataSmallSection.Create(_tx, Name, _tableType, newNumberOfPages);
            _activeDataSmallSection.DataMoved += OnDataMoved;
            var val = _activeDataSmallSection.PageNumber;
            using (Slice.External(_tx.Allocator, (byte*)&val, sizeof(long), out Slice pageNumber))
            {
                _tableTree.Add(TableSchema.ActiveSectionSlice, pageNumber);
            }
        }

        private bool TryFindMatchFromCandidateSections(int size, out long id)
        {
            using (var it = ActiveCandidateSection.Iterate())
            {
                if (it.Seek(long.MinValue))
                {
                    do
                    {
                        var sectionPageNumber = it.CurrentKey;

                        _activeDataSmallSection = new ActiveRawDataSmallSection(_tx, sectionPageNumber);

                        _activeDataSmallSection.DataMoved += OnDataMoved;
                        if (_activeDataSmallSection.TryAllocate(size, out id))
                        {
                            var candidatePage = _activeDataSmallSection.PageNumber;
                            using (Slice.External(_tx.Allocator, (byte*)&candidatePage, sizeof(long), out Slice pageNumber))
                            {
                                _tableTree.Add(TableSchema.ActiveSectionSlice, pageNumber);
                            }

                            ActiveCandidateSection.Delete(sectionPageNumber);
                            return true;
                        }
                    } while (it.MoveNext());
                }
            }
            id = 0;
            return false;
        }

        internal Tree GetTree(Slice name, bool isIndexTree)
        {
            if (_treesBySliceCache != null && _treesBySliceCache.TryGetValue(name, out Tree tree))
                return tree;

            var treeHeader = (TreeRootHeader*)_tableTree.DirectRead(name);
            if (treeHeader == null)
                throw new VoronErrorException($"Cannot find tree {name} in table {Name}");

            tree = Tree.Open(_tx.LowLevelTransaction, _tx, name, *treeHeader, isIndexTree: isIndexTree, newPageAllocator: TablePageAllocator);
            
            _treesBySliceCache ??= new Dictionary<Slice, Tree>(SliceStructComparer.Instance);
            _treesBySliceCache[name] = tree;

            return tree;
        }

        internal Tree GetTree(AbstractTreeIndexDef idx)
        {
            Tree tree;
            if (idx.IsGlobal)
            {
                tree = _tx.ReadTree(idx.Name, isIndexTree: true, newPageAllocator: GlobalPageAllocator);
            }
            else
            {
                tree = GetTree(idx.Name, true);
            }
                
            tree?.AssertNotDisposed();
            return tree;
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

        private IEnumerable<TableValueHolder> GetSecondaryIndexForValue(Tree tree, Slice value, TableSchema.AbstractTreeIndexDef index)
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

        private IEnumerable<TableValueHolder> GetBackwardSecondaryIndexForValue(Tree tree, Slice value, TableSchema.AbstractTreeIndexDef index)
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


        public long GetNumberOfEntriesAfter(TableSchema.FixedSizeKeyIndexDef index, long afterValue, out long totalCount, Stopwatch overallDuration)
        {
            var fst = GetFixedSizeTree(index);

            return fst.GetNumberOfEntriesAfter(afterValue, out totalCount, overallDuration);
        }

        public long GetNumberOfEntriesFor(TableSchema.FixedSizeKeyIndexDef index)
        {
            var fst = GetFixedSizeTree(index);
            return fst.NumberOfEntries;
        }

        public IEnumerable<SeekResult> SeekForwardFrom(TableSchema.AbstractTreeIndexDef index, Slice value, long skip, bool startsWith = false)
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

        public IEnumerable<SeekResult> SeekForwardFromPrefix(TableSchema.AbstractTreeIndexDef index, Slice start, Slice prefix, long skip)
        {
            var tree = GetTree(index);
            if (tree == null)
                yield break;

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

        public TableValueHolder SeekOneForwardFromPrefix(TableSchema.AbstractTreeIndexDef index, Slice value)
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

        public IEnumerable<SeekResult> SeekBackwardFrom(TableSchema.AbstractTreeIndexDef index, Slice? prefix, Slice last, long skip)
        {
            var tree = GetTree(index);
            if (tree == null)
                yield break;

            using (var it = tree.Iterate(true))
            {
                if (it.Seek(last) == false && it.Seek(Slices.AfterAllKeys) == false)
                    yield break;

                if (prefix != null)
                {
                    it.SetRequiredPrefix(prefix.Value);

                    if (SliceComparer.StartWith(it.CurrentKey, it.RequiredPrefix) == false)
                    {
                        if (it.MovePrev() == false)
                            yield break;
                    }
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

        public IEnumerable<SeekResult> SeekBackwardFrom(TableSchema.AbstractTreeIndexDef index, Slice prefix, Slice last)
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

        public TableValueHolder SeekOneBackwardFrom(TableSchema.AbstractTreeIndexDef index, Slice prefix, Slice last)
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

        public long GetCountOfMatchesFor(TableSchema.AbstractTreeIndexDef index, Slice value)
        {
            var tree = GetTree(index);

            var fstIndex = GetFixedSizeTree(tree, value, 0, index.IsGlobal);

            return fstIndex.NumberOfEntries;
        }

        public IEnumerable<SeekResult> SeekByPrefix(DynamicKeyIndexDef def, Slice requiredPrefix, Slice startAfter, long skip)
        {
            var isStartAfter = startAfter.Equals(Slices.Empty) == false;

            var tree = GetTree(def);
            if (tree == null)
                yield break;
            
            using (var it = tree.Iterate(true))
            {
                it.SetRequiredPrefix(requiredPrefix);

                var seekValue = isStartAfter ? startAfter : requiredPrefix;
                if (it.Seek(seekValue) == false)
                    yield break;

                if (it.Skip(skip) == false)
                    yield break;

                do
                {
                    var result = new TableValueHolder();
                    GetTableValueReader(it, out result.Reader);
                    yield return new SeekResult
                    {
                        Key = it.CurrentKey,
                        Result = result
                    };
                }
                while (it.MoveNext());
            }
        }

        public IEnumerable<(Slice Key, TableValueHolder Value)> SeekByPrimaryKeyPrefix(Slice requiredPrefix, Slice startAfter, long skip)
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

        public IEnumerable<TableValueHolder> SeekByPrimaryKey(Slice value, long skip)
        {
            var pk = _schema.Key;
            var tree = GetTree(pk);
            if (tree == null)
                yield break;

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

        public bool SeekOneBackwardByPrimaryKeyPrefix(Slice prefix, Slice value, out TableValueReader reader, bool excludeValueFromSeek = false)
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
                else if (SliceComparer.AreEqual(it.CurrentKey, value) == excludeValueFromSeek)
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

                            tableValueHolder ??= new TableValueHolder();
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

        public TableValueHolder ReadFirst(TableSchema.FixedSizeKeyIndexDef index)
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

        public bool SeekOnePrimaryKeyWithPrefix(Slice prefix, Slice value, out TableValueReader reader)
        {
            var pk = _schema.Key;
            var tree = GetTree(pk);
            using (var it = tree.Iterate(false))
            {
                it.SetRequiredPrefix(prefix);

                if (it.Seek(value) == false)
                {
                    reader = default(TableValueReader);
                    return false;
                }

                GetTableValueReader(it, out reader);
                return true;
            }
        }

        private struct PagePrefetcherIterator : IEnumerator<long>
        {
            private readonly ByteString _locations;
            private readonly int _documents;
            private int _idx;

            public PagePrefetcherIterator(ByteString locations, int documents)
            {
                Debug.Assert(locations.Length >= documents);

                _locations = locations;
                _documents = documents;
                _idx = -1;
            }

            long IEnumerator<long>.Current => ((long*)_locations.Ptr)[_idx] / Constants.Storage.PageSize;

            object IEnumerator.Current => ((long*)_locations.Ptr)[_idx] / Constants.Storage.PageSize;

            void IDisposable.Dispose() { }

            bool IEnumerator.MoveNext()
            {
                _idx++;
                return _idx < _documents;
            }

            void IEnumerator.Reset()
            {
                _idx = -1;
            }
        }

        public IEnumerable<(long Key, TableValueHolder TableValueHolder)> IterateForDictionaryTraining(FixedSizeKeyIndexDef index, long skip = 1, long seek = 0)
        {
            if (skip < 1)
                throw new ArgumentOutOfRangeException(nameof(skip), "The skip must be positive and non zero.");

            var fst = GetFixedSizeTree(index);

            // Dictionary training can be extremely IO intensive, therefore as illustrated in RavenDB-21106 we need to have
            // an heuristic that allows us to get good dictionaries but at the same time minimizing the IO usage specially
            // in IO deprived setups.
            // https://issues.hibernatingrhinos.com/issue/RavenDB-21106

            // If we page fault on every page (specially in cold scenarios) we would be wasting a lot. Therefore we should
            // iterate with prefetching enabled.
            using var it = fst.Iterate(prefetch: true);
            if (it.Seek(seek) == false)
                yield break;

            // Considering that in big collections the max amount of documents has been defined to be in the order of 100K 
            // If we have to big skips we should get as many documents as possible to obtain as high locality as data allows.
            int ChunkSize = 1024 * 16;

            int ReadChunked(FixedSizeTree<long>.IFixedSizeIterator it, ByteString chunk, out bool hasMore)
            {
                // We will fill the buffer with the chunk expecting to have many documents in the chunk that end up 
                // being stored in segments that are close to each other.
                var chunkSpan = new Span<long>(chunk.Ptr, ChunkSize);
                var keySpan = new Span<long>(chunk.Ptr + ChunkSize * sizeof(long), ChunkSize);
                Debug.Assert((chunkSpan.Length + keySpan.Length) * sizeof(long) == chunk.Length);
                Debug.Assert(chunkSpan.Length == ChunkSize);
                Debug.Assert(keySpan.Length == ChunkSize);

                int readDocuments = 0;

                do
                {
                    keySpan[readDocuments] = it.CurrentKey;
                    chunkSpan[readDocuments] = *(long*)it.ValuePtr(out int _);

                    readDocuments++;

                    hasMore = it.MoveNext();
                }
                while (readDocuments < chunkSpan.Length && hasMore);

                if (readDocuments == 0)
                    return 0;

                Debug.Assert(readDocuments <= ChunkSize, "We cannot have more than ChunkSize because that is a buffer overflow.");

                // We will divide the whole set into chunks of at least 128 skips (could be bigger if collection is small).
                // Less than that and the tradeoff will be bad for IO (which we are trying to limit as much as possible). 
                int localChunkSize = Math.Max(128, ChunkSize / ((int)skip + 1));
                if (readDocuments < localChunkSize)
                    return readDocuments;

                // We will sort the memory locations and try to maximize locality by sampling in chunks. 
                chunkSpan = chunkSpan.Slice(0, readDocuments);
                keySpan = keySpan.Slice(0, readDocuments);
                chunkSpan.Sort(keySpan);

                // We select a random start location to get documents.
                int selectedLocation = Random.Shared.Next(readDocuments - localChunkSize);

                // PERF: We copy with the costly version with overlapping handling. 
                // https://learn.microsoft.com/en-us/dotnet/api/system.memoryextensions.copyto?view=net-7.0
                chunkSpan.Slice(selectedLocation, localChunkSize).CopyTo(chunkSpan);
                keySpan.Slice(selectedLocation, localChunkSize).CopyTo(keySpan);

                return localChunkSize;
            }

            long GetTableValueReader(ByteString chunk, long index, out TableValueReader reader)
            {
                // We will load from the actual memory location.
                long location = ((long*)chunk.Ptr)[index];
                var ptr = DirectRead(location, out int size);
                reader = new TableValueReader(location, ptr, size);
                
                // We will return the document etag.
                return ((long*)chunk.Ptr)[ChunkSize + index];
            }

            using (_tx.Allocator.Allocate(2 * ChunkSize * sizeof(long), out ByteString chunk))
            {
                // Since we are in an enumerator using yield return, no method with pointers is accepted therefore
                // all logic that requires the usage of the ByteString pointers will be encapsulated into a 
                // local function, no matter how simple it is. 

                var pager = _tx.LowLevelTransaction.DataPager;
                var dataPagerState = _tx.LowLevelTransaction.DataPagerState;
                var result = new TableValueHolder();

                bool hasMore;
                do
                {
                    // Read and filter the documents based on locality.
                    int readDocuments = ReadChunked(it, chunk, out hasMore);
                    if (readDocuments == 0)
                        break;

                    // Prefetch the read documents in order to ensure we will be able to read the data immediately.
                    pager.MaybePrefetchMemory(dataPagerState,new PagePrefetcherIterator(chunk, readDocuments));

                    // Do the actual processing to train by yielding the reader.
                    for (int i = 0; i < readDocuments; i++)
                    {
                        long currentKey = GetTableValueReader(chunk, i, out result.Reader);
                        yield return (currentKey, result);
                    }
                }
                while (hasMore);
            }
        }


        public IEnumerable<TableValueHolder> SeekForwardFrom(FixedSizeKeyIndexDef index, long key, long skip)
        {
            var fst = GetFixedSizeTree(index);

            using (var it = fst.Iterate())
            {
                if (it.Seek(key) == false)
                    yield break;

                if (it.Skip(skip) == false)
                    yield break;

                var result = new TableValueHolder();
                if (_onCorruptedDataHandler == null)
                {
                    do
                    {
                        GetTableValueReader(it, out result.Reader);
                        yield return result;
                    } while (it.MoveNext());
                }
                else
                {
                    do
                    {
                        bool successfully = true;
                        try
                        {
                            GetTableValueReader(it, out result.Reader);
                        }
                        catch (InvalidOperationException e)
                        {
                            _onCorruptedDataHandler.Invoke(this, e);
                            successfully = false;
                        }

                        if (successfully)
                            yield return result;

                    } while (it.MoveNext());
                }
            }
        }

        public TableValueHolder ReadLast(TableSchema.FixedSizeKeyIndexDef index)
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
        
        public IEnumerable<TableValueHolder> SeekBackwardFromLast(TableSchema.FixedSizeKeyIndexDef index, long skip = 0)
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

        public IEnumerable<TableValueHolder> SeekBackwardFrom(TableSchema.FixedSizeKeyIndexDef index, long key, long skip = 0)
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

        public bool HasEntriesGreaterThanStartAndLowerThanOrEqualToEnd(TableSchema.FixedSizeKeyIndexDef index, long start, long end)
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
            long id = *(long*)it.ValuePtr(out int _);
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

        public long DeleteBackwardFrom(TableSchema.FixedSizeKeyIndexDef index, long value, long numberOfEntriesToDelete)
        {
            AssertWritableTable();

            if (numberOfEntriesToDelete < 0)
                ThrowNonNegativeNumberOfEntriesToDelete();

            long deleted = 0;
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

        public bool FindByIndex(TableSchema.FixedSizeKeyIndexDef index, long value, out TableValueReader reader)
        {
            reader = default;
            var fst = GetFixedSizeTree(index);

            using (var it = fst.Iterate())
            {
                if (it.Seek(value) == false)
                    return false;

                if (it.CurrentKey != value)
                    return false;

                GetTableValueReader(it, out reader);
                return true;
            }
        }

        public bool DeleteByIndex(TableSchema.FixedSizeKeyIndexDef index, long value)
        {
            AssertWritableTable();

            var fst = GetFixedSizeTree(index);

            using (var it = fst.Iterate())
            {
                if (it.Seek(value) == false)
                    return false;

                if (it.CurrentKey != value)
                    return false;

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
                        var ptr = DirectRead(id, out int size);
                        tableValueHolder ??= new TableValueHolder();
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

        public long DeleteForwardFrom(TableSchema.AbstractTreeIndexDef index, Slice value, bool startsWith, long numberOfEntriesToDelete,
            Action<TableValueHolder> beforeDelete = null, Func<TableValueHolder, bool> shouldAbort = null)
        {
            AssertWritableTable();

            if (numberOfEntriesToDelete < 0)
                ThrowNonNegativeNumberOfEntriesToDelete();

            long deleted = 0;
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

        public bool DeleteForwardUpToPrefix(Slice startSlice, long upToIndex, long numberOfEntriesToDelete)
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
                    var ptr = DirectRead(id, out int size);

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

        [DoesNotReturn]
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

                using (_tableTree.DirectAdd(treeName, sizeof(TreeRootHeader), out byte* ptr))
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
                if (tree.State.Header.NumberOfEntries != NumberOfEntries)
                    throw new InvalidDataException($"Mismatch in primary key size to table size: {tree.State.Header.NumberOfEntries} != {NumberOfEntries}");
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

        [DoesNotReturn]
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

            var pkIndexNumberOfEntries = GetTree(_schema.Key).State.Header.NumberOfEntries;
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

        public TableReport GetReport(bool includeDetails, StorageReportGenerator generatorInstance = null)
        {
            generatorInstance ??= new StorageReportGenerator(_tx.LowLevelTransaction);

            var overflowSize = _stats.OverflowPageCount * Constants.Storage.PageSize;
            var report = new TableReport(overflowSize, overflowSize, includeDetails, generatorInstance)
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

            foreach (var section in new[] { inactiveSections, activeCandidateSection })
            {
                using (var it = section.Iterate())
                {
                    if (it.Seek(0))
                    {
                        do
                        {
                            var referencedSection = new RawDataSection(_tx.LowLevelTransaction, it.CurrentKey);
                            report.AddData(referencedSection, includeDetails);
                        } while (it.MoveNext());
                    }
                }
            }

            report.AddData(ActiveDataSmallSection, includeDetails);

            report.AddPreAllocatedBuffers(TablePageAllocator, includeDetails);

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

        public sealed class TableValueHolder
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

        private TestingStuff _forTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff(this);
        }

        internal class TestingStuff
        {
            private readonly Table _table;

            internal bool DisableDebugAssertionForThrowNotOwned;

            public TestingStuff(Table table)
            {
                _table = table;
            }

            public bool? IsTableValueCompressed(Slice key, out bool? isLargeValue)
            {
                if (_table.TryFindIdFromPrimaryKey(key, out long id) == false)
                {
                    isLargeValue = default;
                    return default;
                }

                isLargeValue = id % Constants.Storage.PageSize == 0;

                if (isLargeValue.Value)
                {
                    var page = _table._tx.LowLevelTransaction.GetPage(id / Constants.Storage.PageSize);
                    return page.Flags.HasFlag(PageFlags.Compressed);
                }

                var sizes = RawDataSection.GetRawDataEntrySizeFor(_table._tx.LowLevelTransaction, id);
                return sizes->IsCompressed;
            }
        }
    }
}
