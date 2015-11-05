using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Sparrow;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.RawData;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;
using Voron.Util.Conversion;

namespace Voron.Data.Tables
{
    public unsafe class Table<T>
    {
        private Dictionary<Slice, Tree> _trees;
        private readonly TableSchema<T> _schema;
        private readonly Transaction _tx;
        private readonly Tree _tableTree;
        private ActiveRawDataSmallSection _activeDataSmallSection;
        private FixedSizeTree _fstKey;
        private FixedSizeTree _inactiveSections;
        private FixedSizeTree _activeCandidateSection;
        private readonly int _pageSize;

        public FixedSizeTree FixedSizeKey
        {
            get
            {
                if (_fstKey == null)
                    _fstKey = new FixedSizeTree(_tx.LowLevelTransaction, _tableTree, _schema.Key.Name, sizeof(long));
                return _fstKey;
            }
        }

        public FixedSizeTree InactiveSections
        {
            get
            {
                if (_inactiveSections == null)
                {
                    var inactiveSectionSlice = TableSchema<T>.InactiveSectionSlice;
                    _inactiveSections = new FixedSizeTree(_tx.LowLevelTransaction,
                        _tableTree, inactiveSectionSlice, 0);
                }
                return _inactiveSections;
            }
        }


        public FixedSizeTree ActiveCandidateSection
        {
            get
            {
                if (_activeCandidateSection == null)
                {
                    var availableSectionSlice = TableSchema<T>.ActiveCandidateSection;
                    _activeCandidateSection = new FixedSizeTree(_tx.LowLevelTransaction,
                        _tableTree, availableSectionSlice, 0);
                }
                return _activeCandidateSection;
            }
        }

        public ActiveRawDataSmallSection ActiveDataSmallSection
        {
            get
            {
                if (_activeDataSmallSection == null)
                {
                    var readResult = _tableTree.Read(TableSchema<T>.ActiveSectionSlice);
                    if (readResult == null)
                        throw new InvalidDataException($"Could not find active sections for {_schema.Name}");
                    long pageNumber = readResult.Reader.ReadLittleEndianInt64();
                    _activeDataSmallSection = new ActiveRawDataSmallSection(_tx.LowLevelTransaction, pageNumber);
                    _activeDataSmallSection.DataMoved += OnDataMoved;
                }
                return _activeDataSmallSection;
            }
        }

        private void OnDataMoved(long previousId, long newId, byte* data, int size)
        {
            //TODO: Primary key
            //TODO: Update indexes
            throw new NotImplementedException();
        }

        public Table(TableSchema<T> schema, Transaction tx)
        {
            _schema = schema;
            _tx = tx;
            _tableTree = _tx.ReadTree(_schema.Name);
            _pageSize = _tx.LowLevelTransaction.DataPager.PageSize;

            var stats = (TableSchemaStats*)_tableTree.DirectRead(TableSchema<T>.StatsSlice);
            if (stats == null)
                throw new InvalidDataException($"Cannot find stats value for table {_schema.Name}");
            NumberOfEntries = stats->NumberOfEntries;
        }

        public StructureReader<T> ReadByKey(Slice key)
        {
            var readResult = GetTree(_schema.Key.Name).Read(key);
            if (readResult == null)
                return null;
            var id = readResult.Reader.ReadLittleEndianInt64();
            int size;
            var ptr = DirectRead(id, out size);
            return new StructureReader<T>(ptr, _schema.StructureSchema);
        }

        private byte* DirectRead(long id, out int size)
        {
            var posInPage = id % _pageSize;
            if (posInPage == 0) // large
            {
                var page = _tx.LowLevelTransaction.GetPage(id / _pageSize);
                size = page.OverflowSize;
                return page.Pointer + sizeof(PageHeader);
            }

            // here we rely on the fact that RawDataSmallSection can 
            // read any RawDataSmallSection piece of data, not just something that
            // it exists in its own section, but anything from other sections as well
            return ActiveDataSmallSection.DirectRead(id, out size);
        }

        public void Set(Structure<T> value)
        {
            var reader = new StructureReader<T>(value, _schema.StructureSchema);
            var pkVal = GetPrimaryKeySlice(reader);
            var pkIndex = GetTree(_schema.Key.Name);
            var readResult = pkIndex.Read(pkVal);
            if (readResult == null)
            {
                Insert(value);
                return;
            }
            long id = readResult.Reader.ReadLittleEndianInt64();
            Update(id, value);
        }

        private void Update(long id, Structure<T> value)
        {
            var size = value.GetSize();
            var prevIsSmall = id % _pageSize != 0;
            // first, try to fit in place, either in small or large sections
            if (size < ActiveDataSmallSection.MaxItemSize)
            {
                byte* pos; 
                if (prevIsSmall &&
                    ActiveDataSmallSection.TryWriteDirect(id, size, out pos))
                {
                    // the size fits, so we just write it and we are done
                    value.Write(pos);
                    //TODO: Update indexes
                    return;
                }
            }
            else if (prevIsSmall == false)
            {
                var pageNumber = id/_pageSize;
                var page = _tx.LowLevelTransaction.GetPage(pageNumber);
                var existingNumberOfPages = _tx.LowLevelTransaction
                    .DataPager.GetNumberOfOverflowPages(page.OverflowSize);
                var newNumberOfPages = _tx.LowLevelTransaction
                    .DataPager.GetNumberOfOverflowPages(size);
                if (existingNumberOfPages == newNumberOfPages)
                {
                    page.OverflowSize = size;
                    value.Write(page.Pointer + sizeof (PageHeader));
                    //TODO: Update indexes
                    return;
                }
            }
            // can't fit in place, will just delete & insert instead
            Delete(id);
            Insert(value);
        }

        private void Delete(long id)
        {
            int size;
            var ptr = DirectRead(id, out size);
            if (ptr == null)
                return;
            var keySlice = GetPrimaryKeySlice(new StructureReader<T>(ptr, _schema.StructureSchema));
            var tree = GetTree(_schema.Key.Name);
            tree.Delete(keySlice);

            //TODO: Delete value from indexes

            var largeValue = (id%_pageSize) == 0;
            if (largeValue)
            {
                var page = _tx.LowLevelTransaction.GetPage(id/_pageSize);
                var numberOfPages = _tx.LowLevelTransaction.DataPager.GetNumberOfOverflowPages(page.OverflowSize);
                for (int i = 0; i < numberOfPages; i++)
                {
                    _tx.LowLevelTransaction.FreePage(page.PageNumber + i);
                }
                return;
            }

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
                var newId = AllocateFromSmallActiveSection(size);
                OnDataMoved(idToMove, newId, pos, itemSize);
                byte* writePos;
                if (ActiveDataSmallSection.TryWriteDirect(newId, itemSize, out writePos) == false)
                    throw new InvalidDataException(
                        $"Cannot write to newly allocated size in {_schema.Name} during delete");
                Memory.Copy(writePos, pos, itemSize);
            }
            ActiveDataSmallSection.DeleteSection(sectionPageNumber);
        }

        private void Insert(Structure<T> value)
        {
            var stats = (TableSchemaStats*)_tableTree.DirectAdd(TableSchema<T>.StatsSlice, sizeof(TableSchemaStats));
            NumberOfEntries++;
            stats->NumberOfEntries = NumberOfEntries;

            var size = value.GetSize();
            long id;
            if (size < ActiveDataSmallSection.MaxItemSize)
            {
                id = AllocateFromSmallActiveSection(size);
                byte* pos;
                if (ActiveDataSmallSection.TryWriteDirect(id, size, out pos) == false)
                {
                    throw new InvalidOperationException(
                        $"After successfully allocating {size:#,#;;0} bytes," +
                        $" failed to write them on {_schema.Name}");
                }
                value.Write(pos);
            }
            else
            {
                var numberOfOverflowPages = _tx.LowLevelTransaction.DataPager.GetNumberOfOverflowPages(size);
                var page = _tx.LowLevelTransaction.AllocatePage(numberOfOverflowPages);
                page.Flags = PageFlags.Overflow | PageFlags.RawData;
                page.OverflowSize = size;
                byte* pos = page.Pointer + sizeof(PageHeader);
                value.Write(pos);
                id = page.PageNumber;
            }
            var isAsBytes = EndianBitConverter.Little.GetBytes(id);
            var reader = new StructureReader<T>(value, _schema.StructureSchema);

            var pkval = GetPrimaryKeySlice(reader);
            var pkIndex = GetTree(_schema.Key.Name);
            pkIndex.Add(pkval, isAsBytes, 0);

            // TODO: Update indexes
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
                            if (_activeDataSmallSection.TryAllocate(size, out id))
                            {
                                ActiveCandidateSection.Delete(sectionPageNumber);
                                return id;
                            }
                        } while (it.MoveNext());

                    }
                }

                _activeDataSmallSection = ActiveRawDataSmallSection.Create(_tx.LowLevelTransaction);
                _tableTree.Add(TableSchema<T>.ActiveSectionSlice,
                    EndianBitConverter.Little.GetBytes(_activeDataSmallSection.PageNumber));
                var allocationResult = _activeDataSmallSection.TryAllocate(size, out id);
                Debug.Assert(allocationResult);
            }
            return id;
        }

        private unsafe Slice GetPrimaryKeySlice(StructureReader<T> reader)
        {
            var pkSize = 0;
            foreach (var indexedField in _schema.Key.IndexedFields)
            {
                pkSize += reader.GetSize(indexedField);
            }
            var key = new byte[pkSize];
            var slice = new Slice(key);
            fixed (byte* pKey = key)
            {
                var written = 0;
                foreach (var indexedField in _schema.Key.IndexedFields)
                {
                    written += reader.CopyToIndex(indexedField, pKey + written);
                }
            }
            return slice;
        }

        public long NumberOfEntries { get; private set; }

        private Tree GetTree(Slice name)
        {
            if (_trees == null)
                _trees = new Dictionary<Slice, Tree>();
            Tree tree;
            if (_trees.TryGetValue(name, out tree))
                return tree;

            var treeHeader = _tableTree.DirectRead(name);
            if (treeHeader == null)
                throw new InvalidOperationException($"Cannot find tree {name} in table {_schema.Name}");

            tree = Tree.Open(_tx.LowLevelTransaction, _tx, (TreeRootHeader*)treeHeader);
            _trees[name] = tree;
            return tree;
        }

        public void DeleteByKey(Slice key)
        {
            var readResult = GetTree(_schema.Key.Name).Read(key);
            if (readResult == null)
                return;
            var id = readResult.Reader.ReadLittleEndianInt64();
            Delete(id);
        }
    }
}