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
using Bond.IO.Unsafe;
using Bond;
using Bond.Protocols;

namespace Voron.Data.Tables
{
    internal sealed class SharedPool
    {
        public static readonly ObjectPool<OutputBuffer> Buffers = new ObjectPool<OutputBuffer>(() => new OutputBuffer(512));
    }

    internal sealed class SharedPool<T>
    {
        public static readonly ObjectPool<Deserializer<CompactBinaryReader<InputPointer>>> Reader = new ObjectPool<Deserializer<CompactBinaryReader<InputPointer>>>(() => new Deserializer<CompactBinaryReader<InputPointer>>(typeof(T)));        
    }


    public unsafe struct TableHandle<T, TData>
    {
        public static readonly TableHandle<T, TData> Null;

        public T Key;
        private readonly int Size;
        private readonly byte* DataPointer;        

        public TableHandle( T indexKeys, byte* ptr, int size)
        {
            this.Key = indexKeys;
            this.DataPointer = ptr;
            this.Size = size;
        }

        public TData GetValue()
        {
            var readerOfT = SharedPool<TData>.Reader.Allocate();

            try
            {                    
                var input = new InputPointer(DataPointer, Size);
                var reader = new CompactBinaryReader<InputPointer>(input);
                return readerOfT.Deserialize<TData>(reader);
            }
            finally
            {
                SharedPool<TData>.Reader.Free(readerOfT);
            }                
        }

        public override bool Equals(object obj)
        {
            if (obj is TableHandle<T, TData>)
            {
                var o = (TableHandle<T, TData>)obj;
                return o == this;
            }

            return false;
        }

        public override int GetHashCode()
        {
            return (int)(this.Size * 17 + this.DataPointer);
        }

        public static bool operator ==(TableHandle<T, TData> c1, TableHandle<T, TData> c2)
        {
            return c1.DataPointer == c2.DataPointer && c1.Size == c2.Size;
        }

        public static bool operator !=(TableHandle<T, TData> c1, TableHandle<T, TData> c2)
        {
            return !(c1 == c2);
        }

    }

    public unsafe class Table<T, TData>
    {
        private Dictionary<Slice, Tree> _treesBySlice;
        private Dictionary<string, Slice> _sliceByName;

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
                    var inactiveSectionSlice = TableSchema.InactiveSectionSlice;
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
                    var availableSectionSlice = TableSchema.ActiveCandidateSection;
                    _activeCandidateSection = new FixedSizeTree(_tx.LowLevelTransaction, _tableTree, availableSectionSlice, 0);
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
                    var readResult = _tableTree.Read(TableSchema.ActiveSectionSlice);
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
            // Read from pointer. 
            var readerOfT = SharedPool<T>.Reader.Allocate();

            var input = new InputPointer(data, size);
            var reader = new CompactBinaryReader<InputPointer>(input);
            var value = readerOfT.Deserialize<T>(reader);

            SharedPool<T>.Reader.Free(readerOfT);

            DeleteValueFromIndex(previousId, value);
            InsertIndexValuesFor(newId, value);
        }

        public Table(TableSchema<T> schema, Transaction tx)
        {
            _schema = schema;
            _tx = tx;
            _tableTree = _tx.ReadTree(_schema.Name);
            _pageSize = _tx.LowLevelTransaction.DataPager.PageSize;

            var stats = (TableSchemaStats*)_tableTree.DirectRead(TableSchema.StatsSlice);
            if (stats == null)
                throw new InvalidDataException($"Cannot find stats value for table {_schema.Name}");
            NumberOfEntries = stats->NumberOfEntries;
        }

        public TableHandle<T, TData> ReadByKey(Slice key)
        {
            var readResult = GetTree(_schema.Key.Name).Read(key);
            if (readResult == null)
                return default(TableHandle<T, TData>);

            var id = readResult.Reader.ReadLittleEndianInt64();

            int size;
            var rawData = DirectRead(id, out size);

            var readerOfT = SharedPool<T>.Reader.Allocate();
            try
            {
                // Read from pointer. 
                var input = new InputPointer(rawData, size);
                var reader = new CompactBinaryReader<InputPointer>(input);
                var keys = readerOfT.Deserialize<T>(reader);

                return new TableHandle<T, TData>(keys, rawData + input.Position, size - (int)input.Position);
            }
            finally
            {
                SharedPool<T>.Reader.Free(readerOfT);
            }
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

        public void Set(T value, TData data)
        {
            // We create an indexed 
            var pkValue = GetSliceFromStructure(value, _schema.Key);
            var pkIndex = GetTree(_schema.Key.Name);

            var readResult = pkIndex.Read(pkValue);
            if (readResult == null)
            {
                Insert(value, data);
                return;
            }

            long id = readResult.Reader.ReadLittleEndianInt64();
            Update(id, value, data);
        }

        private void Update(long id, T value, TData data)
        {
            var output = SharedPool.Buffers.Allocate();
            output.Position = 0;

            var writer = new CompactBinaryWriter<OutputBuffer>(output);
            Serialize.To(writer, value);
            Serialize.To(writer, data);

            SharedPool.Buffers.Free(output);
 
            int size = (int)output.Position;

            // first, try to fit in place, either in small or large sections
            var prevIsSmall = id % _pageSize != 0;            
            if (size < ActiveDataSmallSection.MaxItemSize)
            {
                byte* pos;
                if (prevIsSmall && ActiveDataSmallSection.TryWriteDirect(id, size, out pos))
                {
                    int oldDataSize;
                    var oldData = ActiveDataSmallSection.DirectRead(id, out oldDataSize);

                    DeleteValueFromIndex(id, oldData, oldDataSize);

                    // MemoryCopy into final position.
                    unsafe
                    {
                        fixed (byte* array = output.Data.Array)
                        {
                            byte* ptr = array + output.Data.Offset;
                            Memory.Copy(pos, ptr, output.Data.Count);
                        }
                    }

                    InsertIndexValuesFor(id, value);
                    return;
                }
            }
            else if (prevIsSmall == false)
            {
                var pageNumber = id / _pageSize;
                var page = _tx.LowLevelTransaction.GetPage(pageNumber);
                var existingNumberOfPages = _tx.LowLevelTransaction.DataPager.GetNumberOfOverflowPages(page.OverflowSize);
                var newNumberOfPages = _tx.LowLevelTransaction.DataPager.GetNumberOfOverflowPages(size);

                if (existingNumberOfPages == newNumberOfPages)
                {
                    page.OverflowSize = size;
                    var pos = page.Pointer + sizeof(PageHeader);

                    DeleteValueFromIndex(id, pos, size);

                    // MemoryCopy into final position.
                    unsafe
                    {
                        fixed (byte* array = output.Data.Array)
                        {
                            byte* ptr = array + output.Data.Offset;
                            Memory.Copy(pos, ptr, output.Data.Count);
                        }
                    }

                    InsertIndexValuesFor(id, value);

                    return;
                }
            }

            // can't fit in place, will just delete & insert instead
            Delete(id);
            Insert(value, data);
        }

        private void Delete(long id)
        {
            int size;
            var ptr = DirectRead(id, out size);
            if (ptr == null)
                return;

            var readerOfT = SharedPool<T>.Reader.Allocate();

            // Read from pointer. 
            var input = new InputPointer(ptr, size);
            var reader = new CompactBinaryReader<InputPointer>(input);
            var value = readerOfT.Deserialize<T>(reader);

            SharedPool<T>.Reader.Free(readerOfT);

            DeleteValueFromIndex(id, value);

            var largeValue = (id % _pageSize) == 0;
            if (largeValue)
            {
                var page = _tx.LowLevelTransaction.GetPage(id / _pageSize);
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
                    throw new InvalidDataException($"Cannot write to newly allocated size in {_schema.Name} during delete");

                Memory.Copy(writePos, pos, itemSize);
            }

            ActiveDataSmallSection.DeleteSection(sectionPageNumber);
        }

        private void DeleteValueFromIndex(long id, byte* ptr, int size)
        {
            var readerOfT = SharedPool<T>.Reader.Allocate();

            var input = new InputPointer(ptr, size);
            var reader = new CompactBinaryReader<InputPointer>(input);
            T value = readerOfT.Deserialize<T>(reader);

            SharedPool<T>.Reader.Free(readerOfT);

            DeleteValueFromIndex(id, value);
        }

        private void DeleteValueFromIndex(long id, T value)
        {
            var keySlice = GetSliceFromStructure(value, _schema.Key);

            var pkTree = GetTree(_schema.Key.Name);
            pkTree.Delete(keySlice);

            foreach (var indexDef in _schema.Indexes.Values)
            {
                var indexTree = GetTree(indexDef.Name);
                var val = GetSliceFromStructure(value, indexDef);

                var fst = new FixedSizeTree(_tx.LowLevelTransaction, indexTree, val, 0);
                fst.Delete(id);
            }
        }

        private void Insert(T value, TData data)
        {
            var stats = (TableSchemaStats*)_tableTree.DirectAdd(TableSchema.StatsSlice, sizeof(TableSchemaStats));
            NumberOfEntries++;
            stats->NumberOfEntries = NumberOfEntries;

            var output = SharedPool.Buffers.Allocate();
            output.Position = 0;

            var writer = new CompactBinaryWriter<OutputBuffer>(output);
            Serialize.To(writer, value);
            Serialize.To(writer, data);

            SharedPool.Buffers.Free(output);

            int size = (int)output.Position;

            long id;
            if (size < ActiveDataSmallSection.MaxItemSize)
            {
                id = AllocateFromSmallActiveSection(size);

                byte* pos;
                if (ActiveDataSmallSection.TryWriteDirect(id, size, out pos) == false)
                    throw new InvalidOperationException($"After successfully allocating {size:#,#;;0} bytes, failed to write them on {_schema.Name}");

                // MemoryCopy into final position.
                unsafe
                {
                    fixed (byte* array = output.Data.Array)
                    {
                        byte* ptr = array + output.Data.Offset;
                        Memory.Copy(pos, ptr, output.Data.Count);
                    }
                }
            }
            else
            {
                var numberOfOverflowPages = _tx.LowLevelTransaction.DataPager.GetNumberOfOverflowPages(size);
                var page = _tx.LowLevelTransaction.AllocatePage(numberOfOverflowPages);
                page.Flags = PageFlags.Overflow | PageFlags.RawData;
                page.OverflowSize = size;                

                byte* pos = page.Pointer + sizeof(PageHeader);

                unsafe
                {
                    fixed (byte* array = output.Data.Array)
                    {
                        byte* ptr = array + output.Data.Offset;
                        Memory.Copy(pos, ptr, output.Data.Count);
                    }
                }

                id = page.PageNumber;
            }

            InsertIndexValuesFor(id, value);
        }

        private void InsertIndexValuesFor(long id, T value)
        {
            var isAsBytes = EndianBitConverter.Little.GetBytes(id);

            var pkval = GetSliceFromStructure(value, _schema.Key);
            var pkIndex = GetTree(_schema.Key.Name);
            pkIndex.Add(pkval, isAsBytes, 0);

            foreach (var indexDef in _schema.Indexes.Values)
            {
                var indexTree = GetTree(indexDef.Name);
                var val = GetSliceFromStructure(value, indexDef);
                var index = new FixedSizeTree(_tx.LowLevelTransaction, indexTree, val, 0);
                index.Add(id);
            }
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
                _tableTree.Add(TableSchema.ActiveSectionSlice, EndianBitConverter.Little.GetBytes(_activeDataSmallSection.PageNumber));

                var allocationResult = _activeDataSmallSection.TryAllocate(size, out id);

                Debug.Assert(allocationResult);
            }
            return id;
        }

        private Slice GetSliceFromStructure(T reader, TableSchema<T>.SchemaIndexDef definition)
        {
            return definition.CreateKey(reader);
        }

        public long NumberOfEntries { get; private set; }

        private Tree GetTree(string name, out Slice slice)
        {
            if (_sliceByName == null)
                _sliceByName = new Dictionary<string, Slice>();
            if (_sliceByName.TryGetValue(name, out slice) == false)
            {
                slice = name;
                _sliceByName[name] = slice;
            }
            return GetTree(slice);

        }

        private Tree GetTree(Slice name)
        {
            if (_treesBySlice == null)
                _treesBySlice = new Dictionary<Slice, Tree>();

            Tree tree;
            if (_treesBySlice.TryGetValue(name, out tree))
                return tree;

            var treeHeader = _tableTree.DirectRead(name);
            if (treeHeader == null)
                throw new InvalidOperationException($"Cannot find tree {name} in table {_schema.Name}");

            tree = Tree.Open(_tx.LowLevelTransaction, _tx, (TreeRootHeader*)treeHeader);
            _treesBySlice[name] = tree;
            return tree;
        }

        public void DeleteByKey(Slice key)
        {
            var readResult = GetTree(_schema.Key.Name).Read(key);
            if (readResult == null)
                return;

            // This is an implementation detail. We read the absolute location pointer (absolute offset on the file)
            var id = readResult.Reader.ReadLittleEndianInt64();

            // And delete the element accordingly. 
            Delete(id);
        }

        private IEnumerable<TableHandle<T, TData>> GetSecondaryIndexForValue(Tree tree, Slice value)
        {
            var fstIndex = new FixedSizeTree(_tx.LowLevelTransaction, tree, value, 0);
            using (var it = fstIndex.Iterate())
            {
                if (it.Seek(long.MinValue) == false)
                    yield break;

                do
                {
                    var id = it.CurrentKey;
                    yield return ReadById(id);
                } while (it.MoveNext());
            }
        }

        public class SeekResult
        {
            public Slice Key;
            public IEnumerable<TableHandle<T, TData>> Results;
        }

        //TODO: need a proper way to handle this instead of just saying slice
        public IEnumerable<SeekResult> SeekTo(string indexName, Slice secondaryIndexValue)
        {
            Slice treeNameSlice;
            var tree = GetTree(indexName, out treeNameSlice);
            using (var it = tree.Iterate())
            {
                if(it.Seek(secondaryIndexValue) == false)
                    yield break;

                do
                {
                    yield return new SeekResult
                    {
                        Key = it.CurrentKey,
                        Results = GetSecondaryIndexForValue(tree, it.CurrentKey)
                    };

                }
                while (it.MoveNext());
            }
        }

        private TableHandle<T, TData> ReadById(long id)
        {
            int size;
            byte* rawData = DirectRead(id, out size);

            // Read from pointer. 
            var readerOfT = SharedPool<T>.Reader.Allocate();
            try
            {
                var input = new InputPointer(rawData, size);
                var reader = new CompactBinaryReader<InputPointer>(input);
                var keys = readerOfT.Deserialize<T>(reader);

                return new TableHandle<T, TData>(keys, rawData + input.Position, size - (int)input.Position);
            }
            finally
            {
                SharedPool<T>.Reader.Free(readerOfT);
            }
        }
    }
}