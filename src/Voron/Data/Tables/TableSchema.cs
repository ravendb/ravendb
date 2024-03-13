using Sparrow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Sparrow.Server;
using Voron.Data.BTrees;
using Voron.Data.RawData;
using Voron.Impl;

namespace Voron.Data.Tables
{
    public unsafe partial class TableSchema
    {
        public static readonly Slice ActiveSectionSlice;
        public static readonly Slice InactiveSectionSlice;
        public static readonly Slice ActiveCandidateSectionSlice;
        public static readonly Slice StatsSlice;
        public static readonly Slice SchemasSlice;
        public static readonly Slice PkSlice;
        public static readonly Slice CompressionDictionariesSlice;
        public static readonly Slice CurrentCompressionDictionaryIdSlice;

        private IndexDef _primaryKey;
        private bool _compressed;

        public FixedSizeKeyIndexDef CompressedEtagSourceIndex;

        private readonly Dictionary<Slice, IndexDef> _commonIndexes = new(SliceComparer.Instance);

        private readonly Dictionary<Slice, DynamicKeyIndexDef> _dynamicKeyIndexes = new(SliceComparer.Instance);

        private readonly Dictionary<Slice, FixedSizeKeyIndexDef> _fixedSizeIndexes = new(SliceComparer.Instance);

        public byte TableType { get; set; }

        static TableSchema()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "Active-Section", ByteStringType.Immutable, 
                    out ActiveSectionSlice);
                Slice.From(ctx, "Inactive-Section", ByteStringType.Immutable,
                    out InactiveSectionSlice);
                Slice.From(ctx, "Active-Candidate-Section", ByteStringType.Immutable,
                    out ActiveCandidateSectionSlice);
                Slice.From(ctx, "Stats", ByteStringType.Immutable, out StatsSlice);
                Slice.From(ctx, "Schemas", ByteStringType.Immutable, out SchemasSlice);
                Slice.From(ctx, "PK", ByteStringType.Immutable, out PkSlice);
                Slice.From(ctx, "CompressionDictionaries", ByteStringType.Immutable, out CompressionDictionariesSlice);
                Slice.From(ctx, "CurrentCompressionDictionaryId", ByteStringType.Immutable, out CurrentCompressionDictionaryIdSlice);
            }
        }

        public IndexDef Key => _primaryKey;

        // Indexes are conceptually Dictionary<index name, Dictionary<unique index value, HashSet<storage id>>

        /// <summary>
        /// Indexes are conceptually Dictionary&lt;index name, Dictionary&lt;unique index value, HashSet&lt;storage id&gt;&gt;
        /// </summary>
        public Dictionary<Slice, IndexDef> Indexes => _commonIndexes;

        // FixedSizeIndexes are conceptually Dictionary<index name, Dictionary<long value, storage id>>

        /// <summary>
        /// FixedSizeIndexes are conceptually Dictionary&lt;index name, Dictionary&lt;long value, storage id&gt;&gt;
        /// </summary>
        public Dictionary<Slice, FixedSizeKeyIndexDef> FixedSizeIndexes => _fixedSizeIndexes;


        public Dictionary<Slice, DynamicKeyIndexDef> DynamicKeyIndexes => _dynamicKeyIndexes;


        public TableSchema CompressValues(FixedSizeKeyIndexDef etagSource, bool compress)
        {
            _compressed = compress;
            CompressedEtagSourceIndex = etagSource ?? throw new ArgumentNullException(nameof(etagSource));
            return this;
        }

        public bool Compressed
        {
            get => _compressed;
            set => _compressed = value;
        }

        public TableSchema DefineIndex(IndexDef index)
        {
            index.Validate();

            _commonIndexes[index.Name] = index;

            return this;
        }


        public TableSchema DefineIndex(DynamicKeyIndexDef index)
        {
            index.Validate();

            _dynamicKeyIndexes[index.Name] = index;

            return this;
        }

        public TableSchema DefineFixedSizeIndex(FixedSizeKeyIndexDef index)
        {
            if (!index.Name.HasValue || SliceComparer.Equals(Slices.Empty, index.Name))
                throw new ArgumentException("Fixed size index name must be non-empty", nameof(index));

            _fixedSizeIndexes[index.Name] = index;

            return this;
        }

        public TableSchema DefineKey(IndexDef index)
        {
            bool hasEmptyName = !index.Name.HasValue || SliceComparer.Equals(Slices.Empty, index.Name);

            if (index.IsGlobal && hasEmptyName)
                throw new ArgumentException("Name must be non empty for global index as primary key", nameof(index));

            if (hasEmptyName)
                index.Name = PkSlice;

            if (index.Count > 1)
                throw new InvalidOperationException("Primary key must be a single field");

            _primaryKey = index;

            return this;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void Create(Transaction tx, string name, ushort? sizeInPages)
        {
            Slice.From(tx.Allocator, name, ByteStringType.Immutable, out Slice nameSlice);
            Create(tx, nameSlice, sizeInPages);
        }
        
        /// <summary>
        /// A table is stored inside a tree, and has the following keys in it
        /// 
        /// - active-section -> page number - the page number of the current active small data section
        /// - inactive-sections -> fixed size tree with no content where the keys are the page numbers of inactive small raw data sections
        /// - large-values -> fixed size tree with no content where the keys are the page numbers of the large values
        /// - for each index:
        ///     - If can fit into fixed size tree, use that.
        ///     - Otherwise, create a tree (whose key would be the indexed field value and the value would 
        ///         be a fixed size tree of the ids of all the matching values)
        ///  - stats -> header information about the table (number of entries, etc)
        ///  - schemas -> schema definition for the table
        /// 
        /// </summary>
        public void Create(Transaction tx, Slice name, ushort? sizeInPages)
        {
            if (_primaryKey == null && _commonIndexes.Count == 0 && _dynamicKeyIndexes.Count == 0 && _fixedSizeIndexes.Count == 0)
                throw new InvalidOperationException($"Cannot create table {name} without a primary key and no indexes");

            if (_primaryKey?.IsGlobal != false &&
                _commonIndexes.All(x => x.Value.IsGlobal) &&
                _dynamicKeyIndexes.All(x => x.Value.IsGlobal) &&
                _fixedSizeIndexes.All(x => x.Value.IsGlobal))
                throw new InvalidOperationException($"Cannot create table {name} with a global primary key and without at least a single local index, " +
                                                    "otherwise we can't compact it, this is a bug in the table schema.");

            var tableTree = tx.CreateTree(name, RootObjectType.Table);
            if (tableTree.State.Header.NumberOfEntries > 0)
                return; // this was already created

            tableTree.Add(CurrentCompressionDictionaryIdSlice, 0);

            // Create raw data. This is where we will actually store the documents
            var rawDataActiveSection = ActiveRawDataSmallSection.Create(tx, name, TableType, sizeInPages);
            long val = rawDataActiveSection.PageNumber;

            using (Slice.External(tx.Allocator, (byte*)&val, sizeof(long), ByteStringType.Immutable, out Slice pageNumber))
            {
                tableTree.Add(ActiveSectionSlice, pageNumber);
            }

            using (tableTree.DirectAdd(StatsSlice, sizeof(TableSchemaStats), out var ptr))
            {
                var stats = (TableSchemaStats*)ptr;
                stats->NumberOfEntries = 0;
            }

            var tablePageAllocator = new NewPageAllocator(tx.LowLevelTransaction, tableTree);
            tablePageAllocator.Create();

            var globalPageAllocator = new NewPageAllocator(tx.LowLevelTransaction,
                tx.LowLevelTransaction.RootObjects);
            globalPageAllocator.Create();
                
            if (_primaryKey != null)
            {
                if (_primaryKey.IsGlobal == false)
                {
                    var indexTree = Tree.Create(tx.LowLevelTransaction, tx, _primaryKey.Name, isIndexTree: true, newPageAllocator: tablePageAllocator);
                    using (tableTree.DirectAdd(_primaryKey.Name, sizeof(TreeRootHeader), out var ptr))
                    {
                        indexTree.State.CopyTo((TreeRootHeader*)ptr);
                    }
                }
                else
                {
                    tx.CreateTree(_primaryKey.Name.ToString(), isIndexTree: true, newPageAllocator: globalPageAllocator);
                }
            }

            foreach (var indexDef in _commonIndexes.Values)
            {
                if (indexDef.IsGlobal == false)
                {
                    var indexTree = Tree.Create(tx.LowLevelTransaction, tx, indexDef.Name, isIndexTree: true, newPageAllocator: tablePageAllocator);
                    using (tableTree.DirectAdd(indexDef.Name, sizeof(TreeRootHeader), out var ptr))
                    {
                        indexTree.State.CopyTo((TreeRootHeader*)ptr);
                    }
                }
                else
                {
                    tx.CreateTree(indexDef.Name.ToString(), isIndexTree: true, newPageAllocator: globalPageAllocator);
                }
            }

            foreach (var dynamicKeyIndexDef in _dynamicKeyIndexes.Values)
            {
                if (dynamicKeyIndexDef.IsGlobal == false)
                {
                    var indexTree = Tree.Create(tx.LowLevelTransaction, tx, dynamicKeyIndexDef.Name, isIndexTree: true, newPageAllocator: tablePageAllocator);
                    using (tableTree.DirectAdd(dynamicKeyIndexDef.Name, sizeof(TreeRootHeader), out var ptr))
                    {
                        indexTree.State.CopyTo((TreeRootHeader*)ptr);
                    }
                }
                else
                {
                    tx.CreateTree(dynamicKeyIndexDef.Name.ToString(), isIndexTree: true, newPageAllocator: globalPageAllocator);
                }
            }

            // Serialize the schema into the table's tree
            SerializeSchemaIntoTableTree(tableTree);
        }

        internal void SerializeSchemaIntoTableTree(Tree tableTree)
        {
            var serializer = SerializeSchema();

            using (tableTree.DirectAdd(SchemasSlice, serializer.Length, out var ptr))
            {
                fixed (byte* source = serializer)
                {
                    Memory.Copy(ptr, source, serializer.Length);
                }
            }
        }

        [Flags]
        private enum TableSchemaSerializationFlag : byte
        {
            None = 0,
            HasPrimaryKey = 1,
            IsCompressed = 2
        }

        /// <summary>
        /// Serializes structure into a byte array:
        /// 
        ///  1. flags - Whether the schema has a primary key
        ///  2. The primary key (if present)
        ///  3. Number of composite indexes
        ///  4. Values of the composite indexes
        ///  5. Number of fixed size indexes
        ///  6. Values of the fixed size indexes
        /// </summary>
        /// <returns></returns>
        internal byte[] SerializeSchema()
        {
            // Create list of serialized substructures
            var structure = new List<byte[]>();
            var flags = TableSchemaSerializationFlag.None;
            if (_primaryKey != null)
                flags |= TableSchemaSerializationFlag.HasPrimaryKey;

            if(_compressed)
                flags|= TableSchemaSerializationFlag.IsCompressed;

            structure.Add(new []{ (byte)flags });

            if (_primaryKey != null)
                structure.Add(_primaryKey.Serialize());

            structure.Add(BitConverter.GetBytes(_commonIndexes.Count + _dynamicKeyIndexes.Count));
            structure.AddRange(_commonIndexes.Values.Select(index => index.Serialize()));
            structure.AddRange(_dynamicKeyIndexes.Values.Select(index => index.Serialize()));

            structure.Add(BitConverter.GetBytes(_fixedSizeIndexes.Count));
            structure.AddRange(_fixedSizeIndexes.Values.Select(index => index.Serialize()));
            structure.Add(BitConverter.GetBytes(CompressedEtagSourceIndex == null ? 0 : 1));
            if (CompressedEtagSourceIndex != null)
                structure.Add(CompressedEtagSourceIndex.Serialize());

            var totalSize = structure.Select((bytes, i) => bytes.Length).Sum();
            var packed = new byte[totalSize];
            int position = 0;

            fixed (byte* ptr = packed)
            {
                var serializer = new TableValueBuilder();

                foreach (var member in structure)
                {
                    member.CopyTo(packed, position);
                    serializer.Add(&ptr[position], member.Length);
                    position += member.Length;
                }

                var output = new byte[serializer.Size];

                fixed (byte* outputPtr = output)
                {
                    serializer.CopyTo(outputPtr);
                }

                return output;
            }
        }

        public static TableSchema ReadFrom(ByteStringContext context, byte* location, int size)
        {
            var input = new TableValueReader(location, size);
            var schema = new TableSchema();

            // Since there might not be a primary key, we have a moving index to deserialize the schema
            int currentIndex = 0;

            byte* currentPtr = input.Read(currentIndex++, out var currentSize);

            TableSchemaSerializationFlag flags = (TableSchemaSerializationFlag)(*currentPtr);
            bool hasPrimaryKey = flags.HasFlag(TableSchemaSerializationFlag.HasPrimaryKey);
            if (hasPrimaryKey)
            {
                currentPtr = input.Read(currentIndex++, out currentSize);
                var tvr = new TableValueReader(currentPtr, currentSize);
                var pk = IndexDef.ReadFrom(context, ref tvr);
                schema.DefineKey(pk);
            }

            schema._compressed = flags.HasFlag(TableSchemaSerializationFlag.IsCompressed);

            // Read common schema indexes and dynamic key indexes
            currentPtr = input.Read(currentIndex++, out currentSize);
            int indexCount = *(int*)currentPtr;

            while (indexCount > 0)
            {
                currentPtr = input.Read(currentIndex++, out currentSize);
                ReadTreeIndexDefinition(context, schema, currentPtr, currentSize);

                indexCount--;
            }

            // Read fixed size schema indexes
            currentPtr = input.Read(currentIndex++, out currentSize);
            indexCount = *(int*)currentPtr;

            while (indexCount > 0)
            {
                currentPtr = input.Read(currentIndex++, out currentSize);
                var fixedIndexSchemaDef = FixedSizeKeyIndexDef.ReadFrom(context, currentPtr, currentSize);
                schema.DefineFixedSizeIndex(fixedIndexSchemaDef);

                indexCount--;
            }

            if (currentIndex < input.Count)
            {
                currentPtr = input.Read(currentIndex++, out currentSize);
                if (*(int*)currentPtr != 0)
                {
                    currentPtr = input.Read(currentIndex, out currentSize);
                    schema.CompressedEtagSourceIndex = FixedSizeKeyIndexDef.ReadFrom(context, currentPtr, currentSize);
                }
            }

            return schema;
        }

        private static void ReadTreeIndexDefinition(ByteStringContext context, TableSchema schema, byte* currentPtr, int currentSize)
        {
            var reader = new TableValueReader(currentPtr, currentSize);
            var type = (TreeIndexType)(*(ulong*)reader.Read(0, out _));

            switch (type)
            {
                case TreeIndexType.Default:
                {
                    var index = IndexDef.ReadFrom(context, ref reader);
                    schema.DefineIndex(index);
                    break;
                }
                case TreeIndexType.DynamicKeyValues:
                {
                    var index = DynamicKeyIndexDef.ReadFrom(context, ref reader);
                    schema.DefineIndex(index);
                    break;
                }
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public void Validate(TableSchema actual)
        {
            if (actual == null)
                throw new ArgumentNullException(nameof(actual), "Expected a schema but received null");

            if (_primaryKey != null)
                _primaryKey.EnsureIdentical(actual._primaryKey);
            else if (actual._primaryKey != null)
                throw new ArgumentException(
                    "Expected schema not to have a primary key",
                    nameof(actual));

            if (_commonIndexes.Count != actual._commonIndexes.Count)
                throw new ArgumentException(
                    "Expected schema to have the same number of variable size indexes, but it does not",
                    nameof(actual));

            foreach (var entry in _commonIndexes)
            {
                if (!actual._commonIndexes.TryGetValue(entry.Key, out var index))
                    throw new ArgumentException(
                        $"Expected schema to have an index named {entry.Key}",
                        nameof(actual));

                entry.Value.EnsureIdentical(index);
            }

            if (_dynamicKeyIndexes.Count != actual._dynamicKeyIndexes.Count)
                throw new ArgumentException(
                    "Expected schema to have the same number of dynamic-key indexes, but it does not",
                    nameof(actual));

            foreach (var entry in _dynamicKeyIndexes)
            {
                if (!actual._dynamicKeyIndexes.TryGetValue(entry.Key, out var index))
                    throw new ArgumentException(
                        $"Expected schema to have an dynamic-key index named {entry.Key}",
                        nameof(actual));

                entry.Value.EnsureIdentical(index);
            }

            if (_fixedSizeIndexes.Count != actual._fixedSizeIndexes.Count)
                throw new ArgumentException(
                    "Expected schema to have the same number of fixed size indexes, but it does not",
                    nameof(actual));

            foreach (var entry in _fixedSizeIndexes)
            {
                if (!actual._fixedSizeIndexes.TryGetValue(entry.Key, out FixedSizeKeyIndexDef index))
                    throw new ArgumentException(
                        $"Expected schema to have an index named {entry.Key}",
                        nameof(actual));

                entry.Value.EnsureIdentical(index);
            }
        }
    }
}
