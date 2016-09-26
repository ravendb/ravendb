using Sparrow;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Sparrow.Binary;
using Voron.Data.BTrees;
using Voron.Data.RawData;
using Voron.Impl;
using Voron.Util.Conversion;

namespace Voron.Data.Tables
{
    [Flags]
    public enum TableIndexType
    {
        Default = 0x01,
        BTree = 0x01,
    }

    public unsafe class TableSchema
    {
        public static readonly Slice ActiveSectionSlice;
        public static readonly Slice InactiveSectionSlice;
        public static readonly Slice ActiveCandidateSectionSlice;
        public static readonly Slice StatsSlice;
        public static readonly Slice SchemasSlice;
        public static readonly Slice PkSlice;

        private SchemaIndexDef _primaryKey;
        private readonly Dictionary<Slice, SchemaIndexDef> _indexes = new Dictionary<Slice, SchemaIndexDef>(SliceComparer.Instance);
        private readonly Dictionary<Slice, FixedSizeSchemaIndexDef> _fixedSizeIndexes = new Dictionary<Slice, FixedSizeSchemaIndexDef>(SliceComparer.Instance);

        static TableSchema()
        {
            Slice.From(StorageEnvironment.LabelsContext, "Active-Section", ByteStringType.Immutable, out ActiveSectionSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Inactive-Section", ByteStringType.Immutable, out InactiveSectionSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Active-Candidate-Section", ByteStringType.Immutable, out ActiveCandidateSectionSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Stats", ByteStringType.Immutable, out StatsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Schemas", ByteStringType.Immutable, out SchemasSlice);
            Slice.From(StorageEnvironment.LabelsContext, "PK", ByteStringType.Immutable, out PkSlice);
        }

        public SchemaIndexDef Key => _primaryKey;
        public Dictionary<Slice, SchemaIndexDef> Indexes => _indexes;
        public Dictionary<Slice, FixedSizeSchemaIndexDef> FixedSizeIndexes => _fixedSizeIndexes;

        public class SchemaIndexDef
        {
            public TableIndexType Type = TableIndexType.Default;

            /// <summary>
            /// Here we take advantage on the fact that the values are laid out in memory sequentially
            /// we can point to a certain item index, and use one or more fields in the key directly, 
            /// without any copying
            /// </summary>
            public int StartIndex = -1;
            public int Count = -1;
            public bool IsGlobal;
            public Slice Name;

            public ByteStringContext.Scope GetSlice(ByteStringContext context, TableValueReader value, out Slice slice)
            {
                int totalSize;
                var ptr = value.Read(StartIndex, out totalSize);
#if DEBUG
                if (totalSize < 0)
                    throw new ArgumentOutOfRangeException(nameof(totalSize), "Size cannot be negative");
#endif
                for (var i = 1; i < Count; i++)
                {
                    int size;
                    value.Read(i + StartIndex, out size);
#if DEBUG
                    if (size < 0)
                        throw new ArgumentOutOfRangeException(nameof(size), "Size cannot be negative");
#endif
                    totalSize += size;
                }
#if DEBUG
                if (totalSize < 0 || totalSize > value.Size)
                    throw new ArgumentOutOfRangeException(nameof(value), "Reading a slice that is longer than the value");
                if (totalSize > ushort.MaxValue)
                    throw new ArgumentOutOfRangeException(nameof(totalSize), "Reading a slice that too big to be a slice");
#endif
                return Slice.External(context, ptr, (ushort)totalSize, out slice);
            }

            public byte[] Serialize()
            {
                // We serialize the Type enum as ulong to be "future-proof"
                var castedType = (long)Type;

                fixed (int* startIndex = &StartIndex)
                fixed (int* count = &Count)
                fixed (bool* isGlobal = &IsGlobal)
                {
                    var serializer = new TableValueBuilder
                            {
                                &castedType,
                                startIndex,
                                count,
                                isGlobal,
                                Name
                            };

                    byte[] serialized = new byte[serializer.Size];

                    fixed (byte* destination = serialized)
                    {
                        serializer.CopyTo(destination);
                    }

                    return serialized;
                }

            }

            public static SchemaIndexDef ReadFrom(ByteStringContext context, byte* location, int size)
            {
                var input = new TableValueReader(location, size);
                var indexDef = new SchemaIndexDef();

                int currentSize;
                byte* currentPtr = input.Read(0, out currentSize);
                indexDef.Type = (TableIndexType) (*(ulong*) currentPtr);

                currentPtr = input.Read(1, out currentSize);
                indexDef.StartIndex = *(int*) currentPtr;

                currentPtr = input.Read(2, out currentSize);
                indexDef.Count = *(int*) currentPtr;

                currentPtr = input.Read(3, out currentSize);
                indexDef.IsGlobal = Convert.ToBoolean(*currentPtr);

                currentPtr = input.Read(4, out currentSize);
                Slice.From(context, currentPtr, currentSize, ByteStringType.Immutable, out indexDef.Name);

                return indexDef;
            }

            public void Validate(SchemaIndexDef actual)
            {
                if (actual == null)
                    throw new ArgumentNullException(nameof(actual), "Expected an index but received null");

                if (!SliceComparer.Equals(Name, actual.Name))
                    throw new ArgumentException(
                        $"Expected index to have Name='{Name}', got Name='{actual.Name}' instead",
                        nameof(actual));

                if (Type != actual.Type)
                    throw new ArgumentException(
                        $"Expected index {Name} to have Type='{Type}', got Type='{actual.Type}' instead",
                        nameof(actual));

                if (StartIndex != actual.StartIndex)
                    throw new ArgumentException(
                        $"Expected index {Name} to have StartIndex='{StartIndex}', got StartIndex='{actual.StartIndex}' instead",
                        nameof(actual));

                if (Count != actual.Count)
                    throw new ArgumentException(
                        $"Expected index {Name} to have Count='{Count}', got Count='{actual.Count}' instead",
                        nameof(actual));

                if (IsGlobal != actual.IsGlobal)
                    throw new ArgumentException(
                        $"Expected index {Name} to have IsGlobal='{IsGlobal}', got IsGlobal='{actual.IsGlobal}' instead",
                        nameof(actual));
            }
        }

        public class FixedSizeSchemaIndexDef
        {
            public int StartIndex = -1;
            public bool IsGlobal;
            public Slice Name;

            public long GetValue(TableValueReader value)
            {
                int totalSize;
                var ptr = value.Read(StartIndex, out totalSize);
                return EndianBitConverter.Big.ToInt64(ptr);
            }

            public byte[] Serialize()
            {
                fixed (int* startIndex = &StartIndex)
                fixed (bool* isGlobal = &IsGlobal)
                {
                    var serializer = new TableValueBuilder
                    {
                        startIndex,
                        isGlobal,
                        Name
                    };

                    byte[] serialized = new byte[serializer.Size];

                    fixed (byte* destination = serialized)
                    {
                        serializer.CopyTo(destination);
                    }

                    return serialized;
                }
            }

            public static FixedSizeSchemaIndexDef ReadFrom(ByteStringContext context, byte* location, int size)
            {
                var input = new TableValueReader(location, size);
                var output = new FixedSizeSchemaIndexDef();

                int currentSize;
                byte* currentPtr = input.Read(0, out currentSize);
                output.StartIndex = *(int*) currentPtr;

                currentPtr = input.Read(1, out currentSize);
                output.IsGlobal = Convert.ToBoolean(*currentPtr);

                currentPtr = input.Read(2, out currentSize);
                Slice.From(context, currentPtr, currentSize, ByteStringType.Immutable, out output.Name);

                return output;
            }

            public void Validate(FixedSizeSchemaIndexDef actual)
            {
                if (actual == null)
                    throw new ArgumentNullException(nameof(actual), "Expected an index but received null");

                if (!SliceComparer.Equals(Name, actual.Name))
                    throw new ArgumentException(
                        $"Expected index to have Name='{Name}', got Name='{actual.Name}' instead",
                        nameof(actual));

                if (StartIndex != actual.StartIndex)
                    throw new ArgumentException(
                        $"Expected index {Name} to have StartIndex='{StartIndex}', got StartIndex='{actual.StartIndex}' instead",
                        nameof(actual));

                if (IsGlobal != actual.IsGlobal)
                    throw new ArgumentException(
                        $"Expected index {Name} to have IsGlobal='{IsGlobal}', got IsGlobal='{actual.IsGlobal}' instead",
                        nameof(actual));
            }
        }        

        public TableSchema DefineIndex(SchemaIndexDef index)
        {
            if (!index.Name.HasValue || SliceComparer.Equals(Slices.Empty, index.Name))
                throw new ArgumentException("Index name must be non-empty", nameof(index));

            _indexes[index.Name] = index;

            return this;
        }

        public TableSchema DefineFixedSizeIndex(FixedSizeSchemaIndexDef index)
        {
            if (!index.Name.HasValue || SliceComparer.Equals(Slices.Empty, index.Name))
                throw new ArgumentException("Fixed size index name must be non-empty", nameof(index));

            _fixedSizeIndexes[index.Name] = index;

            return this;
        }

        public TableSchema DefineKey(SchemaIndexDef index)
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
        public void Create(Transaction tx, string name)
        {
            if (_primaryKey == null && _indexes.Count == 0 && _fixedSizeIndexes.Count == 0)
                throw new InvalidOperationException($"Cannot create table {name} without a primary key and no indexes");

            var tableTree = tx.CreateTree(name, RootObjectType.Table);
            if (tableTree.State.NumberOfEntries > 0)
                return; // this was already created

            // Create raw data. This is where we will actually store the documents
            var rawDataActiveSection = ActiveRawDataSmallSection.Create(tx.LowLevelTransaction, name);

            long val = rawDataActiveSection.PageNumber;
            Slice pageNumber;
            using (Slice.External(tx.Allocator, (byte*)&val, sizeof(long), ByteStringType.Immutable, out pageNumber))
            {
                tableTree.Add(ActiveSectionSlice, pageNumber);
            }
            
            var stats = (TableSchemaStats*)tableTree.DirectAdd(StatsSlice, sizeof(TableSchemaStats));
            stats->NumberOfEntries = 0;

            if (_primaryKey != null)
            {
                if (_primaryKey.IsGlobal == false)
                {
                    var indexTree = Tree.Create(tx.LowLevelTransaction, tx);
                    var treeHeader = tableTree.DirectAdd(_primaryKey.Name, sizeof(TreeRootHeader));
                    indexTree.State.CopyTo((TreeRootHeader*)treeHeader);
                }
                else
                {
                    tx.CreateTree(_primaryKey.Name.ToString());
                }
            }

            foreach (var indexDef in _indexes.Values)
            {
                if (indexDef.IsGlobal == false)
                {
                    var indexTree = Tree.Create(tx.LowLevelTransaction, tx);
                    var treeHeader = tableTree.DirectAdd(indexDef.Name, sizeof(TreeRootHeader));
                    indexTree.State.CopyTo((TreeRootHeader*)treeHeader);
                }
                else
                {
                    tx.CreateTree(indexDef.Name.ToString());
                }
            }

            // Serialize the schema into the table's tree
            var serializer = SerializeSchema();
            var schemaRepresentation = tableTree.DirectAdd(SchemasSlice, serializer.Length);

            fixed (byte* source = serializer)
            {
                Memory.Copy(schemaRepresentation, source, serializer.Length);
            }
        }

        /// <summary>
        /// Serializes structure into a byte array:
        /// 
        ///  1. Whether the schema has a primary key
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
            bool hasPrimaryKey = _primaryKey != null;

            structure.Add(BitConverter.GetBytes(hasPrimaryKey));

            if (hasPrimaryKey)
                structure.Add(_primaryKey.Serialize());

            structure.Add(BitConverter.GetBytes(_indexes.Count));
            structure.AddRange(_indexes.Values.Select(index => index.Serialize()));

            structure.Add(BitConverter.GetBytes(_fixedSizeIndexes.Count));
            structure.AddRange(_fixedSizeIndexes.Values.Select(index => index.Serialize()));

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

            int currentSize;
            byte* currentPtr = input.Read(currentIndex++, out currentSize);

            bool hasPrimaryKey = Convert.ToBoolean(*currentPtr);
            if (hasPrimaryKey)
            {
                currentPtr = input.Read(currentIndex++, out currentSize);
                var pk = SchemaIndexDef.ReadFrom(context, currentPtr, currentSize);
                schema.DefineKey(pk);
            }

            // Read common schema indexes
            currentPtr = input.Read(currentIndex++, out currentSize);
            int indexCount = *(int*) currentPtr;

            while (indexCount > 0)
            {
                currentPtr = input.Read(currentIndex++, out currentSize);
                var indexSchemaDef = SchemaIndexDef.ReadFrom(context, currentPtr, currentSize);
                schema.DefineIndex(indexSchemaDef);

                indexCount--;
            }

            // Read fixed size schema indexes
            currentPtr = input.Read(currentIndex++, out currentSize);
            indexCount = *(int*) currentPtr;

            while (indexCount > 0)
            {
                currentPtr = input.Read(currentIndex++, out currentSize);
                var fixedIndexSchemaDef = FixedSizeSchemaIndexDef.ReadFrom(context, currentPtr, currentSize);
                schema.DefineFixedSizeIndex(fixedIndexSchemaDef);

                indexCount--;
            }

            return schema;
        }

        public void Validate(TableSchema actual)
        {
            if (actual == null)
                throw new ArgumentNullException(nameof(actual), "Expected a schema but received null");

            if (_primaryKey != null)
                _primaryKey.Validate(actual._primaryKey);
            else if (actual._primaryKey != null)
                throw new ArgumentException(
                    "Expected schema not to have a primary key",
                    nameof(actual));

            if (_indexes.Count != actual._indexes.Count)
                throw new ArgumentException(
                    "Expected schema to have the same number of variable size indexes, but it does not",
                    nameof(actual));

            foreach (var entry in _indexes)
            {
                SchemaIndexDef index;

                if (!actual._indexes.TryGetValue(entry.Key, out index))
                    throw new ArgumentException(
                        $"Expected schema to have an index named {entry.Key}",
                        nameof(actual));

                entry.Value.Validate(index);
            }

            if (_fixedSizeIndexes.Count != actual._fixedSizeIndexes.Count)
                throw new ArgumentException(
                    "Expected schema to have the same number of fixed size indexes, but it does not",
                    nameof(actual));

            foreach (var entry in _fixedSizeIndexes)
            {
                FixedSizeSchemaIndexDef index;

                if (!actual._fixedSizeIndexes.TryGetValue(entry.Key, out index))
                    throw new ArgumentException(
                        $"Expected schema to have an index named {entry.Key}",
                        nameof(actual));

                entry.Value.Validate(index);
            }

        }
    }
}