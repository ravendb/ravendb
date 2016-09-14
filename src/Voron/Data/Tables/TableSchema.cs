using Sparrow;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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

    public unsafe class TableSchema : IDisposable
    {
        public static readonly Slice ActiveSection = Slice.From(StorageEnvironment.LabelsContext, "Active-Section", ByteStringType.Immutable);
        public static readonly Slice InactiveSection = Slice.From(StorageEnvironment.LabelsContext, "Inactive-Section", ByteStringType.Immutable);
        public static readonly Slice ActiveCandidateSection = Slice.From(StorageEnvironment.LabelsContext, "Active-Candidate-Section", ByteStringType.Immutable);
        public static readonly Slice Stats = Slice.From(StorageEnvironment.LabelsContext, "Stats", ByteStringType.Immutable);
        public static readonly Slice Schemas = Slice.From(StorageEnvironment.LabelsContext, "Schemas", ByteStringType.Immutable);

        public SchemaIndexDef Key => _pk;
        public Dictionary<string, SchemaIndexDef> Indexes => _indexes;
        public Dictionary<string, FixedSizeSchemaIndexDef> FixedSizeIndexes => _fixedSizeIndexes;

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
            public Slice NameAsSlice;
            public string Name;

            public bool IsGlobal;                        

            public Slice GetSlice(ByteStringContext context, TableValueReader value)
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
                return Slice.External(context, ptr, (ushort)totalSize);
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
                                NameAsSlice
                            };

                    // It may -or not- have a string, so we have to serialize this conditionally, and do not
                    // know the size beforehand
                    var structure = new List<byte[]>();

                    bool hasName = Name != null;

                    structure.Add(BitConverter.GetBytes(hasName));

                    if (hasName)
                    {
                        structure.Add(Encoding.UTF8.GetBytes(Name));
                    }

                    // Insert additional structure into TableValueBuilder
                    var totalSize = structure.Select((bytes, i) => bytes.Length).Sum();
                    var packed = new byte[totalSize];
                    int position = 0;

                    fixed (byte* ptr = packed)
                    {
                        foreach (var member in structure)
                        {
                            member.CopyTo(packed, position);
                            serializer.Add(&ptr[position], member.Length);
                            position += member.Length;
                        }

                        byte[] serialized = new byte[serializer.Size];

                        fixed (byte* destination = serialized)
                        {
                            serializer.CopyTo(destination);
                        }

                        return serialized;

                    }
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
                indexDef.NameAsSlice = Slice.From(context, currentPtr, currentSize, ByteStringType.Immutable);

                currentPtr = input.Read(5, out currentSize);
                var hasName = Convert.ToBoolean(*currentPtr);
                if (hasName)
                {
                    currentPtr = input.Read(6, out currentSize);
                    indexDef.Name = Encoding.UTF8.GetString(currentPtr, currentSize);
                }

                return indexDef;
            }
        }

        public class FixedSizeSchemaIndexDef
        {
            public int StartIndex = -1;
            public Slice NameAsSlice;
            public string Name;

            public bool IsGlobal;

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
                        NameAsSlice,
                    };

                    // It may -or not- have a string, so we have to serialize this conditionally, and do not
                    // know the size beforehand
                    var structure = new List<byte[]>();

                    bool hasName = Name != null;

                    structure.Add(BitConverter.GetBytes(hasName));

                    if (hasName)
                    {
                        structure.Add(Encoding.UTF8.GetBytes(Name));
                    }

                    // Insert additional structure into TableValueBuilder
                    var totalSize = structure.Select((bytes, i) => bytes.Length).Sum();
                    var packed = new byte[totalSize];
                    int position = 0;

                    fixed (byte* ptr = packed)
                    {
                        foreach (var member in structure)
                        {
                            member.CopyTo(packed, position);
                            serializer.Add(&ptr[position], member.Length);
                            position += member.Length;
                        }

                        byte[] serialized = new byte[serializer.Size];

                        fixed (byte* destination = serialized)
                        {
                            serializer.CopyTo(destination);
                        }

                        return serialized;

                    }
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
                output.NameAsSlice = Slice.From(context, currentPtr, currentSize, ByteStringType.Immutable);

                currentPtr = input.Read(3, out currentSize);
                var hasName = Convert.ToBoolean(*currentPtr);

                if (hasName)
                {
                    currentPtr = input.Read(4, out currentSize);
                    output.Name = Encoding.UTF8.GetString(currentPtr, currentSize);
                }

                return output;
            }
        }        

        private SchemaIndexDef _pk;
        private readonly Dictionary<string, SchemaIndexDef> _indexes = new Dictionary<string, SchemaIndexDef>();
        private readonly Dictionary<string, FixedSizeSchemaIndexDef> _fixedSizeIndexes = new Dictionary<string, FixedSizeSchemaIndexDef>();

        public TableSchema DefineIndex(string name, SchemaIndexDef index)
        {
            index.NameAsSlice = Slice.From(StorageEnvironment.LabelsContext, name, ByteStringType.Immutable);
            _indexes[name] = index;

            return this;
        }


        public TableSchema DefineFixedSizeIndex(string name, FixedSizeSchemaIndexDef index)
        {
            index.NameAsSlice = Slice.From(StorageEnvironment.LabelsContext, name, ByteStringType.Immutable); ;
            _fixedSizeIndexes[name] = index;

            return this;
        }

        public TableSchema DefineKey(SchemaIndexDef pk)
        {
            _pk = pk;
            if (pk.IsGlobal && string.IsNullOrWhiteSpace(pk.Name))
                throw new ArgumentException("When specifying global PK the name must be specified", nameof(pk));

            if (string.IsNullOrWhiteSpace(_pk.Name))
                _pk.Name = "PK";
            _pk.NameAsSlice = Slice.From(StorageEnvironment.LabelsContext, _pk.Name, ByteStringType.Immutable);

            if (_pk.Count > 1)
                throw new InvalidOperationException("Primary key must be a single field");

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
            if (_pk == null && _indexes.Count == 0 && _fixedSizeIndexes.Count == 0)
                throw new InvalidOperationException($"Cannot create table {name} without a primary key and no indexes");

            var tableTree = tx.CreateTree(name, RootObjectType.Table);
            if (tableTree.State.NumberOfEntries > 0)
                return; // this was already created

            // Create raw data. This is where we will actually store the documents
            var rawDataActiveSection = ActiveRawDataSmallSection.Create(tx.LowLevelTransaction, name);

            Slice pageNumber = Slice.From(tx.Allocator, EndianBitConverter.Little.GetBytes(rawDataActiveSection.PageNumber), ByteStringType.Immutable);
            tableTree.Add(ActiveSection, pageNumber);
            
            var stats = (TableSchemaStats*)tableTree.DirectAdd(Stats, sizeof(TableSchemaStats));
            stats->NumberOfEntries = 0;

            if (_pk != null)
            {
                if (_pk.IsGlobal == false)
                {
                    var indexTree = Tree.Create(tx.LowLevelTransaction, tx);
                    var treeHeader = tableTree.DirectAdd(_pk.NameAsSlice, sizeof(TreeRootHeader));
                    indexTree.State.CopyTo((TreeRootHeader*)treeHeader);
                }
                else
                {
                    tx.CreateTree(_pk.Name);
                }
            }

            foreach (var indexDef in _indexes.Values)
            {
                if (indexDef.IsGlobal == false)
                {
                    var indexTree = Tree.Create(tx.LowLevelTransaction, tx);
                    var treeHeader = tableTree.DirectAdd(indexDef.NameAsSlice, sizeof(TreeRootHeader));
                    indexTree.State.CopyTo((TreeRootHeader*)treeHeader);
                }
                else
                {
                    tx.CreateTree(indexDef.Name);
                }
            }

            // Serialize the schema into the table's tree
            var serializer = SerializeSchema();
            var schemaRepresentation = tableTree.DirectAdd(Schemas, serializer.Length);

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
        ///  4. Key value pairs of the composite indexes
        ///  5. Number of fixed size indexes
        ///  6. Key value pairs of the fixed size indexes
        /// </summary>
        /// <returns></returns>
        internal byte[] SerializeSchema()
        {
            // Create list of serialized substructures
            var structure = new List<byte[]>();
            bool hasPrimaryKey = _pk != null;

            structure.Add(BitConverter.GetBytes(hasPrimaryKey));

            if (hasPrimaryKey)
                structure.Add(_pk.Serialize());

            structure.Add(BitConverter.GetBytes(_indexes.Count));

            foreach (var pair in _indexes)
            {
                structure.Add(Encoding.UTF8.GetBytes(pair.Key));
                structure.Add(pair.Value.Serialize());
            }

            structure.Add(BitConverter.GetBytes(_fixedSizeIndexes.Count));

            foreach (var pair in _fixedSizeIndexes)
            {
                structure.Add(Encoding.UTF8.GetBytes(pair.Key));
                structure.Add(pair.Value.Serialize());
            }

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
                var indexName = Encoding.UTF8.GetString(currentPtr, currentSize);

                currentPtr = input.Read(currentIndex++, out currentSize);
                var indexSchemaDef = SchemaIndexDef.ReadFrom(context, currentPtr, currentSize);
                schema.DefineIndex(indexName, indexSchemaDef);

                indexCount--;
            }

            // Read fixed size schema indexes
            currentPtr = input.Read(currentIndex++, out currentSize);
            indexCount = *(int*) currentPtr;

            while (indexCount > 0)
            {
                currentPtr = input.Read(currentIndex++, out currentSize);
                var indexName = Encoding.UTF8.GetString(currentPtr, currentSize);

                currentPtr = input.Read(currentIndex++, out currentSize);
                var fixedIndexSchemaDef = FixedSizeSchemaIndexDef.ReadFrom(context, currentPtr, currentSize);
                schema.DefineFixedSizeIndex(indexName, fixedIndexSchemaDef);

                indexCount--;
            }

            return schema;
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // We will release all the labels allocated for indexes.
                    foreach ( var item in _indexes )
                        item.Value.NameAsSlice.Release(StorageEnvironment.LabelsContext);

                    // We will release all the labels allocated for fixed size indexes.
                    foreach (var item in _fixedSizeIndexes)
                        item.Value.NameAsSlice.Release(StorageEnvironment.LabelsContext);
                }

                disposedValue = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
        }
        #endregion
    }
}