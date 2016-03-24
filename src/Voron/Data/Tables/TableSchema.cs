using System;
using System.Collections.Generic;
using Voron.Data.BTrees;
using Voron.Data.RawData;
using Voron.Impl;
using Voron.Util.Conversion;

namespace Voron.Data.Tables
{
    public unsafe class TableSchema
    {
        public static readonly Slice ActiveSectionSlice = "Active-Section";
        public static readonly Slice InactiveSectionSlice = "Inactive-Section";
        public static readonly Slice ActiveCandidateSection = "Active-Candidate-Section";
        public static readonly Slice StatsSlice = "Stats";

        public SchemaIndexDef Key => _pk;

        public Dictionary<string, SchemaIndexDef> Indexes => _indexes;
        public Dictionary<string, FixedSizeSchemaIndexDef> FixedSizeIndexes => _fixedSizeIndexes;

        public class SchemaIndexDef
        {
            /// <summary>
            /// Here we take advantage on the fact that the values are laid out in memory sequentially
            /// we can point to a certain item index, and use one or more fields in the key directly, 
            /// without any coying
            /// </summary>
            public int StartIndex = -1;
            public int Count = -1;
            public Slice NameAsSlice;
            public string Name;

            public bool IsGlobal;

            public Slice GetSlice(TableValueReader value)
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
                return new Slice(ptr, (ushort)totalSize);
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
        }

        private SchemaIndexDef _pk;
        private readonly Dictionary<string, SchemaIndexDef> _indexes = new Dictionary<string, SchemaIndexDef>();
        private readonly Dictionary<string, FixedSizeSchemaIndexDef> _fixedSizeIndexes = new Dictionary<string, FixedSizeSchemaIndexDef>();


        public TableSchema DefineIndex(string name, SchemaIndexDef index)
        {
            index.NameAsSlice = name;
            _indexes[name] = index;

            return this;
        }


        public TableSchema DefineFixedSizeIndex(string name, FixedSizeSchemaIndexDef index)
        {
            index.NameAsSlice = name;
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
            _pk.NameAsSlice = _pk.Name;

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
        /// 
        /// </summary>
        public void Create(Transaction tx, string name)
        {
            if (_pk == null && _indexes.Count == 0 && _fixedSizeIndexes.Count == 0)
                throw new InvalidOperationException($"Cannot create table {name} without a primary key and no indexes");

            var tableTree = tx.CreateTree(name);
            if (tableTree.State.NumberOfEntries > 0)
                return; // this was already created

            var rawDataActiveSection = ActiveRawDataSmallSection.Create(tx.LowLevelTransaction, name);
            tableTree.Add(ActiveSectionSlice, EndianBitConverter.Little.GetBytes(rawDataActiveSection.PageNumber));
            var stats = (TableSchemaStats*)tableTree.DirectAdd(StatsSlice, sizeof(TableSchemaStats));
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
        }
    }
}