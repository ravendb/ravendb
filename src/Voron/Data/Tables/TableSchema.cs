using Bond;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Voron.Data.BTrees;
using Voron.Data.RawData;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Util;
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

        public class SchemaIndexDef
        {
            /// <summary>
            /// Here we take advantage on the fact that the values are laid out in memory sequentially
            /// we can point to a certain item index, and use one or more fields in the key directly, 
            /// without any coying
            /// </summary>
            public int StartIndex = -1;
            public int Count = -1;
            public int? Size ;
            public bool MultiValue;
            public Slice Name;

            public bool CanUseFixedSizeTree =>
                Size != null && 
                Size <= sizeof(long) &&
                MultiValue == false;

            public Slice GetSlice(TableValueReader value)
            {
                int totalSize;
                var ptr = value.Read(StartIndex, out totalSize);
                for (var i = 1; i < Count; i++)
                {
                    int size;
                    value.Read(i + StartIndex, out size);
                    totalSize += size;
                }
                return new Slice(ptr, (ushort)totalSize);
            }
        }

        public string Name { get; }

        private SchemaIndexDef _pk;
        private readonly Dictionary<string, SchemaIndexDef> _indexes = new Dictionary<string, SchemaIndexDef>();

        public TableSchema(string name)
        {
            Name = name;
        }

        public TableSchema DefineIndex(string name, SchemaIndexDef index)
        {
            index.Name = name;
            _indexes[name] = index;

            return this;
        }

        public TableSchema DefineKey(SchemaIndexDef pk)
        {

            _pk = pk;
            _pk.Name = "PK";
            _pk.MultiValue = false;

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
        public void Create(Transaction tx)
        {
            if (_pk == null)
                throw new InvalidOperationException($"Cannot create table {Name} without a primary key");

            var tableTree = tx.CreateTree(Name);
            var rawDataActiveSection = ActiveRawDataSmallSection.Create(tx.LowLevelTransaction);
            tableTree.Add(TableSchema.ActiveSectionSlice, EndianBitConverter.Little.GetBytes(rawDataActiveSection.PageNumber));
            var stats = (TableSchemaStats*)tableTree.DirectAdd(TableSchema.StatsSlice, sizeof(TableSchemaStats));
            stats->NumberOfEntries = 0;

            if (_pk.CanUseFixedSizeTree == false)
            {
                var indexTree = Tree.Create(tx.LowLevelTransaction, tx);
                var treeHeader = tableTree.DirectAdd(_pk.Name, sizeof(TreeRootHeader));
                indexTree.State.CopyTo((TreeRootHeader*)treeHeader);
            }

            foreach (var indexDef in _indexes.Values)
            {
                var indexTree = Tree.Create(tx.LowLevelTransaction, tx);
                var treeHeader = tableTree.DirectAdd(indexDef.Name, sizeof(TreeRootHeader));
                indexTree.State.CopyTo((TreeRootHeader*)treeHeader);
            }
        }
    }
}