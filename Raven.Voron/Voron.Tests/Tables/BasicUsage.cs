// -----------------------------------------------------------------------
//  <copyright file="BasicUsage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.RawData;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;
using Voron.Util.Conversion;
using Xunit;

namespace Voron.Tests.Tables
{
    public class BasicUsage : StorageTest
    {
        [Fact]
        public void Test()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Test");
                tree.Add("foo", EndianBitConverter.Little.GetBytes(1L));
                var fixedSizeTree = new FixedSizeTree(tx.LowLevelTransaction, tree, "baz", 0);
                fixedSizeTree.Add(1);
                tree.MultiAdd("bar", new Slice(EndianBitConverter.Little.GetBytes(1L)));
            }
        }
        public enum DocumentsFields
        {
            Etag,
            Key,
            Data
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);

            var schema = new TableSchema<DocumentsFields>("docs")
                .DefineField<long>(DocumentsFields.Etag)
                .DefineField<string>(DocumentsFields.Key)
                .DefineField<byte[]>(DocumentsFields.Data)
                .DefineIndex("By/Etag", DocumentsFields.Etag)
                .DefineIndex("By/Key", DocumentsFields.Key);
        }
    }


    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct TableSchemaStats
    {
        [FieldOffset(0)]
        public long NumberOfEntries;
    }

    public unsafe class TableSchema<T>
    {
        public SchemaIndexDef Key => _pk;

        public Dictionary<string, SchemaIndexDef> Indexes => _indexes;

        public class SchemaIndexDef
        {
            public T[] IndexedFields;
            public int Size;
            public bool IsFixedSize;
            public bool MultiValue;
            public Slice Name;

            public bool CanUseFixedSizeTree =>
                IndexedFields.Length == 1 &&
                IsFixedSize &&
                Size == sizeof(long) &&
                MultiValue == false;
        }

        public string Name { get; }
        public StructureSchema<T> StrcutureSchema => _schema;


        private readonly StructureSchema<T> _schema = new StructureSchema<T>();
        private SchemaIndexDef _pk;
        private readonly Dictionary<string, SchemaIndexDef> _indexes = new Dictionary<string, SchemaIndexDef>();
        public static readonly Slice ActiveSectionSlice = "Active-Section";
        public static readonly Slice InactiveSectionSlice = "Inactive-Section";
        public static readonly Slice StatsSlice = "Stats";


        public TableSchema(string name)
        {
            Name = name;
        }

        public TableSchema<T> DefineField<TValue>(T field)
        {
            _schema.Add<TValue>(field);
            return this;
        }

        public TableSchema<T> DefineIndex(string name, params T[] fieldsToIndex)
        {
            return DefineIndex(name, false, fieldsToIndex);
        }

        public TableSchema<T> DefineIndex(string name, bool multipleValue, params T[] fieldsToIndex)
        {
            _indexes.Add(name, CreateSchemaIndexDef(name, multipleValue, fieldsToIndex));
            return this;
        }

        private SchemaIndexDef CreateSchemaIndexDef(string name, bool multipleValue, T[] fieldsToIndex)
        {
            var isFixedSize = true;
            var size = 0;

            foreach (var field in fieldsToIndex)
            {
                var structureField = _schema.Fields[field.GetHashCode()];
                var fixedSizeField = structureField as FixedSizeField;
                isFixedSize &= fixedSizeField != null;
                size = fixedSizeField?.Size ?? 0;
            }

            var schemaIndexDef = new SchemaIndexDef
            {
                IndexedFields = fieldsToIndex,
                IsFixedSize = isFixedSize,
                Size = size,
                MultiValue = multipleValue,
                Name = name
            };
            return schemaIndexDef;
        }

        public TableSchema<T> DefineKey(params T[] fieldsToIndex)
        {
            _pk = CreateSchemaIndexDef("PK", false, fieldsToIndex);
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
            var rawDataActiveSection = RawDataSmallSection.Create(tx.LowLevelTransaction);
            tableTree.Add(ActiveSectionSlice, EndianBitConverter.Little.GetBytes(rawDataActiveSection.PageNumber));
            var stats = (TableSchemaStats*)tableTree.DirectAdd(StatsSlice, sizeof(TableSchemaStats));
            stats->NumberOfEntries = 0;

            if (_pk.CanUseFixedSizeTree == false)
            {
                var indexTree = Tree.Create(tx.LowLevelTransaction, tx);
                var treeHeader = tableTree.DirectAdd(_pk.Name, sizeof(TreeRootHeader));
                indexTree.State.CopyTo((TreeRootHeader*)treeHeader);
            }

            foreach (var indexDef in _indexes.Values)
            {
                if (indexDef.CanUseFixedSizeTree)
                    continue;
                var indexTree = Tree.Create(tx.LowLevelTransaction, tx);
                var treeHeader = tableTree.DirectAdd(indexDef.Name, sizeof(TreeRootHeader));
                indexTree.State.CopyTo((TreeRootHeader*)treeHeader);
            }
        }
    }

    public unsafe class Table<T>
    {
        private readonly TableSchema<T> _schema;
        private readonly Transaction _tx;
        private readonly Tree _tableTree;
        private RawDataSmallSection _activeDataSmallSection;
        private FixedSizeTree _fstKey;
        private FixedSizeTree _inactiveSections;

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

        public RawDataSmallSection ActiveDataSmallSection
        {
            get
            {
                if (_activeDataSmallSection == null)
                {
                    var readResult = _tableTree.Read(TableSchema<T>.ActiveSectionSlice);
                    if (readResult == null)
                        throw new InvalidDataException($"Could not find active sections for {_schema.Name}");
                    long pageNumber = readResult.Reader.ReadLittleEndianInt64();
                    _activeDataSmallSection = new RawDataSmallSection(_tx.LowLevelTransaction, pageNumber);
                }
                return _activeDataSmallSection;
            }
        }

        public Table(TableSchema<T> schema, Transaction tx)
        {
            _schema = schema;
            _tx = tx;
            _tableTree = _tx.ReadTree(_schema.Name);
        }

        public void Insert(Structure<T> value)
        {
            var size = value.GetSize();
            long id;
            byte* pos;
            if (size < _activeDataSmallSection.MaxItemSize)
            {
                if (_activeDataSmallSection.TryAllocate(size, out id) == false)
                {
                    InactiveSections.Add(_activeDataSmallSection.PageNumber);
                    _activeDataSmallSection = RawDataSmallSection.Create(_tx.LowLevelTransaction);
                    _tableTree.Add(TableSchema<T>.ActiveSectionSlice,
                        EndianBitConverter.Little.GetBytes(_activeDataSmallSection.PageNumber));
                    _activeDataSmallSection.TryAllocate(size, out id);
                }
                if (_activeDataSmallSection.TryWriteDirect(id, size, out pos) == false)
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
                pos = page.Pointer + sizeof(PageHeader);
                value.Write(pos);
                id = page.PageNumber;
            }
            var isAsBytes = EndianBitConverter.Little.GetBytes(id);
            var reader = new StructureReader<T>(value, _schema.StrcutureSchema);
            if (_schema.Key.CanUseFixedSizeTree)
            {
                var key = reader.ReadLong(_schema.Key.IndexedFields[0]);
                FixedSizeKey.Add(key, isAsBytes);
            }
            else
            {
                throw new NotImplementedException();
            }
        }
    }
}