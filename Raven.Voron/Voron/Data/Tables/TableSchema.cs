using Bond;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Voron.Data.BTrees;
using Voron.Data.RawData;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Util.Conversion;

namespace Voron.Data.Tables
{
    public class TableSchema
    {
        public static readonly Slice ActiveSectionSlice = "Active-Section";
        public static readonly Slice InactiveSectionSlice = "Inactive-Section";
        public static readonly Slice ActiveCandidateSection = "Active-Candidate-Section";
        public static readonly Slice StatsSlice = "Stats";
    }

    public unsafe class TableSchema<T>
    {
        public SchemaIndexDef Key => _pk;

        public Dictionary<string, SchemaIndexDef> Indexes => _indexes;

        public class SchemaIndexDef
        {
            public int FieldsCount;
            public Func<T, Slice> CreateKey;

            public int Size;
            public bool IsFixedSize;
            public bool MultiValue;
            public Slice Name;

            public bool CanUseFixedSizeTree =>
                FieldsCount == 1 &&
                IsFixedSize &&
                Size == sizeof(long) &&
                MultiValue == false;
        }

        public string Name { get; }

        private SchemaIndexDef _pk;
        private readonly Dictionary<string, SchemaIndexDef> _indexes = new Dictionary<string, SchemaIndexDef>();

        public TableSchema(string name)
        {
            Name = name;
        }

        private bool IsFixedSizeType<X>()
        {
            var fields = Reflection.GetSchemaFields(typeof(X));
            foreach (var field in fields)
            {
                switch (Reflection.GetBondDataType(field.MemberType))
                {
                    case BondDataType.BT_LIST:
                    case BondDataType.BT_MAP:
                    case BondDataType.BT_SET:
                    case BondDataType.BT_STOP:
                    case BondDataType.BT_STOP_BASE:
                    case BondDataType.BT_STRING:
                    case BondDataType.BT_WSTRING:
                    case BondDataType.BT_STRUCT:
                    case BondDataType.BT_UNAVAILABLE:
                        {
                            return false;
                        }
                }
            }

            return true;
        }

        private int GetExpectedTypeSize<X>()
        {
            switch (typeof(X).GetBondDataType())
            {
                case BondDataType.BT_LIST:
                case BondDataType.BT_MAP:
                case BondDataType.BT_SET:
                case BondDataType.BT_STOP:
                case BondDataType.BT_STOP_BASE:
                case BondDataType.BT_STRING:
                case BondDataType.BT_WSTRING:
                case BondDataType.BT_STRUCT:
                case BondDataType.BT_UNAVAILABLE:
                    return -1;
                case BondDataType.BT_BOOL:
                case BondDataType.BT_INT8:
                case BondDataType.BT_UINT8:
                    return 1;
                case BondDataType.BT_INT16:
                case BondDataType.BT_UINT16:
                    return 2;
                case BondDataType.BT_FLOAT:
                case BondDataType.BT_INT32:
                case BondDataType.BT_UINT32:
                    return 4;                    
                case BondDataType.BT_DOUBLE:
                case BondDataType.BT_INT64:
                case BondDataType.BT_UINT64:
                    return 8;
            }

            return -1;
        }

        public TableSchema<T> DefineIndex<W1>(string name, Func<T, Slice> first, bool multipleValue = false)
        {
            // Construct an Expression<Func, byte[]> to build the key from object Keys

            var schemaIndexDef = new SchemaIndexDef
            {
                CreateKey = first,
                FieldsCount = 1,
                IsFixedSize = IsFixedSizeType<W1>(),
                Size = GetExpectedTypeSize<W1>(),
                MultiValue = multipleValue,
                Name = name
            };

            this._indexes[name] = schemaIndexDef;

            return this;
        }

        //public TableSchema<T> DefineIndex<W1, W2>(string name, Expression<Func<T, W1>> first, Expression<Func<T, W2>> second, bool multipleValue = false)
        //{
        //    int size = -1;
        //    int sizeW1 = GetExpectedTypeSize<W1>();
        //    int sizeW2 = GetExpectedTypeSize<W1>();
        //    if (sizeW1 != -1 && sizeW2 != -1)
        //        size = sizeW1 + sizeW2;

        //    // Construct an Expression<Func, byte[]> to build the key from object Keys

        //    var schemaIndexDef = new SchemaIndexDef
        //    {
        //        IndexedFieldsTypes = new[] { typeof(W1) },
        //        IsFixedSize = false,
        //        Size = size,
        //        MultiValue = multipleValue,
        //        Name = name
        //    };

        //    this._indexes[name] = schemaIndexDef;

        //    return this;
        //}


        //private SchemaIndexDef CreateSchemaIndexDef(string name, bool multipleValue, T[] fieldsToIndex)
        //{
        //    var isFixedSize = true;
        //    var size = 0;

        //    foreach (var field in fieldsToIndex)
        //    {
        //        var structureField = _schema.Fields[field.GetHashCode()];
        //        var fixedSizeField = structureField as FixedSizeField;
        //        isFixedSize &= fixedSizeField != null;
        //        size = fixedSizeField?.Size ?? 0;
        //    }

        //    var schemaIndexDef = new SchemaIndexDef
        //    {
        //        IndexedFields = fieldsToIndex,
        //        IsFixedSize = isFixedSize,
        //        Size = size,
        //        MultiValue = multipleValue,
        //        Name = name
        //    };
        //    return schemaIndexDef;
        //}


        //public TableSchema<T> DefineKey(params T[] fieldsToIndex)
        //{
        //    _pk = CreateSchemaIndexDef("PK", false, fieldsToIndex);
        //    return this;
        //}


        public TableSchema<T> DefineKey<W1>(Func<T, Slice> first)
        {
            // Construct an Expression<Func, byte[]> to build the key from object Keys

            _pk = new SchemaIndexDef
            {
                CreateKey = first,
                FieldsCount = 1,
                IsFixedSize = IsFixedSizeType<W1>(),
                Size = GetExpectedTypeSize<W1>(),
                MultiValue = false,
                Name = "PK"
            };

            return this;
        }

        public TableSchema<T> DefineKey<W1, W2>(Func<T, W1> first, Func<T, W2> second)
        {
            //_pk = CreateSchemaIndexDef("PK", false, fieldsToIndex);
            throw new NotImplementedException();
            return this;
        }

        public TableSchema<T> DefineKey<W1, W2, W3>(Func<T, W1> first, Func<T, W2> second, Func<T, W3> third)
        {
            //_pk = CreateSchemaIndexDef("PK", false, fieldsToIndex);
            throw new NotImplementedException();
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