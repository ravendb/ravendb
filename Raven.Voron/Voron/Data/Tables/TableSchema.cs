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

        //public StructureSchema<T> StructureSchema => _schema;


        //private readonly StructureSchema<T> _schema = new StructureSchema<T>();
        private SchemaIndexDef _pk;
        private readonly Dictionary<string, SchemaIndexDef> _indexes = new Dictionary<string, SchemaIndexDef>();


        public TableSchema(string name)
        {
            Name = name;
        }

        //public TableSchema<T> DefineField<TValue>(T field)
        //{
        //    _schema.Add<TValue>(field);
        //    return this;
        //}

        //public TableSchema<T> DefineIndex(string name, params T[] fieldsToIndex)
        //{
        //    return DefineIndex(name, false, fieldsToIndex);
        //}

        //public TableSchema<T> DefineIndex(string name, bool multipleValue, params T[] fieldsToIndex)
        //{
        //    _indexes.Add(name, CreateSchemaIndexDef(name, multipleValue, fieldsToIndex));
        //    return this;
        //}

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

        private int GetTypeSize<W1>()
        {
            throw new NotImplementedException();
        }

        public TableSchema<T> DefineIndex<W1>(string name, Expression<Func<T, W1>> first, bool multipleValue = false)
        {
            throw new NotImplementedException();

            //var schemaIndexDef = new SchemaIndexDef
            //{
            //    IndexedFields = fieldsToIndex,
            //    IsFixedSize = IsFixedSizeType<W1>(),
            //    Size = GetTypeSize<W1>(),
            //    MultiValue = multipleValue,
            //    Name = name
            //};
            //return schemaIndexDef;
        }

        public TableSchema<T> DefineIndex<W1, W2>(string name, Func<T, W1> first, Func<T, W2> second, bool multipleValue = false)
        {
            throw new NotImplementedException();
        }


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


        public TableSchema<T> DefineKey<W1>(Func<T, W1> first)
        {
            //_pk = CreateSchemaIndexDef("PK", false, fieldsToIndex);
            throw new NotImplementedException();
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