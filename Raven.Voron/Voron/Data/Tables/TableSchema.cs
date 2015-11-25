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
    public class TableSchema
    {
        public static readonly Slice ActiveSectionSlice = "Active-Section";
        public static readonly Slice InactiveSectionSlice = "Inactive-Section";
        public static readonly Slice ActiveCandidateSection = "Active-Candidate-Section";
        public static readonly Slice StatsSlice = "Stats";
    }

    public class TableIndexer<T>
    {
        public TableIndexer()
        {
            IsFixedSize = true;
        }

        private readonly List<Expression<Func<T, int>>> sizeCountingFunc = new List<Expression<Func<T, int>>>();
        private readonly List<Expression> writing = new List<Expression>();

        private readonly ParameterExpression Writer = Expression.Parameter(typeof(SliceWriter), "writer");
        private readonly ParameterExpression Value = Expression.Parameter(typeof(T), "value");

        private Expression WriteAction<W>(Expression<Func<T, W>> exp, MethodInfo method)
        {
            return Expression.Call(Writer, method, new[] { Expression.Invoke(exp, new[] { Value }) });
        }

        public TableIndexer<T> Add<W>(Expression<Func<T, W>> exp)
        {
            var type = typeof(W).GetBondDataType();            

            int expectedSize = 0;
            switch (type)
            {
                case BondDataType.BT_STRING:
                case BondDataType.BT_WSTRING:
                    {
                        IsFixedSize = false;

                        var pParam = Expression.Parameter(typeof(T), "pParam");

                        Expression<Func<string, int>> calculate = x => Encoding.UTF8.GetByteCount(x);
                        Expression<Func<T, int>> stringCount = Expression.Lambda<Func<T, int>>(
                                                Expression.Invoke(calculate, new[] {
                                                     Expression.Invoke(exp, new[] { pParam })
                                                }), pParam);
                        sizeCountingFunc.Add(stringCount);

                        writing.Add(WriteAction(exp, Util.Reflection.MethodInfoOf((SliceWriter w) => w.Write(default(string)))));
                        break;
                    }
                case BondDataType.BT_BOOL:
                    {
                        expectedSize = 1;
                        var action = WriteAction(exp, Util.Reflection.MethodInfoOf((SliceWriter w) => w.Write(default(bool))));
                        writing.Add(action);
                        break;
                    }
                case BondDataType.BT_INT8:
                case BondDataType.BT_UINT8:
                    {
                        expectedSize = 1;
                        var action = WriteAction(exp, Util.Reflection.MethodInfoOf((SliceWriter w) => w.Write(default(byte))));
                        writing.Add(action);
                        break;
                    }
                case BondDataType.BT_INT16:
                    {
                        expectedSize = 2;
                        ;

                        var action = WriteAction(exp, Util.Reflection.MethodInfoOf((SliceWriter w) => w.WriteBigEndian(default(short))));
                        writing.Add(action);
                        break;
                    }
                case BondDataType.BT_UINT16:
                    {
                        expectedSize = 2;
                        var action = WriteAction(exp, Util.Reflection.MethodInfoOf((SliceWriter w) => w.WriteBigEndian(default(ushort))));
                        writing.Add(action);
                        break;
                    }
                case BondDataType.BT_INT32:
                    {
                        expectedSize = 4;
                        var action = WriteAction(exp, Util.Reflection.MethodInfoOf((SliceWriter w) => w.WriteBigEndian(default(int))));
                        writing.Add(action);
                        break;
                    }
                case BondDataType.BT_UINT32:
                    {
                        expectedSize = 4;
                        var action = WriteAction(exp, Util.Reflection.MethodInfoOf((SliceWriter w) => w.WriteBigEndian(default(uint))));
                        writing.Add(action);
                        break;
                    }
                case BondDataType.BT_INT64:
                    {
                        expectedSize = 8;
                        var action = WriteAction(exp, Util.Reflection.MethodInfoOf((SliceWriter w) => w.WriteBigEndian(default(long))));
                        writing.Add(action);
                        break;
                    }
                case BondDataType.BT_UINT64:
                    {
                        expectedSize = 8;
                        var action = WriteAction(exp, Util.Reflection.MethodInfoOf((SliceWriter w) => w.WriteBigEndian(default(long))));
                        writing.Add(action);
                        break;
                    }

                default:
                    throw new ArgumentException($"The type of {typeof(W).FullName} is not supported by the {nameof(TableIndexer<T>)}. Only basic types are supported.");
            }
        
            ExpectedSize += expectedSize;                
            Fields++;

            return this;
        }

        static readonly ConstructorInfo sliceWriterCtor = typeof(SliceWriter).GetConstructor(new[] { typeof(int) });
        static readonly MethodInfo createSliceMethod = Util.Reflection.MethodInfoOf( (SliceWriter w) => w.CreateSlice());

        internal Expression<Func<T, Slice>> Generate()
        {           
            Expression addition = Expression.Constant(ExpectedSize);
            foreach (var countFunc in sizeCountingFunc)
                addition = Expression.Add(addition, Expression.Invoke(countFunc, new[] { Value }));

            var vSize = Expression.Variable(typeof(int), "vSize");
            var body = Expression.Block(
                            new[] { vSize, Writer },
                            Expression.Assign(vSize, addition),  // vSize = this.ExpectedSize + countFunc[0](T) + ... + countFunc[n](T)                
                            Expression.Assign(Writer, Expression.New(sliceWriterCtor, new[] { vSize })),
                            Expression.Block(writing),
                            Expression.Call(Writer, createSliceMethod)
                        );

            Expression<Func<T, Slice>> result = Expression.Lambda<Func<T,Slice>>(body, Value);

            return result;                                    
        }

        public int Fields { get; private set; }

        public bool IsFixedSize { get; private set; }

        /// <summary>
        /// This is the expected size of the fixed size part of the index key.
        /// </summary>
        public int ExpectedSize { get; private set; }
    }

    public unsafe class TableSchema<T>
    {
        public SchemaIndexDef Key => _pk;

        public Dictionary<string, SchemaIndexDef> Indexes => _indexes;

        public class SchemaIndexDef
        {
            public Func<T, Slice> CreateKey;

            public int Size;
            public bool IsFixedSize;
            public bool MultiValue;
            public Slice Name;

            public bool CanUseFixedSizeTree =>
                IsFixedSize &&
                Size <= sizeof(long) &&
                MultiValue == false;
        }

        public string Name { get; }

        private SchemaIndexDef _pk;
        private readonly Dictionary<string, SchemaIndexDef> _indexes = new Dictionary<string, SchemaIndexDef>();

        public TableSchema(string name)
        {
            Name = name;
        }

        public TableSchema<T> DefineIndex(string name, Action<TableIndexer<T>> indexGenerator, bool multipleValue = false)
        {
            var indexer = new TableIndexer<T>();
            indexGenerator(indexer);

            // Construct an Expression<Func, byte[]> to build the key from object Keys
            var schemaIndexDef = new SchemaIndexDef
            {
                CreateKey = indexer.Generate().Compile(),
                IsFixedSize = indexer.IsFixedSize,
                Size = indexer.ExpectedSize,
                MultiValue = multipleValue,
                Name = name
            };

            this._indexes[name] = schemaIndexDef;

            return this;
        }

        public TableSchema<T> DefineKey(Action<TableIndexer<T>> indexGenerator)
        {
            // Construct an Expression<Func, byte[]> to build the key from object Keys
            var indexer = new TableIndexer<T>();
            indexGenerator(indexer);

            _pk = new SchemaIndexDef
            {
                CreateKey = indexer.Generate().Compile(),
                IsFixedSize = indexer.IsFixedSize,
                Size = indexer.ExpectedSize,
                MultiValue = false,
                Name = "PK"
            };

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