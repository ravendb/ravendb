//-----------------------------------------------------------------------
// <copyright file="HelpersTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for the meta-data helpers.
    /// </summary>
    [TestClass]
    public class HelpersTests
    {
        /// <summary>
        /// The directory being used for the database and its files.
        /// </summary>
        private string directory;

        /// <summary>
        /// The path to the database being used by the test.
        /// </summary>
        private string database;

        /// <summary>
        /// The name of the table.
        /// </summary>
        private string table;

        /// <summary>
        /// The instance used by the test.
        /// </summary>
        private JET_INSTANCE instance;

        /// <summary>
        /// The session used by the test.
        /// </summary>
        private JET_SESID sesid;

        /// <summary>
        /// Identifies the database used by the test.
        /// </summary>
        private JET_DBID dbid;

        /// <summary>
        /// The tableid being used by the test.
        /// </summary>
        private JET_TABLEID tableid;

        /// <summary>
        /// A dictionary that maps column names to column ids.
        /// </summary>
        private IDictionary<string, JET_COLUMNID> columnidDict;

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// All DDL should be done in this method.
        /// </summary>
        [TestInitialize]
        [Description("Fixture setup for HelpersTests")]
        public void Setup()
        {
            this.directory = SetupHelper.CreateRandomDirectory();
            this.database = Path.Combine(this.directory, "database.edb");
            this.table = "table";
            this.instance = SetupHelper.CreateNewInstance(this.directory);

            // turn off logging so initialization is faster
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.Recovery, 0, "off");
            Api.JetInit(ref this.instance);
            Api.JetBeginSession(this.instance, out this.sesid, String.Empty, String.Empty);
            Api.JetCreateDatabase(this.sesid, this.database, String.Empty, out this.dbid, CreateDatabaseGrbit.None);
            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateTable(this.sesid, this.dbid, this.table, 0, 100, out this.tableid);

            JET_COLUMNDEF columndef = null;
            JET_COLUMNID columnid;
            
            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.Bit };
            Api.JetAddColumn(this.sesid, this.tableid, "Boolean", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.UnsignedByte };
            Api.JetAddColumn(this.sesid, this.tableid, "Byte", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.Short };
            Api.JetAddColumn(this.sesid, this.tableid, "Int16", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.Long };
            Api.JetAddColumn(this.sesid, this.tableid, "Int32", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.Currency };
            Api.JetAddColumn(this.sesid, this.tableid, "Int64", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.IEEESingle };
            Api.JetAddColumn(this.sesid, this.tableid, "Float", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.IEEEDouble };
            Api.JetAddColumn(this.sesid, this.tableid, "Double", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.DateTime };
            Api.JetAddColumn(this.sesid, this.tableid, "DateTime", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.LongBinary };
            Api.JetAddColumn(this.sesid, this.tableid, "Binary", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.LongText, cp = JET_CP.ASCII };
            Api.JetAddColumn(this.sesid, this.tableid, "ASCII", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.LongText, cp = JET_CP.Unicode };
            Api.JetAddColumn(this.sesid, this.tableid, "Unicode", columndef, null, 0, out columnid);

            if (EsentVersion.SupportsVistaFeatures)
            {
                // Starting with windows Vista esent provides support for these columns.) 
                columndef = new JET_COLUMNDEF() { coltyp = VistaColtyp.UnsignedShort };
                Api.JetAddColumn(this.sesid, this.tableid, "UInt16", columndef, null, 0, out columnid);

                columndef = new JET_COLUMNDEF() { coltyp = VistaColtyp.UnsignedLong };
                Api.JetAddColumn(this.sesid, this.tableid, "UInt32", columndef, null, 0, out columnid);

                columndef = new JET_COLUMNDEF() { coltyp = VistaColtyp.GUID };
                Api.JetAddColumn(this.sesid, this.tableid, "Guid", columndef, null, 0, out columnid);
            }
            else
            {
                // Older version of esent don't support these column types natively so we'll just use binary columns.
                columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.Binary, cbMax = 2 };
                Api.JetAddColumn(this.sesid, this.tableid, "UInt16", columndef, null, 0, out columnid);

                columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.Binary, cbMax = 4 };
                Api.JetAddColumn(this.sesid, this.tableid, "UInt32", columndef, null, 0, out columnid);

                columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.Binary, cbMax = 16 };
                Api.JetAddColumn(this.sesid, this.tableid, "Guid", columndef, null, 0, out columnid);
            }

            // Not natively supported by any version of Esent
            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.Binary, cbMax = 8 };
            Api.JetAddColumn(this.sesid, this.tableid, "UInt64", columndef, null, 0, out columnid);

            Api.JetCloseTable(this.sesid, this.tableid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out this.tableid);

            this.columnidDict = Api.GetColumnDictionary(this.sesid, this.tableid);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Fixture cleanup for HelpersTests")]
        public void Teardown()
        {
            Api.JetCloseTable(this.sesid, this.tableid);
            Api.JetEndSession(this.sesid, EndSessionGrbit.None);
            Api.JetTerm(this.instance);
            Cleanup.DeleteDirectoryWithRetry(this.directory);
        }

        /// <summary>
        /// Verify that the HelpersTests.Setup has setup the test fixture properly.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that the HelpersTests.Setup has setup the test fixture properly.")]
        public void VerifyFixtureSetup()
        {
            Assert.IsNotNull(this.table);
            Assert.AreNotEqual(JET_INSTANCE.Nil, this.instance);
            Assert.AreNotEqual(JET_SESID.Nil, this.sesid);
            Assert.AreNotEqual(JET_DBID.Nil, this.dbid);
            Assert.AreNotEqual(JET_TABLEID.Nil, this.tableid);
            Assert.IsNotNull(this.columnidDict);

            Assert.IsTrue(this.columnidDict.ContainsKey("boolean"));
            Assert.IsTrue(this.columnidDict.ContainsKey("byte"));
            Assert.IsTrue(this.columnidDict.ContainsKey("int16"));
            Assert.IsTrue(this.columnidDict.ContainsKey("int32"));
            Assert.IsTrue(this.columnidDict.ContainsKey("int64"));
            Assert.IsTrue(this.columnidDict.ContainsKey("float"));
            Assert.IsTrue(this.columnidDict.ContainsKey("double"));
            Assert.IsTrue(this.columnidDict.ContainsKey("binary"));
            Assert.IsTrue(this.columnidDict.ContainsKey("ascii"));
            Assert.IsTrue(this.columnidDict.ContainsKey("unicode"));
            Assert.IsTrue(this.columnidDict.ContainsKey("guid"));
            Assert.IsTrue(this.columnidDict.ContainsKey("datetime"));
            Assert.IsTrue(this.columnidDict.ContainsKey("uint16"));
            Assert.IsTrue(this.columnidDict.ContainsKey("uint32"));
            Assert.IsTrue(this.columnidDict.ContainsKey("uint64"));

            Assert.IsFalse(this.columnidDict.ContainsKey("nosuchcolumn"));
        }

        #endregion Setup/Teardown

        #region MetaData helpers tests

        /// <summary>
        /// Test the helper method that gets table names.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the helper method that gets table names.")]
        public void GetTableNames()
        {
            string actual = Api.GetTableNames(this.sesid, this.dbid).Single();
            Assert.AreEqual(this.table, actual);
        }

        /// <summary>
        /// Search the column information structures with Linq.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Search the column information structures with Linq.")]
        public void SearchColumnInfosWithLinq()
        {
            IEnumerable<string> columnnames = from c in Api.GetTableColumns(this.sesid, this.tableid)
                             where c.Coltyp == JET_coltyp.Long
                             select c.Name;
            Assert.AreEqual("Int32", columnnames.Single());
        }

        /// <summary>
        /// Iterate through the column information structures.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Iterate through the column information structures.")]
        public void GetTableColumnsFromTableid()
        {
            foreach (ColumnInfo col in Api.GetTableColumns(this.sesid, this.tableid))
            {
                Assert.AreEqual(this.columnidDict[col.Name], col.Columnid);
            }
        }

        /// <summary>
        /// Use GetTableColumnid to get a columnid.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Use GetTableColumnid to get a columnid.")]
        public void GetTableColumnid()
        {
            foreach (string column in this.columnidDict.Keys)
            {
                Assert.AreEqual(this.columnidDict[column], Api.GetTableColumnid(this.sesid, this.tableid, column));
            }
        }

        /// <summary>
        /// Iterate through the column information structures, using
        /// the dbid and tablename to specify the table.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Iterate through the column information structures, using the dbid and tablename to specify the table.")]
        public void GetTableColumnsByTableNameTest()
        {
            foreach (ColumnInfo col in Api.GetTableColumns(this.sesid, this.dbid, this.table))
            {
                Assert.AreEqual(this.columnidDict[col.Name], col.Columnid);
            }
        }

        /// <summary>
        /// Get index information when there are no indexes on the table.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Get index information when there are no indexes on the table.")]
        public void GetIndexInformationNoIndexes()
        {
            IEnumerable<IndexInfo> indexes = Api.GetTableIndexes(this.sesid, this.tableid);
            Assert.AreEqual(0, indexes.Count());
        }

        /// <summary>
        /// Get index information for one index.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Get index information for one index.")]
        public void GetIndexInformationOneIndex()
        {
            string indexname = "myindex";
            string indexdef = "+ascii\0\0";
            CreateIndexGrbit grbit = CreateIndexGrbit.IndexUnique;

            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateIndex(this.sesid, this.tableid, indexname, grbit, indexdef, indexdef.Length, 100);
            IEnumerable<IndexInfo> indexes = Api.GetTableIndexes(this.sesid, this.tableid);

            // There should be only one index
            IndexInfo info = indexes.Single();
            Assert.AreEqual(indexname, info.Name);
            Assert.AreEqual(grbit, info.Grbit);

            Assert.AreEqual(1, info.IndexSegments.Length);
            Assert.AreEqual("ascii", info.IndexSegments[0].ColumnName, true);
            Assert.IsTrue(info.IndexSegments[0].IsAscending);
            Assert.AreEqual(JET_coltyp.LongText, info.IndexSegments[0].Coltyp);
            Assert.IsTrue(info.IndexSegments[0].IsASCII);

            Api.JetRollback(this.sesid, RollbackTransactionGrbit.None);
        }

        /// <summary>
        /// Get index information for one index, where the index has multiple segments.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Get index information for one index, where the index has multiple segments.")]
        public void GetIndexInformationOneIndexMultipleSegments()
        {
            string indexname = "multisegmentindex";
            string indexdef = "+ascii\0-boolean\0\0";
            CreateIndexGrbit grbit = CreateIndexGrbit.IndexUnique;

            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateIndex(this.sesid, this.tableid, indexname, grbit, indexdef, indexdef.Length, 100);
            IEnumerable<IndexInfo> indexes = Api.GetTableIndexes(this.sesid, this.tableid);

            // There should be only one index
            IndexInfo info = indexes.Single();
            Assert.AreEqual(indexname, info.Name);
            Assert.AreEqual(grbit, info.Grbit);

            Assert.AreEqual(2, info.IndexSegments.Length);
            Assert.AreEqual("ascii", info.IndexSegments[0].ColumnName, true);
            Assert.IsTrue(info.IndexSegments[0].IsAscending);
            Assert.AreEqual(JET_coltyp.LongText, info.IndexSegments[0].Coltyp);
            Assert.IsTrue(info.IndexSegments[0].IsASCII);

            Assert.AreEqual("boolean", info.IndexSegments[1].ColumnName, true);
            Assert.IsFalse(info.IndexSegments[1].IsAscending);
            Assert.AreEqual(JET_coltyp.Bit, info.IndexSegments[1].Coltyp);

            Api.JetRollback(this.sesid, RollbackTransactionGrbit.None);
        }

        /// <summary>
        /// Get index information for one index.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Get index information for one index.")]
        public void GetIndexInformationByTableNameOneIndex()
        {
            string indexname = "myindex";
            string indexdef = "+ascii\0\0";
            CreateIndexGrbit grbit = CreateIndexGrbit.IndexUnique;

            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateIndex(this.sesid, this.tableid, indexname, grbit, indexdef, indexdef.Length, 100);
            IEnumerable<IndexInfo> indexes = Api.GetTableIndexes(this.sesid, this.dbid, this.table);

            // There should be only one index
            IndexInfo info = indexes.Single();
            Assert.AreEqual(indexname, info.Name);
            Assert.AreEqual(grbit, info.Grbit);

            Assert.AreEqual(1, info.IndexSegments.Length);
            Assert.AreEqual("ascii", info.IndexSegments[0].ColumnName, true);
            Assert.IsTrue(info.IndexSegments[0].IsAscending);
            Assert.AreEqual(JET_coltyp.LongText, info.IndexSegments[0].Coltyp);
            Assert.IsTrue(info.IndexSegments[0].IsASCII);

            Api.JetRollback(this.sesid, RollbackTransactionGrbit.None);
        }

        /// <summary>
        /// Get index information for one index.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Get index information for one index.")]
        public void GetIndexInformationOneIndexWithCompareOptions()
        {
            const string Indexname = "myindex";
            const string Indexdef = "-unicode\0\0";

            var pidxUnicode = new JET_UNICODEINDEX
            {
                lcid = CultureInfo.CurrentCulture.LCID,
                dwMapFlags = Conversions.LCMapFlagsFromCompareOptions(CompareOptions.IgnoreSymbols | CompareOptions.IgnoreCase),
            };

            var indexcreate = new JET_INDEXCREATE
            {
                szIndexName = Indexname,
                szKey = Indexdef,
                cbKey = Indexdef.Length,
                grbit = CreateIndexGrbit.IndexDisallowNull,
                pidxUnicode = pidxUnicode,
            };

            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateIndex2(this.sesid, this.tableid, new[] { indexcreate }, 1);
            IEnumerable<IndexInfo> indexes = Api.GetTableIndexes(this.sesid, this.tableid);

            // There should be only one index
            IndexInfo info = indexes.Single();
            Assert.AreEqual(Indexname, info.Name);
            Assert.AreEqual(CreateIndexGrbit.IndexDisallowNull, info.Grbit);

            Assert.AreEqual(1, info.IndexSegments.Length);
            Assert.AreEqual("unicode", info.IndexSegments[0].ColumnName, true);
            Assert.IsFalse(info.IndexSegments[0].IsAscending);
            Assert.AreEqual(JET_coltyp.LongText, info.IndexSegments[0].Coltyp);
            Assert.IsFalse(info.IndexSegments[0].IsASCII);
            Assert.AreEqual(CompareOptions.IgnoreSymbols | CompareOptions.IgnoreCase, info.CompareOptions);

            Api.JetRollback(this.sesid, RollbackTransactionGrbit.None);
        }

        #endregion MetaData helpers tests
    }
}
