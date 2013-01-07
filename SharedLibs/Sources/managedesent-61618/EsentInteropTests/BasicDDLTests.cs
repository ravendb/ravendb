//-----------------------------------------------------------------------
// <copyright file="BasicDDLTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Basic DDL tests
    /// </summary>
    [TestClass]
    public class BasicDdlTests
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
        /// Columnid of the column in the table.
        /// </summary>
        private JET_COLUMNID testColumnid;

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// All DDL should be done in this method.
        /// </summary>
        [TestInitialize]
        [Description("Setup for BasicDDLTests")]
        public void Setup()
        {
            this.directory = SetupHelper.CreateRandomDirectory();
            this.database = Path.Combine(this.directory, "database.edb");
            this.table = "table";
            this.instance = SetupHelper.CreateNewInstance(this.directory);

            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.Recovery, 0, "off");
            Api.JetInit(ref this.instance);
            Api.JetBeginSession(this.instance, out this.sesid, String.Empty, String.Empty);
            Api.JetCreateDatabase(this.sesid, this.database, String.Empty, out this.dbid, CreateDatabaseGrbit.None);
            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateTable(this.sesid, this.dbid, this.table, 0, 100, out this.tableid);

            var columndef = new JET_COLUMNDEF()
            {
                cp = JET_CP.Unicode,
                coltyp = JET_coltyp.LongText,
            };
            Api.JetAddColumn(this.sesid, this.tableid, "TestColumn", columndef, null, 0, out this.testColumnid);

            Api.JetCloseTable(this.sesid, this.tableid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out this.tableid);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup for BasicDDLTests")]
        public void Teardown()
        {
            Api.JetCloseTable(this.sesid, this.tableid);
            Api.JetEndSession(this.sesid, EndSessionGrbit.None);
            Api.JetTerm(this.instance);
            Cleanup.DeleteDirectoryWithRetry(this.directory);
        }

        /// <summary>
        /// Verify that BasicDDLTests has setup the test fixture properly.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that BasicDDLTests has setup the test fixture properly")]
        public void VerifyFixtureSetup()
        {
            Assert.IsNotNull(this.table);
            Assert.AreNotEqual(JET_INSTANCE.Nil, this.instance);
            Assert.AreNotEqual(JET_SESID.Nil, this.sesid);
            Assert.AreNotEqual(JET_DBID.Nil, this.dbid);
            Assert.AreNotEqual(JET_TABLEID.Nil, this.tableid);
            Assert.AreNotEqual(JET_COLUMNID.Nil, this.testColumnid);

            JET_COLUMNDEF columndef;
            Api.JetGetTableColumnInfo(this.sesid, this.tableid, this.testColumnid, out columndef);
            Assert.AreEqual(JET_coltyp.LongText, columndef.coltyp);
        }

        #endregion Setup/Teardown

        #region DDL Tests

        /// <summary>
        /// Create one column of each type.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Create one column of each type")]
        public void CreateOneColumnOfEachType()
        {
            Api.JetBeginTransaction(this.sesid);
            foreach (JET_coltyp coltyp in Enum.GetValues(typeof(JET_coltyp)))
            {
                if (JET_coltyp.Nil != coltyp)
                {
                    var columndef = new JET_COLUMNDEF { coltyp = coltyp };
                    JET_COLUMNID columnid;
                    Api.JetAddColumn(this.sesid, this.tableid, coltyp.ToString(), columndef, null, 0, out columnid);
                    Assert.AreEqual(columnid, columndef.columnid);
                }
            }

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
        }

        /// <summary>
        /// Create a column with a default value.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Create a column with a default value")]
        public void CreateColumnWithDefaultValue()
        {
            int expected = Any.Int32;

            Api.JetBeginTransaction(this.sesid);
            var columndef = new JET_COLUMNDEF { coltyp = JET_coltyp.Long };
            JET_COLUMNID columnid;
            Api.JetAddColumn(this.sesid, this.tableid, "column_with_default", columndef, BitConverter.GetBytes(expected), 4, out columnid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Assert.AreEqual(expected, Api.RetrieveColumnAsInt32(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Add a column and retrieve its information using JetGetTableColumnInfo.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Add a column and retrieve its information using JetGetTableColumnInfo")]
        public void JetGetTableColumnInfo()
        {
            const string ColumnName = "column1";
            Api.JetBeginTransaction(this.sesid);
            var columndef = new JET_COLUMNDEF()
            {
                cbMax = 4096,
                cp = JET_CP.Unicode,
                coltyp = JET_coltyp.LongText,
                grbit = ColumndefGrbit.None,
            };

            JET_COLUMNID columnid;
            Api.JetAddColumn(this.sesid, this.tableid, ColumnName, columndef, null, 0, out columnid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            JET_COLUMNDEF retrievedColumndef;
            Api.JetGetTableColumnInfo(this.sesid, this.tableid, ColumnName, out retrievedColumndef);

            Assert.AreEqual(columndef.cbMax, retrievedColumndef.cbMax);
            Assert.AreEqual(columndef.cp, retrievedColumndef.cp);
            Assert.AreEqual(columndef.coltyp, retrievedColumndef.coltyp);
            Assert.AreEqual(columnid, retrievedColumndef.columnid);

            // The grbit isn't asserted as esent will add some options by default
        }

        /// <summary>
        /// Add a column and retrieve its information using JetGetTableColumnInfo.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Add a column and retrieve its information using JetGetTableColumnInfo")]
        public void JetGetTableColumnInfoByColumnid()
        {
            const string ColumnName = "column2";
            Api.JetBeginTransaction(this.sesid);
            var columndef = new JET_COLUMNDEF()
            {
                cbMax = 8192,
                cp = JET_CP.ASCII,
                coltyp = JET_coltyp.LongText,
                grbit = ColumndefGrbit.None,
            };

            JET_COLUMNID columnid;
            Api.JetAddColumn(this.sesid, this.tableid, ColumnName, columndef, null, 0, out columnid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            JET_COLUMNDEF retrievedColumndef;
            Api.JetGetTableColumnInfo(this.sesid, this.tableid, columnid, out retrievedColumndef);

            Assert.AreEqual(columndef.cbMax, retrievedColumndef.cbMax);
            Assert.AreEqual(columndef.cp, retrievedColumndef.cp);
            Assert.AreEqual(columndef.coltyp, retrievedColumndef.coltyp);
            Assert.AreEqual(columnid, retrievedColumndef.columnid);

            // The grbit isn't asserted as esent will add some options by default
        }

        /// <summary>
        /// Add a column and retrieve its information using JetGetColumnInfo.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Add a column and retrieve its information using JetGetColumnInfo")]
        public void JetGetColumnInfo()
        {
            const string ColumnName = "column3";
            Api.JetBeginTransaction(this.sesid);
            var columndef = new JET_COLUMNDEF()
            {
                cbMax = 200,
                cp = JET_CP.ASCII,
                coltyp = JET_coltyp.LongText,
                grbit = ColumndefGrbit.None,
            };

            JET_COLUMNID columnid;
            Api.JetAddColumn(this.sesid, this.tableid, ColumnName, columndef, null, 0, out columnid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            JET_COLUMNDEF retrievedColumndef;
            Api.JetGetColumnInfo(this.sesid, this.dbid, this.table, ColumnName, out retrievedColumndef);

            Assert.AreEqual(columndef.cbMax, retrievedColumndef.cbMax);
            Assert.AreEqual(columndef.cp, retrievedColumndef.cp);
            Assert.AreEqual(columndef.coltyp, retrievedColumndef.coltyp);
            Assert.AreEqual(columnid, retrievedColumndef.columnid);

            // The grbit isn't asserted as esent will add some options by default
        }

        /// <summary>
        /// Add a column and retrieve its information using GetColumnDictionary.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Add a column and retrieve its information using GetColumnDictionary")]
        public void GetColumnDictionary()
        {
            const string ColumnName = "column4";
            Api.JetBeginTransaction(this.sesid);
            var columndef = new JET_COLUMNDEF()
            {
                cbMax = 10000,
                cp = JET_CP.Unicode,
                coltyp = JET_coltyp.LongText,
                grbit = ColumndefGrbit.None,
            };

            JET_COLUMNID columnid;
            Api.JetAddColumn(this.sesid, this.tableid, ColumnName, columndef, null, 0, out columnid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            IDictionary<string, JET_COLUMNID> dict = Api.GetColumnDictionary(this.sesid, this.tableid);
            Assert.AreEqual(columnid, dict[ColumnName]);
        }

        /// <summary>
        /// Check that the dictionary returned by GetColumnDictionary is case-insensitive.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Check that the dictionary returned by GetColumnDictionary is case-insensitive")]
        public void VerifyGetColumnDictionaryReturnsCaseInsensitiveDictionary()
        {
            IDictionary<string, JET_COLUMNID> dict = Api.GetColumnDictionary(this.sesid, this.tableid);
            Assert.AreEqual(this.testColumnid, dict["tEsTcOLuMn"]);
        }

        /// <summary>
        /// Create an index.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Create an index")]
        public void JetCreateIndex()
        {
            const string IndexDescription = "+TestColumn\0";
            const string IndexName = "new_index";
            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateIndex(this.sesid, this.tableid, IndexName, CreateIndexGrbit.None, IndexDescription, IndexDescription.Length + 1, 100);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetSetCurrentIndex(this.sesid, this.tableid, IndexName);
        }

        /// <summary>
        /// Creates an index with JetCreateIndex2.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Creates an index with JetCreateIndex2")]
        public void JetCreateIndex2()
        {
            Api.JetBeginTransaction(this.sesid);

            const string IndexName = "another_index";
            const string IndexDescription = "-TestColumn\0\0";

            var indexcreate = new JET_INDEXCREATE
            {
                szIndexName = IndexName,
                szKey = IndexDescription,
                cbKey = IndexDescription.Length,
                grbit = CreateIndexGrbit.IndexIgnoreAnyNull,
                ulDensity = 100,
            };
            Api.JetCreateIndex2(this.sesid, this.tableid, new[] { indexcreate }, 1);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetSetCurrentIndex(this.sesid, this.tableid, IndexName);
        }

        /// <summary>
        /// Creates an index with JetCreateIndex2 and space hints.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Creates an index with JetCreateIndex2 and space hints.")]
        public void JetCreateIndex2SpaceHints()
        {
            Api.JetBeginTransaction(this.sesid);

            const string IndexName = "another_index";
            const string IndexDescription = "-TestColumn\0\0";

            var spacehintsIndex = new JET_SPACEHINTS()
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };

            var indexcreate = new JET_INDEXCREATE
            {
                szIndexName = IndexName,
                szKey = IndexDescription,
                cbKey = IndexDescription.Length,
                grbit = CreateIndexGrbit.IndexIgnoreAnyNull,
                pSpaceHints = spacehintsIndex,
            };
            Api.JetCreateIndex2(this.sesid, this.tableid, new[] { indexcreate }, 1);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetSetCurrentIndex(this.sesid, this.tableid, IndexName);
        }

        /// <summary>
        /// Creates two indexes using JetCreateIndex2.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Creates two indexes using JetCreateIndex2")]
        public void CreateTwoIndexes()
        {
            JET_TABLEID tableToIndex;

            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateTable(this.sesid, this.dbid, "tabletoindex", 1, 100, out tableToIndex);

            var columndef = new JET_COLUMNDEF()
            {
                cp = JET_CP.Unicode,
                coltyp = JET_coltyp.LongText,
            };
            Api.JetAddColumn(this.sesid, tableToIndex, "column", columndef, null, 0, out this.testColumnid);

            Api.JetCloseTable(this.sesid, tableToIndex);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetOpenTable(this.sesid, this.dbid, "tabletoindex", null, 0, OpenTableGrbit.DenyRead, out tableToIndex);
            const string Index1Name = "firstIndex";
            const string Index1Description = "-column\0\0";

            const string Index2Name = "secondIndex";
            const string Index2Description = "+column\0\0";

            var indexcreates = new[]
            {
                new JET_INDEXCREATE
                {
                    szIndexName = Index1Name,
                    szKey = Index1Description,
                    cbKey = Index1Description.Length,
                    grbit = CreateIndexGrbit.None,
                    ulDensity = 100,
                },
                new JET_INDEXCREATE
                {
                    szIndexName = Index2Name,
                    szKey = Index2Description,
                    cbKey = Index2Description.Length,
                    grbit = CreateIndexGrbit.None,
                    ulDensity = 100,
                },
            };
            Api.JetCreateIndex2(this.sesid, tableToIndex, indexcreates, indexcreates.Length);

            Api.JetSetCurrentIndex(this.sesid, tableToIndex, Index1Name);
            Api.JetSetCurrentIndex(this.sesid, tableToIndex, Index2Name);
            Api.JetSetCurrentIndex(this.sesid, tableToIndex, null);
            Api.JetCloseTable(this.sesid, tableToIndex);
        }

        /// <summary>
        /// Creates a table, two columns, and two indexes using JetCreateTableColumnIndex3.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Creates a table, two columns, and two indexes using JetCreateTableColumnIndex3")]
        public void JetCreateTableColumnIndex()
        {
            var columncreates = new JET_COLUMNCREATE[]
            {
                new JET_COLUMNCREATE()
                {
                    szColumnName = "col1_short",
                    coltyp = JET_coltyp.Short,
                    cbMax = 2,
                },
                new JET_COLUMNCREATE()
                {
                    szColumnName = "col2_longtext",
                    coltyp = JET_coltyp.LongText,
                    cp = JET_CP.Unicode,
                },
            };

            const string Index1Name = "firstIndex";
            const string Index1Description = "+col1_short\0-col2_longtext\0";

            const string Index2Name = "secondIndex";
            const string Index2Description = "+col2_longtext\0-col1_short\0";

            var indexcreates = new JET_INDEXCREATE[]
            {
                  new JET_INDEXCREATE
                {
                    szIndexName = Index1Name,
                    szKey = Index1Description,
                    cbKey = Index1Description.Length + 1,
                    grbit = CreateIndexGrbit.None,
                    ulDensity = 99,
                },
                new JET_INDEXCREATE
                {
                    szIndexName = Index2Name,
                    szKey = Index2Description,
                    cbKey = Index2Description.Length + 1,
                    grbit = CreateIndexGrbit.None,
                    ulDensity = 79,
                },
            };

            var tablecreate = new JET_TABLECREATE()
            {
                szTableName = "tableBigBang",
                ulPages = 23,
                ulDensity = 75,
                cColumns = columncreates.Length,
                rgcolumncreate = columncreates,
                rgindexcreate = indexcreates,
                cIndexes = indexcreates.Length,
                cbSeparateLV = 100,
                cbtyp = JET_cbtyp.Null,
                grbit = CreateTableColumnIndexGrbit.None,
            };

            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateTableColumnIndex3(this.sesid, this.dbid, tablecreate);

            var tableCreated = new JET_TABLEID()
            {
                Value = tablecreate.tableid.Value
            };

            Assert.AreNotEqual<JET_TABLEID>(JET_TABLEID.Nil, tableCreated);

            // 1 table, 2 columns, 2 indices = 5 objects.
            Assert.AreEqual<int>(tablecreate.cCreated, 5);

            Assert.AreNotEqual(tablecreate.rgcolumncreate[0].columnid, JET_COLUMNID.Nil);
            Assert.AreNotEqual(tablecreate.rgcolumncreate[1].columnid, JET_COLUMNID.Nil);

            Api.JetCloseTable(this.sesid, tableCreated);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
        }

        /// <summary>
        /// Creates a template table, two columns, and two indexes using JetCreateTableColumnIndex3.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Creates a template table, two columns, and two indexes using JetCreateTableColumnIndex3")]
        public void JetCreateTemplateTableColumnIndex()
        {
            var columncreates = new JET_COLUMNCREATE[]
            {
                new JET_COLUMNCREATE()
                {
                    szColumnName = "col1_short",
                    coltyp = JET_coltyp.Short,
                    cbMax = 2,
                },
                new JET_COLUMNCREATE()
                {
                    szColumnName = "col2_longtext",
                    coltyp = JET_coltyp.LongText,
                    cp = JET_CP.Unicode,
                },
            };

            const string Index1Name = "firstIndex";
            const string Index1Description = "+col1_short\0-col2_longtext\0";

            const string Index2Name = "secondIndex";
            const string Index2Description = "+col2_longtext\0-col1_short\0";

            var indexcreates = new JET_INDEXCREATE[]
            {
                  new JET_INDEXCREATE
                {
                    szIndexName = Index1Name,
                    szKey = Index1Description,
                    cbKey = Index1Description.Length + 1,
                    grbit = CreateIndexGrbit.None,
                    ulDensity = 99,
                },
                new JET_INDEXCREATE
                {
                    szIndexName = Index2Name,
                    szKey = Index2Description,
                    cbKey = Index2Description.Length + 1,
                    grbit = CreateIndexGrbit.None,
                    ulDensity = 79,
                },
            };

            var tablecreateTemplate = new JET_TABLECREATE()
            {
                szTableName = "tableOld",
                ulPages = 23,
                ulDensity = 75,
                cColumns = columncreates.Length,
                rgcolumncreate = columncreates,
                rgindexcreate = indexcreates,
                cIndexes = indexcreates.Length,
                cbSeparateLV = 100,
                cbtyp = JET_cbtyp.Null,
                grbit = CreateTableColumnIndexGrbit.TemplateTable,
            };

            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateTableColumnIndex3(this.sesid, this.dbid, tablecreateTemplate);

            var tableCreated = new JET_TABLEID()
            {
                Value = tablecreateTemplate.tableid.Value
            };

            Assert.AreNotEqual<JET_TABLEID>(JET_TABLEID.Nil, tableCreated);

            // 1 table, 2 columns, 2 indices = 5 objects.
            Assert.AreEqual<int>(tablecreateTemplate.cCreated, 5);

            Assert.AreNotEqual(tablecreateTemplate.rgcolumncreate[0].columnid, JET_COLUMNID.Nil);
            Assert.AreNotEqual(tablecreateTemplate.rgcolumncreate[1].columnid, JET_COLUMNID.Nil);

            var tablecreateChild = new JET_TABLECREATE()
            {
                szTableName = "tableNew",
                szTemplateTableName = "tableOld",
                ulPages = 23,
                ulDensity = 75,
                rgcolumncreate = null,
                cColumns = 0,
                rgindexcreate = null,
                cIndexes = 0,
                cbSeparateLV = 100,
                cbtyp = JET_cbtyp.Null,
                grbit = CreateTableColumnIndexGrbit.None,
            };

            Api.JetCreateTableColumnIndex3(this.sesid, this.dbid, tablecreateChild);

            var tableidChild = new JET_TABLEID()
            {
                Value = tablecreateChild.tableid.Value
            };

            Assert.AreNotEqual<JET_TABLEID>(JET_TABLEID.Nil, tableidChild);

            // 1 table = 1 object
            Assert.AreEqual<int>(tablecreateChild.cCreated, 1);

            Api.JetCloseTable(this.sesid, tableCreated);
            Api.JetCloseTable(this.sesid, tableidChild);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
        }

        /// <summary>
        /// Creates a table, two columns, and two indexes using JetCreateTableColumnIndex3 and Space Hints.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Creates a table, two columns, and two indexes using JetCreateTableColumnIndex3 and Space Hints.")]
        public void JetCreateTableColumnIndexSpaceHints()
        {
            var columncreates = new[]
            {
                new JET_COLUMNCREATE()
                {
                    szColumnName = "col1_short",
                    coltyp = JET_coltyp.Short,
                    cbMax = 2,
                    pvDefault = BitConverter.GetBytes((short)37),
                    cbDefault = 2,
                },
                new JET_COLUMNCREATE()
                {
                    szColumnName = "col2_longtext",
                    coltyp = JET_coltyp.LongText,
                    cp = JET_CP.Unicode,
                },
            };

            const string Index1Name = "firstIndex";
            const string Index1Description = "+col1_short\0-col2_longtext\0";

            const string Index2Name = "secondIndex";
            const string Index2Description = "+col2_longtext\0-col1_short\0";

            var spacehintsIndex = new JET_SPACEHINTS()
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };

            var spacehintsSeq = new JET_SPACEHINTS()
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };

            var spacehintsLv = new JET_SPACEHINTS()
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };

            var indexcreates = new JET_INDEXCREATE[]
            {
                new JET_INDEXCREATE
                {
                    szIndexName = Index1Name,
                    szKey = Index1Description,
                    cbKey = Index1Description.Length + 1,
                    grbit = CreateIndexGrbit.None,
                    ulDensity = 99,
                    pSpaceHints = spacehintsIndex,
                },
                new JET_INDEXCREATE
                {
                    szIndexName = Index2Name,
                    szKey = Index2Description,
                    cbKey = Index2Description.Length + 1,
                    grbit = CreateIndexGrbit.None,
                    ulDensity = 79,
                },
            };

            var tablecreate = new JET_TABLECREATE()
            {
                szTableName = "tableBigBang",
                ulPages = 23,
                ulDensity = 75,
                cColumns = columncreates.Length,
                rgcolumncreate = columncreates,
                rgindexcreate = indexcreates,
                cIndexes = indexcreates.Length,
                cbSeparateLV = 100,
                cbtyp = JET_cbtyp.Null,
                grbit = CreateTableColumnIndexGrbit.None,
                pSeqSpacehints = spacehintsSeq,
                pLVSpacehints = spacehintsLv,
            };

            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateTableColumnIndex3(this.sesid, this.dbid, tablecreate);

            var tableCreated = new JET_TABLEID()
            {
                Value = tablecreate.tableid.Value
            };

            Assert.AreNotEqual<JET_TABLEID>(JET_TABLEID.Nil, tableCreated);

            // 1 table, 2 columns, 2 indices = 5 objects.
            Assert.AreEqual<int>(tablecreate.cCreated, 5);

            Api.JetCloseTable(this.sesid, tableCreated);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
        }

        /// <summary>
        /// Verify that JetGetCurrentIndex returns the name of the index.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that JetGetCurrentIndex returns the name of the index")]
        public void VerifyJetGetCurrentIndexReturnsIndexName()
        {
            const string IndexDescription = "+TestColumn\0";
            const string IndexName = "myindexname";
            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateIndex(this.sesid, this.tableid, IndexName, CreateIndexGrbit.None, IndexDescription, IndexDescription.Length + 1, 100);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetSetCurrentIndex(this.sesid, this.tableid, IndexName);
            string actual;
            Api.JetGetCurrentIndex(this.sesid, this.tableid, out actual, SystemParameters.NameMost);
            Assert.AreEqual(IndexName, actual);
        }

        /// <summary>
        /// Delete an index and make sure we can't use it afterwards.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Delete an index and make sure we can't use it afterwards")]
        public void JetDeleteIndex()
        {
            const string IndexDescription = "+TestColumn\0";
            const string IndexName = "index_to_delete";
            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateIndex(this.sesid, this.tableid, IndexName, CreateIndexGrbit.None, IndexDescription, IndexDescription.Length + 1, 100);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetBeginTransaction(this.sesid);
            Api.JetDeleteIndex(this.sesid, this.tableid, IndexName);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            try
            {
                Api.JetSetCurrentIndex(this.sesid, this.tableid, IndexName);
                Assert.Fail("Index is still visible");
            }
            catch (EsentErrorException)
            {
            }
        }

        /// <summary>
        /// Delete a column and make sure we can't see it afterwards.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Delete a column and make sure we can't see it afterwards")]
        public void TestJetDeleteColumn()
        {
            const string ColumnName = "column_to_delete";
            Api.JetBeginTransaction(this.sesid);
            var columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.Long };
            JET_COLUMNID columnid;
            Api.JetAddColumn(this.sesid, this.tableid, ColumnName, columndef, null, 0, out columnid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetBeginTransaction(this.sesid);
            Api.JetDeleteColumn(this.sesid, this.tableid, ColumnName);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            try
            {
                // TODO: deal with versions of this API that return info on the deleted column
                Api.JetGetTableColumnInfo(this.sesid, this.tableid, ColumnName, out columndef);
                Assert.Fail("Column is still visible");
            }
            catch (EsentErrorException)
            {
            }
        }

        /// <summary>
        /// Delete a column with JetDeleteColumn2 and make sure we can't see it afterwards.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Delete a column with JetDeleteColumn2 and make sure we can't see it afterwards")]
        public void TestJetDeleteColumn2()
        {
            const string ColumnName = "column_to_delete";
            Api.JetBeginTransaction(this.sesid);
            var columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.Long };
            JET_COLUMNID columnid;
            Api.JetAddColumn(this.sesid, this.tableid, ColumnName, columndef, null, 0, out columnid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetBeginTransaction(this.sesid);
            Api.JetDeleteColumn2(this.sesid, this.tableid, ColumnName, DeleteColumnGrbit.None);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            try
            {
                // TODO: deal with versions of this API that return info on the deleted column
                Api.JetGetTableColumnInfo(this.sesid, this.tableid, ColumnName, out columndef);
                Assert.Fail("Column is still visible");
            }
            catch (EsentErrorException)
            {
            }
        }

        /// <summary>
        /// Delete a table and make sure we can't see it afterwards.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Delete a table and make sure we can't see it afterwards")]
        public void DeleteTable()
        {
            const string TableName = "table_to_delete";
            Api.JetBeginTransaction(this.sesid);
            JET_TABLEID newtable;
            Api.JetCreateTable(this.sesid, this.dbid, TableName, 16, 100, out newtable);
            Api.JetCloseTable(this.sesid, newtable);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetBeginTransaction(this.sesid);
            Api.JetDeleteTable(this.sesid, this.dbid, TableName);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            try
            {
                Api.JetOpenTable(this.sesid, this.dbid, TableName, null, 0, OpenTableGrbit.None, out newtable);
                Assert.Fail("Column is still visible");
            }
            catch (EsentErrorException)
            {
            }
        }

        /// <summary>
        /// Verify an error is thrown when key is truncated and truncation is disallowed.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify an error is thrown when key is truncated and truncation is disallowed")]
        public void TestDisallowTruncation()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                return;
            }

            const string IndexDescription = "+TestColumn\0";
            const string IndexName = "no_trunacation_index";
            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateIndex(this.sesid, this.tableid, IndexName, VistaGrbits.IndexDisallowTruncation, IndexDescription, IndexDescription.Length + 1, 100);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, this.testColumnid, new string('X', 4096), Encoding.Unicode);
            try
            {
                Api.JetUpdate(this.sesid, this.tableid);
                Assert.Fail("Expected a truncation error");
            }
            catch (EsentErrorException)
            {
                // Expected
            }

            Api.JetRollback(this.sesid, RollbackTransactionGrbit.None);
        }

        /// <summary>
        /// Test the version of JetGetTableInfo that returns a JET_OBJECTINFO.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the version of JetGetTableInfo that returns a JET_OBJECTINFO")]
        public void TestGetTableInfoObjectinfo()
        {
            JET_OBJECTINFO objectinfo;
            Api.JetGetTableInfo(this.sesid, this.tableid, out objectinfo, JET_TblInfo.Default);
            Assert.AreEqual(ObjectInfoFlags.None, objectinfo.flags);
        }

        /// <summary>
        /// Test the version of JetGetObjectInfo that returns a JET_OBJECTINFO.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the version of JetGetObjetInfo that returns a JET_OBJECTINFO")]
        public void TestGetObjectInfoObjectinfo()
        {
            JET_OBJECTINFO objectinfo;
            Api.JetGetObjectInfo(this.sesid, this.dbid, JET_objtyp.Table, this.table, out objectinfo);
            Assert.AreEqual(ObjectInfoFlags.None, objectinfo.flags);
        }

        /// <summary>
        /// Test the version of JetGetTableInfo that returns a string.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the version of JetGetTableInfo that returns a string")]
        public void TestGetTableInfoString()
        {
            string name;
            Api.JetGetTableInfo(this.sesid, this.tableid, out name, JET_TblInfo.Name);
            Assert.AreEqual(this.table, name);
        }

        /// <summary>
        /// Test the version of JetGetTableInfo that returns a JET_DBID.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the version of JetGetTableInfo that returns a JET_DBID")]
        public void TestGetTableInfoDbid()
        {
            if (!EsentVersion.SupportsWindows7Features)
            {
                Assert.Inconclusive("Pre Windows7 versions of ESENT incorrectly return JET_err.FeatureNotAvailable");
            }

            JET_DBID actualDbid;
            Api.JetGetTableInfo(this.sesid, this.tableid, out actualDbid, JET_TblInfo.Dbid);
            Assert.AreEqual(this.dbid, actualDbid);
        }

        /// <summary>
        /// Test the version of JetGetTableInfo that returns an array of integers.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the version of JetGetTableInfo that returns an array of integers")]
        public void TestGetTableInfoIntArray()
        {
            int[] result = new int[7];
            Api.JetGetTableInfo(this.sesid, this.tableid, result, JET_TblInfo.SpaceUsage);
            Assert.AreNotEqual(0, result[0]);
        }

        /// <summary>
        /// Test the version of JetGetTableInfo that returns an integer.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the version of JetGetTableInfo that returns an integer")]
        public void TestGetTableInfoInt()
        {
            int result;
            Api.JetGetTableInfo(this.sesid, this.tableid, out result, JET_TblInfo.SpaceOwned);
            Assert.AreNotEqual(0, result);
        }

        #endregion DDL Tests

        #region Helper Methods

        /// <summary>
        /// Update the cursor and goto the returned bookmark.
        /// </summary>
        private void UpdateAndGotoBookmark()
        {
            var bookmark = new byte[SystemParameters.BookmarkMost];
            int bookmarkSize;
            Api.JetUpdate(this.sesid, this.tableid, bookmark, bookmark.Length, out bookmarkSize);
            Api.JetGotoBookmark(this.sesid, this.tableid, bookmark, bookmarkSize);
        }

        #endregion HelperMethods
    }
}
