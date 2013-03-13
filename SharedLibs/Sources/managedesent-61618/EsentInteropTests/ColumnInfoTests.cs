//-----------------------------------------------------------------------
// <copyright file="ColumnInfoTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.IO;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for JetGetIndexInfo and JetGetTableIndexInfo.
    /// </summary>
    [TestClass]
    public class ColumnInfoTests
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
        /// The tableid being used by the parent table in the test.
        /// </summary>
        private JET_TABLEID tableidParent;

        /// <summary>
        /// The tableid of the child table.
        /// </summary>
        private JET_TABLEID tableidChild;

        /// <summary>
        /// The DDL for the parent.
        /// </summary>
        private JET_TABLECREATE tablecreateTemplate;

        /// <summary>
        /// The DDL for the child.
        /// </summary>
        private JET_TABLECREATE tablecreateChild;

        /// <summary>
        /// The parent's columns.
        /// </summary>
        private JET_COLUMNCREATE[] columncreatesBase;

        /// <summary>
        /// The child's columns.
        /// </summary>
        private JET_COLUMNCREATE[] columncreatesChild;

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// All DDL should be done in this method.
        /// </summary>
        [TestInitialize]
        [Description("Setup for ColumnInfoTests")]
        public void Setup()
        {
            this.directory = SetupHelper.CreateRandomDirectory();
            this.database = Path.Combine(this.directory, "database.edb");
            this.instance = SetupHelper.CreateNewInstance(this.directory);

            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.Recovery, 0, "off");
            Api.JetInit(ref this.instance);
            Api.JetBeginSession(this.instance, out this.sesid, String.Empty, String.Empty);
            Api.JetCreateDatabase(this.sesid, this.database, String.Empty, out this.dbid, CreateDatabaseGrbit.None);
            Api.JetBeginTransaction(this.sesid);

            this.columncreatesBase = new JET_COLUMNCREATE[]
            {
                new JET_COLUMNCREATE()
                {
                    szColumnName = "col1_short",
                    coltyp = JET_coltyp.Short,
                    grbit = ColumndefGrbit.ColumnFixed,
                    cbMax = 2,
                },
                new JET_COLUMNCREATE()
                {
                    szColumnName = "col2_longtext",
                    coltyp = JET_coltyp.LongText,
                    cp = JET_CP.Unicode,
                },
            };

            this.columncreatesChild = new JET_COLUMNCREATE[]
            {
                new JET_COLUMNCREATE()
                {
                    szColumnName = "col1_short_child",
                    coltyp = JET_coltyp.Short,
                    cbMax = 2,
                },
                new JET_COLUMNCREATE()
                {
                    szColumnName = "col2_longtext_child",
                    coltyp = JET_coltyp.LongText,
                    grbit = ColumndefGrbit.ColumnTagged,
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

            this.tablecreateTemplate = new JET_TABLECREATE()
            {
                szTableName = "tableBase",
                ulPages = 23,
                ulDensity = 75,
                cColumns = this.columncreatesBase.Length,
                rgcolumncreate = this.columncreatesBase,
                rgindexcreate = indexcreates,
                cIndexes = indexcreates.Length,
                cbSeparateLV = 100,
                cbtyp = JET_cbtyp.Null,
                grbit = CreateTableColumnIndexGrbit.TemplateTable,
            };

            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateTableColumnIndex3(this.sesid, this.dbid, this.tablecreateTemplate);

            var columndef = new JET_COLUMNDEF()
            {
                cp = JET_CP.Unicode,
                coltyp = JET_coltyp.LongText,
            };

            var tableCreated = new JET_TABLEID()
            {
                Value = this.tablecreateTemplate.tableid.Value
            };

            Api.JetCloseTable(this.sesid, tableCreated);

            this.tablecreateChild = new JET_TABLECREATE()
            {
                szTableName = "tableChild",
                szTemplateTableName = "tableBase",
                ulPages = 23,
                ulDensity = 75,
                rgcolumncreate = this.columncreatesChild,
                cColumns = this.columncreatesChild.Length,
                rgindexcreate = null,
                cIndexes = 0,
                cbSeparateLV = 100,
                cbtyp = JET_cbtyp.Null,
                grbit = CreateTableColumnIndexGrbit.None,
            };

            Api.JetCreateTableColumnIndex3(this.sesid, this.dbid, this.tablecreateChild);

            this.tableidChild = new JET_TABLEID()
            {
                Value = this.tablecreateChild.tableid.Value
            };
            Api.JetCloseTable(this.sesid, this.tableidChild);

            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetOpenTable(this.sesid, this.dbid, this.tablecreateTemplate.szTableName, null, 0, OpenTableGrbit.None, out this.tableidParent);
            Api.JetOpenTable(this.sesid, this.dbid, this.tablecreateChild.szTableName, null, 0, OpenTableGrbit.None, out this.tableidChild);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup for ColumnInfoTest")]
        public void Teardown()
        {
            Api.JetCloseTable(this.sesid, this.tableidParent);
            Api.JetCloseTable(this.sesid, this.tableidChild);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetEndSession(this.sesid, EndSessionGrbit.None);
            Api.JetTerm(this.instance);
            Cleanup.DeleteDirectoryWithRetry(this.directory);
        }

        /// <summary>
        /// Verify that ColumnInfoTest has setup the test fixture properly.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that ColumnInfoTest has setup the test fixture properly")]
        public void VerifyFixtureSetup()
        {
            Assert.AreNotEqual(JET_INSTANCE.Nil, this.instance);
            Assert.AreNotEqual(JET_SESID.Nil, this.sesid);
            Assert.AreNotEqual(JET_DBID.Nil, this.dbid);
            Assert.AreNotEqual(JET_TABLEID.Nil, this.tableidParent);
            Assert.AreNotEqual(JET_TABLEID.Nil, this.tableidChild);

            // 1 table, 2 columns, 2 indices = 5 objects.
            Assert.AreEqual<int>(this.tablecreateTemplate.cCreated, 5);

            Assert.AreNotEqual(this.tablecreateTemplate.rgcolumncreate[0].columnid, JET_COLUMNID.Nil);
            Assert.AreNotEqual(this.tablecreateTemplate.rgcolumncreate[1].columnid, JET_COLUMNID.Nil);

            Assert.AreNotEqual(this.tablecreateChild.rgcolumncreate[0].columnid, JET_COLUMNID.Nil);
            Assert.AreNotEqual(this.tablecreateChild.rgcolumncreate[1].columnid, JET_COLUMNID.Nil);

            // 1 table + 2 columns = 3 objects
            Assert.AreEqual<int>(this.tablecreateChild.cCreated, 3);
        }

        #endregion Setup/Teardown

        #region JetGetColumnInfo

        /// <summary>
        /// Calls JetGetColumnInfo using a columnn name that's inherited.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Calls JetGetColumnInfo using a columnn name that's inherited.")]
        public void JetGetTemplateColumnInfoByColumnNameOnInheritedColumn()
        {
            // Get columninfo on a column from the base table.
            JET_COLUMNBASE columnbase;
            Api.JetGetColumnInfo(this.sesid, this.dbid, "tableChild", this.columncreatesBase[0].szColumnName, out columnbase);

            Assert.AreEqual(columnbase.coltyp, this.columncreatesBase[0].coltyp);
            Assert.AreEqual(columnbase.szBaseColumnName, this.columncreatesBase[0].szColumnName);

            // REVIEW: This returns the name of the current table, not the base table!
            Assert.AreEqual(columnbase.szBaseTableName, this.tablecreateChild.szTableName);
            Assert.AreEqual(columnbase.cp, this.columncreatesBase[0].cp);
            Assert.AreEqual(columnbase.cbMax, this.columncreatesBase[0].cbMax);
            Assert.AreEqual(columnbase.grbit, this.columncreatesBase[0].grbit);
        }

        /// <summary>
        /// Calls JetGetColumnInfo using a columnn name that's defined in the child table.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Calls JetGetColumnInfo using a columnn name that's defined in the child table.")]
        public void JetGetTemplateColumnInfoByColumnNameOnChildColumn()
        {
            // Get column info on a child column.
            JET_COLUMNBASE columnbase;
            Api.JetGetColumnInfo(this.sesid, this.dbid, "tableChild", this.columncreatesChild[1].szColumnName, out columnbase);

            Assert.AreEqual(columnbase.coltyp, this.columncreatesChild[1].coltyp);
            Assert.AreEqual(columnbase.szBaseColumnName, this.columncreatesChild[1].szColumnName);
            Assert.AreEqual(columnbase.szBaseTableName, this.tablecreateChild.szTableName);
            Assert.AreEqual(columnbase.cp, this.columncreatesChild[1].cp);
            Assert.AreEqual(columnbase.cbMax, this.columncreatesChild[1].cbMax);
            Assert.AreEqual(columnbase.grbit, this.columncreatesChild[1].grbit);
        }

        /// <summary>
        /// Calls JetGetColumnInfo using a columnnid.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Calls JetGetColumnInfo using a columnnid.")]
        public void JetGetTemplateColumnInfoByColumnId()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                return;
            }

            // Get columninfo on a column from the base table using the column id.
            JET_COLUMNBASE columnbase;
            VistaApi.JetGetColumnInfo(this.sesid, this.dbid, "tableChild", this.columncreatesBase[0].columnid, out columnbase);

            Assert.AreEqual(columnbase.coltyp, this.columncreatesBase[0].coltyp);
            Assert.AreEqual(columnbase.szBaseColumnName, this.columncreatesBase[0].szColumnName);
            Assert.AreEqual(columnbase.szBaseTableName, this.tablecreateChild.szTableName);
            Assert.AreEqual(columnbase.cp, this.columncreatesBase[0].cp);
            Assert.AreEqual(columnbase.cbMax, this.columncreatesBase[0].cbMax);
            Assert.AreEqual(columnbase.grbit, this.columncreatesBase[0].grbit);
        }

        /// <summary>
        /// Calls JetGetColumnInfo using a columnn id that's defined in the child table.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Calls JetGetColumnInfo using a columnn id that's defined in the child table.")]
        public void JetGetTemplateColumnInfoByColumIdOnChildColumn()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                return;
            }

            // Get column info on a child column.
            JET_COLUMNBASE columnbase;
            VistaApi.JetGetColumnInfo(this.sesid, this.dbid, "tableChild", this.columncreatesChild[1].columnid, out columnbase);

            Assert.AreEqual(columnbase.coltyp, this.columncreatesChild[1].coltyp);
            Assert.AreEqual(columnbase.szBaseColumnName, this.columncreatesChild[1].szColumnName);
            Assert.AreEqual(columnbase.szBaseTableName, this.tablecreateChild.szTableName);
            Assert.AreEqual(columnbase.cp, this.columncreatesChild[1].cp);
            Assert.AreEqual(columnbase.cbMax, this.columncreatesChild[1].cbMax);
            Assert.AreEqual(columnbase.grbit, this.columncreatesChild[1].grbit);
        }

        #endregion
    }
}