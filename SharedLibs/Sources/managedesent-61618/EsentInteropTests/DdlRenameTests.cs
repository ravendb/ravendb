//-----------------------------------------------------------------------
// <copyright file="DdlRenameTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.IO;
    using System.Text;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Basic Api tests
    /// </summary>
    [TestClass]
    public class DdlRenameTests
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

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// All DDL should be done in this method.
        /// </summary>
        [TestInitialize]
        public void Setup()
        {
            this.directory = SetupHelper.CreateRandomDirectory();
            this.database = Path.Combine(this.directory, "database.edb");
            this.instance = SetupHelper.CreateNewInstance(this.directory);

            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.MaxTemporaryTables, 0, null);
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.Recovery, 0, "off");
            Api.JetInit(ref this.instance);
            Api.JetBeginSession(this.instance, out this.sesid, String.Empty, String.Empty);
            Api.JetCreateDatabase(this.sesid, this.database, String.Empty, out this.dbid, CreateDatabaseGrbit.None);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        public void Teardown()
        {
            Api.JetEndSession(this.sesid, EndSessionGrbit.None);
            Api.JetTerm(this.instance);
            Cleanup.DeleteDirectoryWithRetry(this.directory);
        }

        #endregion

        /// <summary>
        /// Test JetRenameTable.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetRenameTable")]
        public void TestJetRenameTable()
        {
            JET_TABLEID tableid;
            Api.JetCreateTable(this.sesid, this.dbid, "table", 1, 100, out tableid);
            Api.JetCloseTable(this.sesid, tableid);
            Api.JetRenameTable(this.sesid, this.dbid, "table", "newtable");
            Api.JetOpenTable(this.sesid, this.dbid, "newtable", null, 0, OpenTableGrbit.None, out tableid);
        }

        /// <summary>
        /// Test JetRenameColumn.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetRenameColumn")]
        public void TestJetRenameColumn()
        {
            JET_TABLEID tableid;
            Api.JetCreateTable(this.sesid, this.dbid, "table", 1, 100, out tableid);
            JET_COLUMNID columnid;
            Api.JetAddColumn(this.sesid, tableid, "old", new JET_COLUMNDEF { coltyp = JET_coltyp.Long }, null, 0, out columnid);
            Api.JetRenameColumn(this.sesid, tableid, "old", "new", RenameColumnGrbit.None);
            Api.GetTableColumnid(this.sesid, tableid, "new");
            Api.JetCloseTable(this.sesid, tableid);
        }

        /// <summary>
        /// Test JetSetColumnDefaultValue.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetSetColumnDefaultValue")]
        public void TestJetSetColumnDefaultValue()
        {
            JET_TABLEID tableid;
            Api.JetCreateTable(this.sesid, this.dbid, "table", 1, 100, out tableid);

            // The column needs to be a tagged column so the default value isn't persisted
            // in the record at insert time.
            var columndef = new JET_COLUMNDEF
                {
                coltyp = JET_coltyp.LongText,
                cp = JET_CP.Unicode,
                };
            byte[] defaultValue = Encoding.ASCII.GetBytes("default");
            JET_COLUMNID columnid;
            Api.JetAddColumn(this.sesid, tableid, "column", columndef, defaultValue, defaultValue.Length, out columnid);
            Api.JetPrepareUpdate(this.sesid, tableid, JET_prep.Insert);
            Api.JetUpdate(this.sesid, tableid);
            Assert.AreEqual("default", this.RetrieveAsciiColumnFromFirstRecord(tableid, columnid));
            Api.JetCloseTable(this.sesid, tableid);

            byte[] newDefaultValue = Encoding.ASCII.GetBytes("newfault");
            Api.JetSetColumnDefaultValue(
                this.sesid, this.dbid, "table", "column", newDefaultValue, newDefaultValue.Length, SetColumnDefaultValueGrbit.None);

            Api.JetOpenTable(this.sesid, this.dbid, "table", null, 0, OpenTableGrbit.None, out tableid);
            Assert.AreEqual("newfault", this.RetrieveAsciiColumnFromFirstRecord(tableid, columnid));
        }

        /// <summary>
        /// Go to the first record in the table and retrieve the specified column as a string.
        /// </summary>
        /// <param name="tableid">The table containing the record.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <returns>The column as a string.</returns>
        private string RetrieveAsciiColumnFromFirstRecord(JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            Api.JetMove(this.sesid, tableid, JET_Move.First, MoveGrbit.None);
            return Api.RetrieveColumnAsString(this.sesid, tableid, columnid, Encoding.ASCII);
        }
    }
}
