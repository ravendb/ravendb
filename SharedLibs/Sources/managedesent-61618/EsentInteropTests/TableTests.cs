//-----------------------------------------------------------------------
// <copyright file="TableTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.IO;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for the disposable Table object that wraps a JET_TABLEID.
    /// </summary>
    [TestClass]
    public class TableTests
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
        private string tableName;

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
        [Description("Setup the TableTests fixture.")]
        public void Setup()
        {
            this.directory = SetupHelper.CreateRandomDirectory();
            this.database = Path.Combine(this.directory, "database.edb");
            this.tableName = "table";
            this.instance = SetupHelper.CreateNewInstance(this.directory);

            // turn off logging so initialization is faster
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.Recovery, 0, "off");
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.MaxTemporaryTables, 0, null);
            Api.JetInit(ref this.instance);
            Api.JetBeginSession(this.instance, out this.sesid, String.Empty, String.Empty);
            Api.JetCreateDatabase(this.sesid, this.database, String.Empty, out this.dbid, CreateDatabaseGrbit.None);

            Api.JetBeginTransaction(this.sesid);
            JET_TABLEID tableid;
            Api.JetCreateTable(this.sesid, this.dbid, this.tableName, 0, 100, out tableid);
            Api.JetCloseTable(this.sesid, tableid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.None);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup the TableTests fixture.")]
        public void Teardown()
        {
            Api.JetEndSession(this.sesid, EndSessionGrbit.None);
            Api.JetTerm(this.instance);
            Cleanup.DeleteDirectoryWithRetry(this.directory);
        }

        /// <summary>
        /// Verify that TableTests.Setup has setup the test fixture properly.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that TableTests.Setup has setup the test fixture properly.")]
        public void VerifyFixtureSetup()
        {
            Assert.IsNotNull(this.tableName);
            Assert.AreNotEqual(JET_INSTANCE.Nil, this.instance);
            Assert.AreNotEqual(JET_SESID.Nil, this.sesid);
        }

        #endregion Setup/Teardown

        /// <summary>
        /// Test Table.ToString().
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test Table.ToString()")]
        public void TestTableToString()
        {
            using (var table = new Table(this.sesid, this.dbid, this.tableName, OpenTableGrbit.None))
            {
                Assert.AreEqual(this.tableName, table.ToString());
            }
        }

        /// <summary>
        /// Verify that creating a table object gets a tableid.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that creating a table object gets a tableid.")]
        public void TestTableCreateOpensTable()
        {
            using (var table = new Table(this.sesid, this.dbid, this.tableName, OpenTableGrbit.None))
            {
                Assert.AreNotEqual(JET_TABLEID.Nil, table.JetTableid);
            }
        }

        /// <summary>
        /// Verify that creating a table object sets the name.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that creating a table object sets the name.")]
        public void VerifyTableCreateSetsName()
        {
            using (var table = new Table(this.sesid, this.dbid, this.tableName, OpenTableGrbit.None))
            {
                Assert.AreEqual(this.tableName, table.Name);
            }
        }

        /// <summary>
        /// Verify that a Table object can be implicitly converted to a JET_TABLEID.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that a Table object can be implicitly converted to a JET_TABLEID.")]
        public void VerifyTableCanConvertToJetTableid()
        {
            using (var table = new Table(this.sesid, this.dbid, this.tableName, OpenTableGrbit.None))
            {
                JET_TABLEID tableid = table;
                Assert.AreEqual(tableid, table.JetTableid);
            }
        }

        /// <summary>
        /// Verify that Table.Close zeroes the tableid.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that Table.Close zeroes the tableid.")]
        public void VerifyTableCloseZeroesJetTableid()
        {
            using (var table = new Table(this.sesid, this.dbid, this.tableName, OpenTableGrbit.None))
            {
                table.Close();
                Assert.AreEqual(JET_TABLEID.Nil, table.JetTableid);
            }
        }

        /// <summary>
        /// Verify that Table.Close sets the name to null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that Table.Close sets the name to null.")]
        public void TestTableCloseZeroesName()
        {
            using (var table = new Table(this.sesid, this.dbid, this.tableName, OpenTableGrbit.None))
            {
                table.Close();
                Assert.AreEqual(null, table.Name);
            }
        }

        /// <summary>
        /// Try to close a disposed table, expecting an exception.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Try to close a disposed table, expecting an exception.")]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void TestCloseThrowsExceptionIfTableIsDisposed()
        {
            var table = new Table(this.sesid, this.dbid, this.tableName, OpenTableGrbit.None);
            table.Dispose();
            table.Close();
        }

        /// <summary>
        /// Try to access the JetTableid property of a disposed table,
        /// expecting an exception.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Try to access the JetTableid property of a disposed table, expecting an exception.")]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void TestJetTableidThrowsExceptionIfTableIsDisposed()
        {
            var table = new Table(this.sesid, this.dbid, this.tableName, OpenTableGrbit.None);
            table.Dispose();
            JET_TABLEID x = table.JetTableid;
        }

        /// <summary>
        /// Try to access the Name property of a disposed table,
        /// expecting an exception.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Try to access the Name property of a disposed table, expecting an exception.")]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void TestNamePropertyThrowsExceptionIfTableIsDisposed()
        {
            var table = new Table(this.sesid, this.dbid, this.tableName, OpenTableGrbit.None);
            table.Dispose();
            string x = table.Name;
        }
    }
}