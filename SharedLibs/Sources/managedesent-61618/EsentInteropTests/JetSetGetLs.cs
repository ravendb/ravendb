//-----------------------------------------------------------------------
// <copyright file="JetSetGetLs.cs" company="Microsoft Corporation">
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
    /// Tests for getting and setting JET_LS.
    /// </summary>
    [TestClass]
    public class JetSetGetLs
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
        /// Set when the runtime callback is called.
        /// </summary>
        private bool runtimeCallbackWasCalled;

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// All DDL should be done in this method.
        /// </summary>
        [TestInitialize]
        [Description("Setup for JetSetGetLs")]
        public void Setup()
        {
            this.directory = SetupHelper.CreateRandomDirectory();
            this.database = Path.Combine(this.directory, "database.edb");
            this.table = "table";
            this.instance = SetupHelper.CreateNewInstance(this.directory);

            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.Recovery, 0, "off");
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.MaxTemporaryTables, 0, null);

            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.RuntimeCallback, this.RuntimeCallback, null);

            Api.JetInit(ref this.instance);
            Api.JetBeginSession(this.instance, out this.sesid, String.Empty, String.Empty);
            Api.JetCreateDatabase(this.sesid, this.database, String.Empty, out this.dbid, CreateDatabaseGrbit.None);
            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateTable(this.sesid, this.dbid, this.table, 0, 100, out this.tableid);
            Api.JetCloseTable(this.sesid, this.tableid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out this.tableid);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup for BasicTableTests")]
        public void Teardown()
        {
            Api.JetEndSession(this.sesid, EndSessionGrbit.None);
            Api.JetTerm(this.instance);
            Cleanup.DeleteDirectoryWithRetry(this.directory);
        }

        #endregion Setup/Teardown

        /// <summary>
        /// Set and retrieve a JET_LS handle.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Set and retrieve a JET_LS handle")]
        public void SetAndGetLs()
        {
            JET_LS expected = new JET_LS { Value = new IntPtr(8) };
            JET_LS actual;

            Api.JetSetLS(this.sesid, this.tableid, expected, LsGrbit.Cursor);
            Api.JetGetLS(this.sesid, this.tableid, out actual, LsGrbit.Cursor);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Verify the runtime callback is called when the cursor is closed.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify the runtime callback is called when the cursor is closed")]
        public void VerifyRuntimeCallbackIsCalled()
        {
            var ls = new JET_LS { Value = new IntPtr(8) };
            Api.JetSetLS(this.sesid, this.tableid, ls, LsGrbit.Cursor);
            Api.JetCloseTable(this.sesid, this.tableid);
            Assert.IsTrue(this.runtimeCallbackWasCalled);
        }

        /// <summary>
        /// A JET_CALLBACK delegate used as the runtime callback.
        /// </summary>
        /// <param name="callbackSesid">The session.</param>
        /// <param name="callbackDbid">The database.</param>
        /// <param name="callbackTableid">The table.</param>
        /// <param name="cbtyp">The callback type.</param>
        /// <param name="arg1">Argument 1.</param>
        /// <param name="arg2">Argument 2.</param>
        /// <param name="context">Unused context.</param>
        /// <param name="unused">This parameter is ignored.</param>
        /// <returns>Always returns JET_err.Success.</returns>
        private JET_err RuntimeCallback(
            JET_SESID callbackSesid,
            JET_DBID callbackDbid,
            JET_TABLEID callbackTableid,
            JET_cbtyp cbtyp,
            object arg1,
            object arg2,
            IntPtr context,
            IntPtr unused)
        {
            this.runtimeCallbackWasCalled = true;
            return JET_err.Success;    
        }
    }
}
