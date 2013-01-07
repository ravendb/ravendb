//-----------------------------------------------------------------------
// <copyright file="JetGetThreadStatsTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for JetGetThreadStats
    /// </summary>
    [TestClass]
    public class JetGetThreadStatsTests
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
        /// Columnid of the LongText column in the table.
        /// </summary>
        private JET_COLUMNID columnid;

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// All DDL should be done in this method.
        /// </summary>
        [TestInitialize]
        [Description("Setup for JetGetThreadStatsTests")]
        public void Setup()
        {
            this.directory = SetupHelper.CreateRandomDirectory();
            this.database = Path.Combine(this.directory, "database.edb");
            this.table = "table";
            this.instance = SetupHelper.CreateNewInstance(this.directory);

            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.MaxTemporaryTables, 0, null);
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
            Api.JetAddColumn(this.sesid, this.tableid, "TestColumn", columndef, null, 0, out this.columnid);

            Api.JetCloseTable(this.sesid, this.tableid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out this.tableid);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup for JetGetThreadStatsTests")]
        public void Teardown()
        {
            Api.JetCloseTable(this.sesid, this.tableid);
            Api.JetEndSession(this.sesid, EndSessionGrbit.None);
            Api.JetTerm(this.instance);
            Cleanup.DeleteDirectoryWithRetry(this.directory);
            SetupHelper.CheckProcessForInstanceLeaks();
        }

        /// <summary>
        /// Verify that JetGetThreadStatsTests has setup the test fixture properly.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that JetGetThreadStatsTests has setup the test fixture properly")]
        public void VerifyFixtureSetup()
        {
            Assert.IsNotNull(this.table);
            Assert.AreNotEqual(JET_INSTANCE.Nil, this.instance);
            Assert.AreNotEqual(JET_SESID.Nil, this.sesid);
            Assert.AreNotEqual(JET_DBID.Nil, this.dbid);
            Assert.AreNotEqual(JET_TABLEID.Nil, this.tableid);
            Assert.AreNotEqual(JET_COLUMNID.Nil, this.columnid);

            JET_COLUMNDEF columndef;
            Api.JetGetTableColumnInfo(this.sesid, this.tableid, this.columnid, out columndef);
            Assert.AreEqual(JET_coltyp.LongText, columndef.coltyp);
        }

        #endregion Setup/Teardown

        /// <summary>
        /// Measure JetGetThreadStats perf.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Measure JetGetThreadStats perf")]
        public void JetGetThreadStatsPerf()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                return;
            }

            // Call the method once to force JIT compilation
            JET_THREADSTATS threadstats;
            VistaApi.JetGetThreadStats(out threadstats);

#if DEBUG 
            const int N = 1000;
#else
            const int N = 5000000;
#endif
            var stopwatch = Stopwatch.StartNew();
            for (int i = 0; i < N; ++i)
            {
                VistaApi.JetGetThreadStats(out threadstats);
            }                

            stopwatch.Stop();

            Console.WriteLine(
                "Made {0:N0} calls to JetGetThreadStats in {1} ({2:N6} milliseconds/call)",
                N,
                stopwatch.Elapsed,
                (double)stopwatch.ElapsedMilliseconds / N);
        }

        /// <summary>
        /// Call JetGetThreadStats.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Call JetGetThreadStats")]
        public void JetGetThreadStats()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                return;
            }

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            byte[] data = Any.Bytes;
            Api.JetSetColumn(this.sesid, this.tableid, this.columnid, data, data.Length, SetColumnGrbit.None, null);
            Api.JetUpdate(this.sesid, this.tableid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            JET_THREADSTATS threadstats;
            VistaApi.JetGetThreadStats(out threadstats);
            Assert.AreNotEqual(0, threadstats.cPageReferenced);
            Assert.AreNotEqual(0, threadstats.cLogRecord);
            Assert.AreNotEqual(0, threadstats.cbLogRecord);
        }
    }
}
