//-----------------------------------------------------------------------
// <copyright file="IndexInfoTests.cs" company="Microsoft Corporation">
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
    /// Tests for JetGetIndexInfo and JetGetTableIndexInfo.
    /// </summary>
    [TestClass]
    public class IndexInfoTests
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

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// All DDL should be done in this method.
        /// </summary>
        [TestInitialize]
        [Description("Setup for IndexInfoTests")]
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

            JET_COLUMNID ignored;
            var columndef = new JET_COLUMNDEF { coltyp = JET_coltyp.Text, cp = JET_CP.Unicode };

            Api.JetAddColumn(this.sesid, this.tableid, "C1", columndef, null, 0, out ignored);
            Api.JetAddColumn(this.sesid, this.tableid, "C2", columndef, null, 0, out ignored);
            Api.JetAddColumn(this.sesid, this.tableid, "C3", columndef, null, 0, out ignored);

            Api.JetCreateIndex(this.sesid, this.tableid, "Primary", CreateIndexGrbit.IndexPrimary, "+C1\0\0", 5, 100);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            JET_INDEXCREATE[] indexcreates = new[]
            {
                new JET_INDEXCREATE { szIndexName = "Index2", cbKey = 5, szKey = "+C2\0\0" },
                new JET_INDEXCREATE { szIndexName = "Index3", cbKey = 5, szKey = "+C3\0\0", cbVarSegMac = 100 },
            };
            Api.JetCreateIndex2(this.sesid, this.tableid, indexcreates, indexcreates.Length);

            Api.JetCloseTable(this.sesid, this.tableid);
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out this.tableid);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup for IndexInfoTests")]
        public void Teardown()
        {
            Api.JetCloseTable(this.sesid, this.tableid);
            Api.JetEndSession(this.sesid, EndSessionGrbit.None);
            Api.JetTerm(this.instance);
            Cleanup.DeleteDirectoryWithRetry(this.directory);
        }

        #endregion

        #region JetGetIndexInfo

        /// <summary>
        /// Test the overload of JetGetIndexInfo that returns a ushort.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the overload of JetGetIndexInfo that returns a ushort")]
        public void TestJetGetIndexInfoUshort()
        {
            ushort result;
            Api.JetGetIndexInfo(this.sesid, this.dbid, this.table, "Index3", out result, JET_IdxInfo.VarSegMac);
            Assert.AreEqual((ushort)100, result);
        }

        /// <summary>
        /// Test the overload of JetGetIndexInfo that returns an int.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the overload of JetGetIndexInfo that returns an int")]
        public void TestJetGetIndexInfoInt()
        {
            int result;
            Api.JetGetIndexInfo(this.sesid, this.dbid, this.table, null, out result, JET_IdxInfo.Count);
            Assert.AreEqual(3, result);
        }

        /// <summary>
        /// Test the overload of JetGetIndexInfo that returns a JET_INDEXID.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the overload of JetGetIndexInfo that returns a JET_INDEXID")]
        public void TestJetGetIndexInfoIndexId()
        {
            JET_INDEXID result;
            Api.JetGetIndexInfo(this.sesid, this.dbid, this.table, "Index2", out result, JET_IdxInfo.IndexId);
        }

        /// <summary>
        /// Test the overload of JetGetIndexInfo that returns a JET_INDEXLIST.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the overload of JetGetIndexInfo that returns a JET_INDEXLIST")]
        public void TestJetGetIndexInfoIndexList()
        {
            JET_INDEXLIST result;
            Api.JetGetIndexInfo(this.sesid, this.dbid, this.table, null, out result, JET_IdxInfo.List);
            Assert.AreEqual(3, result.cRecord);
            Api.JetCloseTable(this.sesid, result.tableid);
        }

        /// <summary>
        /// Test the obsolete overload of JetGetIndexInfo that returns a JET_INDEXLIST.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the obsolete overload of JetGetIndexInfo that returns a JET_INDEXLIST")]
        public void TestJetGetIndexInfoIndexListObsolete()
        {
            JET_INDEXLIST result;
#pragma warning disable 612,618
            Api.JetGetIndexInfo(this.sesid, this.dbid, this.table, null, out result);
#pragma warning restore 612,618
            Assert.AreEqual(3, result.cRecord);
            Api.JetCloseTable(this.sesid, result.tableid);
        }

        #endregion

        #region JetGetTableIndexInfo

        /// <summary>
        /// Test the overload of JetGetTableIndexInfo that returns a ushort.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the overload of JetGetTableIndexInfo that returns a ushort")]
        public void TestJetGetTableIndexInfoUshort()
        {
            ushort result;
            Api.JetGetTableIndexInfo(this.sesid, this.tableid, "Index3", out result, JET_IdxInfo.VarSegMac);
            Assert.AreEqual((ushort)100, result);
        }

        /// <summary>
        /// Test the overload of JetGetTableIndexInfo that returns an int.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the overload of JetGetTableIndexInfo that returns an int")]
        public void TestJetGetTableIndexInfoInt()
        {
            int result;
            Api.JetGetTableIndexInfo(this.sesid, this.tableid, null, out result, JET_IdxInfo.Count);
            Assert.AreEqual(3, result);
        }

        /// <summary>
        /// Test the overload of JetGetTableIndexInfo that returns a JET_INDEXID.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the overload of JetGetTableIndexInfo that returns a JET_INDEXID")]
        public void TestJetGetTableIndexInfoIndexId()
        {
            JET_INDEXID result;
            Api.JetGetTableIndexInfo(this.sesid, this.tableid, "Index2", out result, JET_IdxInfo.IndexId);
        }

        /// <summary>
        /// Test the overload of JetGetTableIndexInfo that returns a JET_INDEXLIST.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the overload of JetGetTableIndexInfo that returns a JET_INDEXLIST")]
        public void TestJetGetTableIndexInfoIndexList()
        {
            JET_INDEXLIST result;
            Api.JetGetTableIndexInfo(this.sesid, this.tableid, null, out result, JET_IdxInfo.List);
            Assert.AreEqual(3, result.cRecord);
            Api.JetCloseTable(this.sesid, result.tableid);
        }

        /// <summary>
        /// Test the obsolete overload of JetGetTableIndexInfo that returns a JET_INDEXLIST.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the obsolete overload of JetGetTableIndexInfo that returns a JET_INDEXLIST")]
        public void TestJetGetTableIndexInfoIndexListObsolete()
        {
            JET_INDEXLIST result;
#pragma warning disable 612,618
            Api.JetGetTableIndexInfo(this.sesid, this.tableid, null, out result);
#pragma warning restore 612,618
            Assert.AreEqual(3, result.cRecord);
            Api.JetCloseTable(this.sesid, result.tableid);
        }

        #endregion
    }
}