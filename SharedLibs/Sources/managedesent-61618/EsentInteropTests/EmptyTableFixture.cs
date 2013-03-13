//-----------------------------------------------------------------------
// <copyright file="EmptyTableFixture.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests that use an empty table fixture
    /// </summary>
    [TestClass]
    public class EmptyTableFixture
    {
        /// <summary>
        /// The directory being used for the database and its files.
        /// </summary>
        private string directory;

        /// <summary>
        /// The instance used by the test.
        /// </summary>
        private JET_INSTANCE instance;

        /// <summary>
        /// The session used by the test.
        /// </summary>
        private JET_SESID sesid;

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
        [Description("Setup the EmptyTableFixture")]
        public void Setup()
        {
            this.directory = SetupHelper.CreateRandomDirectory();
            this.instance = SetupHelper.CreateNewInstance(this.directory);

            // turn off logging so initialization is faster
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.Recovery, 0, "off");
            Api.JetInit(ref this.instance);
            Api.JetBeginSession(this.instance, out this.sesid, String.Empty, String.Empty);

            var columns = new[] { new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.TTKey } };
            var columnids = new JET_COLUMNID[columns.Length];

            // BUG: use TempTableGrbit.Indexed once in-memory TT bugs are fixed
            Api.JetOpenTempTable(this.sesid, columns, columns.Length, TempTableGrbit.Scrollable, out this.tableid, columnids);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup the EmptyTableFixture")]
        public void Teardown()
        {
            Api.JetCloseTable(this.sesid, this.tableid);
            Api.JetEndSession(this.sesid, EndSessionGrbit.None);
            Api.JetTerm(this.instance);
            Cleanup.DeleteDirectoryWithRetry(this.directory);
        }

        /// <summary>
        /// Verify that the test class has setup the test fixture properly.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that EmptyTableFixture created the fixture properly")]
        public void VerifyEmptyTableFixtureSetup()
        {
            Assert.AreNotEqual(JET_INSTANCE.Nil, this.instance);
            Assert.AreNotEqual(JET_SESID.Nil, this.sesid);
            Assert.AreNotEqual(JET_TABLEID.Nil, this.tableid);
        }

        #endregion Setup/Teardown

        /// <summary>
        /// Verify that TryMoveFirst returns false when called on an empty table.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that TryMoveFirst returns false when called on an empty table.")]
        public void TryMoveFirstOnEmptyTableReturnsFalse()
        {
            Assert.IsFalse(Api.TryMoveFirst(this.sesid, this.tableid));
        }

        /// <summary>
        /// Verify that TryMoveLast returns false when called on an empty table.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that TryMoveLast returns false when called on an empty table.")]
        public void TryMoveLastOnEmptyTableReturnsFalse()
        {
            Assert.IsFalse(Api.TryMoveLast(this.sesid, this.tableid));
        }

        /// <summary>
        /// Verify that TryMoveNext returns false when called on an empty table.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that TryMoveNext returns false when called on an empty table.")]
        public void TryMoveNextOnEmptyTableReturnsFalse()
        {
            Api.MoveBeforeFirst(this.sesid, this.tableid);
            Assert.IsFalse(Api.TryMoveNext(this.sesid, this.tableid));
        }

        /// <summary>
        /// Verify that TryMovePrevious returns false when called on an empty table.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that TryMovePrevious returns false when called on an empty table.")]
        public void TryMovePreviousOnEmptyTableReturnsFalse()
        {
            Api.MoveAfterLast(this.sesid, this.tableid);
            Assert.IsFalse(Api.TryMovePrevious(this.sesid, this.tableid));
        }

        /// <summary>
        /// Verify that MoveBeforeFirst does not throw an exception when the table is empty.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that MoveBeforeFirst does not throw an exception when the table is empty.")]
        public void MoveBeforeFirstOnEmptyTableDoesNotThrowException()
        {
            Api.MoveBeforeFirst(this.sesid, this.tableid);
        }

        /// <summary>
        /// Verify that MoveAfterLast does not throw an exception when the table is empty.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that MoveAfterLast does not throw an exception when the table is empty.")]
        public void MoveAfterLastOnEmptyTableDoesNotThrowException()
        {
            Api.MoveAfterLast(this.sesid, this.tableid);
        }

        /// <summary>
        /// Verify that TrySetIndexRange throws an exception when ESENT returns an unexpected error.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that TrySetIndexRange throws an exception when ESENT returns an unexpected error.")]
        [ExpectedException(typeof(EsentIllegalOperationException))]
        public void TrySetIndexRangeThrowsExceptionOnError()
        {
            // No key has been made so this call will fail
            Api.TrySetIndexRange(this.sesid, this.tableid, SetIndexRangeGrbit.RangeInstantDuration);
        }
    }
}
