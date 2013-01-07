//-----------------------------------------------------------------------
// <copyright file="TempTableNavigationTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test JetMove/Seek on a temp table.
    /// </summary>
    [TestClass]
    public class TempTableNavigationTests
    {
        /// <summary>
        /// The directory containing the temp db.
        /// </summary>
        private string directory;

        /// <summary>
        /// Number of records inserted in the table.
        /// </summary>
        private int numRecords;

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

        /// <summary>
        /// Columnid of the Long column in the table.
        /// </summary>
        private JET_COLUMNID columnidLong;

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// All DDL should be done in this method.
        /// </summary>
        [TestInitialize]
        [Description("Setup the TempTableNavigationTests fixture")]
        public void Setup()
        {
            var random = new Random();
            this.numRecords = random.Next(5, 20);

            this.directory = SetupHelper.CreateRandomDirectory();
            this.instance = SetupHelper.CreateNewInstance(this.directory);

            // turn off logging so initialization is faster
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.Recovery, 0, "off");
            Api.JetInit(ref this.instance);
            Api.JetBeginSession(this.instance, out this.sesid, String.Empty, String.Empty);

            var columns = new[] { new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.TTKey } };
            var columnids = new JET_COLUMNID[columns.Length];

            // BUG: use TempTableGrbit.Indexed once in-memory TT bugs are fixed
            Api.JetOpenTempTable(this.sesid, columns, columns.Length, TempTableGrbit.ForceMaterialization, out this.tableid, columnids);
            this.columnidLong = columnids[0];

            for (int i = 0; i < this.numRecords; ++i)
            {
                Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
                Api.JetSetColumn(this.sesid, this.tableid, this.columnidLong, BitConverter.GetBytes(i), 4, SetColumnGrbit.None, null);
                int ignored;
                Api.JetUpdate(this.sesid, this.tableid, null, 0, out ignored);
            }
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup the TempTableNavigationTests fixture")]
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
        [Description("Verify the TempTableNavigationTests fixture is setup correctly")]
        public void VerifyFixtureSetup()
        {
            Assert.AreNotEqual(JET_INSTANCE.Nil, this.instance);
            Assert.AreNotEqual(JET_SESID.Nil, this.sesid);
            Assert.IsTrue(this.numRecords > 0);
            Assert.AreNotEqual(JET_COLUMNID.Nil, this.columnidLong);
        }

        #endregion Setup/Teardown

        #region JetSeek Tests

        /// <summary>
        /// Seek for a record with SeekLT.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Seek for a record with SeekLT")]
        public void SeekLT()
        {
            int expected = this.numRecords / 2;
            this.MakeKeyForRecord(expected + 1);    // use the next higher key
            Api.JetSeek(this.sesid, this.tableid, SeekGrbit.SeekLT);
            int actual = this.GetLongColumn();
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Seek for a record with SeekLE.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Seek for a record with SeekLE")]
        public void SeekLE()
        {
            int expected = this.numRecords / 2;
            this.MakeKeyForRecord(expected);
            Api.JetSeek(this.sesid, this.tableid, SeekGrbit.SeekLE);
            int actual = this.GetLongColumn();
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Seek for a record with SeekEQ.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Seek for a record with SeekEQ")]
        public void SeekEQ()
        {
            int expected = this.numRecords / 2;
            this.MakeKeyForRecord(expected);
            Api.JetSeek(this.sesid, this.tableid, SeekGrbit.SeekEQ);
            int actual = this.GetLongColumn();
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Seek for a record with SeekGE.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Seek for a record with SeekGE")]
        public void SeekGE()
        {
            int expected = this.numRecords / 2;
            this.MakeKeyForRecord(expected);
            Api.JetSeek(this.sesid, this.tableid, SeekGrbit.SeekGE);
            int actual = this.GetLongColumn();
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Seek for a record with SeekGT.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Seek for a record with SeekGT")]
        public void SeekGT()
        {
            int expected = this.numRecords / 2;
            this.MakeKeyForRecord(expected - 1);    // use the previous key
            Api.JetSeek(this.sesid, this.tableid, SeekGrbit.SeekGT);
            int actual = this.GetLongColumn();
            Assert.AreEqual(expected, actual);
        }

        #endregion JetSeek Tests

        #region JetMove Tests

        /// <summary>
        /// Test moving to the first record.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test moving to the first record")]
        public void MoveFirst()
        {
            int expected = 0;
            Api.JetMove(this.sesid, this.tableid, JET_Move.First, MoveGrbit.None);
            int actual = this.GetLongColumn();
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test moving previous to the first record.
        /// This should generate an exception.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test moving previous to the first record")]
        [ExpectedException(typeof(EsentNoCurrentRecordException))]
        public void MovingBeforeFirstThrowsException()
        {
            Api.JetMove(this.sesid, this.tableid, JET_Move.First, MoveGrbit.None);
            Api.JetMove(this.sesid, this.tableid, JET_Move.Previous, MoveGrbit.None);
        }

        /// <summary>
        /// Test moving to the next record.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test moving to the next record")]
        public void MoveNext()
        {
            int expected = 1;
            Api.JetMove(this.sesid, this.tableid, JET_Move.First, MoveGrbit.None);
            Api.JetMove(this.sesid, this.tableid, JET_Move.Next, MoveGrbit.None);
            int actual = this.GetLongColumn();
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test moving several records.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test moving several records")]
        public void MoveForwardSeveralRecords()
        {
            int expected = 3;
            Api.JetMove(this.sesid, this.tableid, JET_Move.First, MoveGrbit.None);
            Api.JetMove(this.sesid, this.tableid, expected, MoveGrbit.None);
            int actual = this.GetLongColumn();
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test moving to the last record.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test moving to the last record")]
        public void MoveLast()
        {
            int expected = this.numRecords - 1;
            Api.JetMove(this.sesid, this.tableid, JET_Move.Last, MoveGrbit.None);
            int actual = this.GetLongColumn();
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test moving after the last record.
        /// This should generate an exception.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test moving after the last record")]
        [ExpectedException(typeof(EsentNoCurrentRecordException))]
        public void MovingAfterLastThrowsException()
        {
            Api.JetMove(this.sesid, this.tableid, JET_Move.Last, MoveGrbit.None);
            Api.JetMove(this.sesid, this.tableid, JET_Move.Next, MoveGrbit.None);
        }

        /// <summary>
        /// Test moving to the previous record.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test to the previous record")]
        public void MovePrevious()
        {
            int expected = this.numRecords - 2;
            Api.JetMove(this.sesid, this.tableid, JET_Move.Last, MoveGrbit.None);
            Api.JetMove(this.sesid, this.tableid, JET_Move.Previous, MoveGrbit.None);
            int actual = this.GetLongColumn();
            Assert.AreEqual(expected, actual);
        }

        #endregion JetMove Tests

        #region JetSetIndexRange Tests

        /// <summary>
        /// Test an ascending inclusive index range.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test an ascending inclusive index range")]
        public void TestAscendingInclusiveIndexRange()
        {
            int first = 1;
            int last = this.numRecords - 1;

            this.MakeKeyForRecord(first);
            Api.JetSeek(this.sesid, this.tableid, SeekGrbit.SeekEQ);

            this.MakeKeyForRecord(last);
            Api.JetSetIndexRange(this.sesid, this.tableid, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);

            for (int i = first; i <= last; ++i)
            {
                int actual = this.GetLongColumn();
                Assert.AreEqual(i, actual);
                if (last != i)
                {
                    Api.JetMove(this.sesid, this.tableid, JET_Move.Next, MoveGrbit.None);
                }
            }

            Assert.IsFalse(Api.TryMoveNext(this.sesid, this.tableid));
        }

        /// <summary>
        /// Test an ascending exclusive index range.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test an ascending exclusive index range")]
        public void TestAscendingExclusiveIndexRange()
        {
            int first = 1;
            int last = this.numRecords - 1;

            this.MakeKeyForRecord(first);
            Api.JetSeek(this.sesid, this.tableid, SeekGrbit.SeekEQ);

            this.MakeKeyForRecord(last);
            Api.JetSetIndexRange(this.sesid, this.tableid, SetIndexRangeGrbit.RangeUpperLimit);

            for (int i = first; i < last; ++i)
            {
                int actual = this.GetLongColumn();
                Assert.AreEqual(i, actual);
                if (last - 1 != i)
                {
                    Api.JetMove(this.sesid, this.tableid, JET_Move.Next, MoveGrbit.None);
                }
            }

            Assert.IsFalse(Api.TryMoveNext(this.sesid, this.tableid));
        }

        /// <summary>
        /// Test a descending inclusive index range.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test a descending inclusive index range")]
        public void TestDescendingInclusiveIndexRange()
        {
            int first = 1;
            int last = this.numRecords - 1;

            this.MakeKeyForRecord(last);
            Api.JetSeek(this.sesid, this.tableid, SeekGrbit.SeekEQ);

            this.MakeKeyForRecord(first);
            Api.JetSetIndexRange(this.sesid, this.tableid, SetIndexRangeGrbit.RangeInclusive);

            for (int i = last; i >= first; --i)
            {
                int actual = this.GetLongColumn();
                Assert.AreEqual(i, actual);
                if (first != i)
                {
                    Api.JetMove(this.sesid, this.tableid, JET_Move.Previous, MoveGrbit.None);
                }
            }

            Assert.IsFalse(Api.TryMovePrevious(this.sesid, this.tableid));
        }

        /// <summary>
        /// Test a descending exclusive index range.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test a descending exclusive index range")]
        public void TestDescendingExclusiveIndexRange()
        {
            int first = 1;
            int last = this.numRecords - 1;

            this.MakeKeyForRecord(last);
            Api.JetSeek(this.sesid, this.tableid, SeekGrbit.SeekEQ);

            this.MakeKeyForRecord(first);
            Api.JetSetIndexRange(this.sesid, this.tableid, SetIndexRangeGrbit.None);

            for (int i = last; i > first; --i)
            {
                int actual = this.GetLongColumn();
                Assert.AreEqual(i, actual);
                if (first + 1 != i)
                {
                    Api.JetMove(this.sesid, this.tableid, JET_Move.Previous, MoveGrbit.None);
                }
            }

            Assert.IsFalse(Api.TryMovePrevious(this.sesid, this.tableid));
        }

        /// <summary>
        /// Create a descending exclusive index range with TrySetIndexRange.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Create a descending exclusive index range with TrySetIndexRange")]
        public void TryCreateDescendingExclusiveIndexRange()
        {
            int first = 1;
            int last = this.numRecords - 1;

            this.MakeKeyForRecord(last);
            Api.JetSeek(this.sesid, this.tableid, SeekGrbit.SeekEQ);

            this.MakeKeyForRecord(first);
            Assert.IsTrue(Api.TrySetIndexRange(this.sesid, this.tableid, SetIndexRangeGrbit.None));

            for (int i = last; i > first; --i)
            {
                int actual = this.GetLongColumn();
                Assert.AreEqual(i, actual);
                if (first + 1 != i)
                {
                    Api.JetMove(this.sesid, this.tableid, JET_Move.Previous, MoveGrbit.None);
                }
            }

            Assert.IsFalse(Api.TryMovePrevious(this.sesid, this.tableid));
        }

        /// <summary>
        /// Test removing an index range.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test removing an index range")]
        public void RemoveIndexRange()
        {
            int first = 2;
            int last = this.numRecords - 2;

            this.MakeKeyForRecord(first);
            Api.JetSeek(this.sesid, this.tableid, SeekGrbit.SeekEQ);

            this.MakeKeyForRecord(last);
            Api.JetSetIndexRange(this.sesid, this.tableid, SetIndexRangeGrbit.RangeUpperLimit);
            Api.ResetIndexRange(this.sesid, this.tableid);

            int countedRecords = this.IndexRecordCount();
            Assert.AreEqual(this.numRecords - first, countedRecords);
        }

        /// <summary>
        /// Verify that removing a non-existant index range is not an error when
        /// ResetIndexRange is used.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that removing a non-existant index range is not an error when ResetIndexRange is used.")]
        public void TestRemovingIndexRangeWhenNoRangeExists()
        {
            // Move the table out of insert mode.
            Api.JetMove(this.sesid, this.tableid, JET_Move.First, MoveGrbit.None);
            Api.ResetIndexRange(this.sesid, this.tableid);
        }

        #endregion

        #region MoveHelper Tests

        /// <summary>
        /// Try moving to the first record.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Try moving to the first record")]
        public void TestTryMoveFirst()
        {
            int expected = 0;
            Assert.AreEqual(true, Api.TryMoveFirst(this.sesid, this.tableid));
            int actual = this.GetLongColumn();
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Try moving previous to the first record.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Try moving previous to the first record")]
        public void TestTryMovePreviousReturnsFalseWhenOnFirstRecord()
        {
            Assert.AreEqual(true, Api.TryMoveFirst(this.sesid, this.tableid));
            Assert.AreEqual(false, Api.TryMovePrevious(this.sesid, this.tableid));
        }

        /// <summary>
        /// Move before the first record.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Move before the first record.")]
        public void TestMoveBeforeFirst()
        {
            Api.MoveBeforeFirst(this.sesid, this.tableid);
            Api.JetMove(this.sesid, this.tableid, JET_Move.Next, MoveGrbit.None);
            this.AssertOnRecord(0);
        }

        /// <summary>
        /// Move after the last record.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Move after the last record")]
        public void TestMoveAfterLast()
        {
            Api.MoveAfterLast(this.sesid, this.tableid);
            Api.JetMove(this.sesid, this.tableid, JET_Move.Previous, MoveGrbit.None);
            this.AssertOnRecord(this.numRecords - 1);
        }

        /// <summary>
        /// Try moving to the next record.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Try moving to the next record")]
        public void TestTryMoveNext()
        {
            int expected = 1;
            Assert.AreEqual(true, Api.TryMoveFirst(this.sesid, this.tableid));
            Assert.AreEqual(true, Api.TryMoveNext(this.sesid, this.tableid));
            int actual = this.GetLongColumn();
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Try moving to the last record.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Try moving to the last record")]
        public void TestTryMoveLast()
        {
            int expected = this.numRecords - 1;
            Assert.AreEqual(true, Api.TryMoveLast(this.sesid, this.tableid));
            int actual = this.GetLongColumn();
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Try moving after the last record.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Try moving after the last record")]
        public void VerifyTryMoveNextReturnsFalseWhenOnLastRecord()
        {
            Assert.AreEqual(true, Api.TryMoveLast(this.sesid, this.tableid));
            Assert.AreEqual(false, Api.TryMoveNext(this.sesid, this.tableid));
        }

        /// <summary>
        /// Try moving to the previous record.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Try moving to the previous record")]
        public void TestTryMovePrevious()
        {
            int expected = this.numRecords - 2;
            Assert.IsTrue(Api.TryMoveLast(this.sesid, this.tableid));
            Assert.IsTrue(Api.TryMovePrevious(this.sesid, this.tableid));
            int actual = this.GetLongColumn();
            Assert.AreEqual(expected, actual);
        }

        #endregion MoveHelper Tests

        #region Helper Methods

        /// <summary>
        /// Count the number of records from the current cursor location to the end of the table.
        /// </summary>
        /// <returns>
        /// The number of records from the current cursor location to the end of the table.
        /// </returns>
        private int IndexRecordCount()
        {
            int count = 1;
            while (Api.TryMoveNext(this.sesid, this.tableid))
            {
                count++;
            }

            return count;
        }

        /// <summary>
        /// Assert that we are currently positioned on the given record.
        /// </summary>
        /// <param name="recordId">The expected record ID.</param>
        private void AssertOnRecord(int recordId)
        {
            int actualId = this.GetLongColumn();
            Assert.AreEqual(recordId, actualId);
        }

        /// <summary>
        /// Return the value of the columnidLong of the current record.
        /// </summary>
        /// <returns>The value of the columnid, converted to an int.</returns>
        private int GetLongColumn()
        {
            var data = new byte[4];
            int actualDataSize;
            Api.JetRetrieveColumn(this.sesid, this.tableid, this.columnidLong, data, data.Length, out actualDataSize, RetrieveColumnGrbit.None, null);
            Assert.AreEqual(data.Length, actualDataSize);
            return BitConverter.ToInt32(data, 0);
        }

        /// <summary>
        /// Make a key for a record with the given ID
        /// </summary>
        /// <param name="id">The id of the record.</param>
        private void MakeKeyForRecord(int id)
        {
            byte[] data = BitConverter.GetBytes(id);
            Api.JetMakeKey(this.sesid, this.tableid, data, data.Length, MakeKeyGrbit.NewKey);
        }

        #endregion Helper Methods
    }
}
