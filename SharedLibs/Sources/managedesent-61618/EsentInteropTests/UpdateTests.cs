//-----------------------------------------------------------------------
// <copyright file="UpdateTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for the disposable Update object that wraps
    /// JetPrepareUpdate and JetUpdate.
    /// </summary>
    [TestClass]
    public class UpdateTests
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
        /// Identifies the table used by the test.
        /// </summary>
        private JET_TABLEID tableid;

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// All DDL should be done in this method.
        /// </summary>
        [TestInitialize]
        [Description("Setup the UpdateTests fixture.")]
        public void Setup()
        {
            this.directory = SetupHelper.CreateRandomDirectory();
            this.instance = SetupHelper.CreateNewInstance(this.directory);

            // turn off logging so initialization is faster
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.Recovery, 0, "off");
            Api.JetInit(ref this.instance);
            Api.JetBeginSession(this.instance, out this.sesid, String.Empty, String.Empty);

            var columns = new[]
            {
                new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.TTKey }
            };

            var columnids = new JET_COLUMNID[columns.Length];
            Api.JetOpenTempTable(
                this.sesid, columns, columns.Length, TempTableGrbit.ForceMaterialization, out this.tableid, columnids);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup the UpdateTests fixture.")]
        public void Teardown()
        {
            Api.JetEndSession(this.sesid, EndSessionGrbit.None);
            Api.JetTerm(this.instance);
            Cleanup.DeleteDirectoryWithRetry(this.directory);
        }

        /// <summary>
        /// Verify that the UpdateTests.Setup has setup the test fixture properly.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that the UpdateTests.Setup has setup the test fixture properly.")]
        public void VerifyFixtureSetup()
        {
            Assert.AreNotEqual(JET_INSTANCE.Nil, this.instance);
            Assert.AreNotEqual(JET_SESID.Nil, this.sesid);
        }

        #endregion Setup/Teardown

        /// <summary>
        /// Test Update.ToString().
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test Update.ToString()")]
        public void TestUpdateToString()
        {
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Assert.AreEqual("Update (Insert)", update.ToString());
            }
        }

        /// <summary>
        /// Start an update and insert the record.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Start an update and insert the record.")]
        public void TestSaveUpdate()
        {
            Assert.IsFalse(Api.TryMoveFirst(this.sesid, this.tableid));
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                update.Save();
            }
            
            // the table shouldn't be empty any more
            Assert.IsTrue(Api.TryMoveFirst(this.sesid, this.tableid));
        }

        /// <summary>
        /// Start an update, insert the record and goto the bookmark.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Start an update, insert the record and goto the bookmark.")]
        public void TestSaveUpdateGetsBookmark()
        {
            var bookmark = new byte[SystemParameters.BookmarkMost];
            int bookmarkSize;
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                update.Save(bookmark, bookmark.Length, out bookmarkSize);
            }

            Api.JetGotoBookmark(this.sesid, this.tableid, bookmark, bookmarkSize);
        }

        /// <summary>
        /// Start an update, insert the record, save while goto the bookmark.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Start an update, insert the record, save while goto the bookmark.")]
        public void TestSaveAndGotoBookmarkPositionsCursor()
        {
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                update.SaveAndGotoBookmark();
            }

            Api.RetrieveKey(this.sesid, this.tableid, RetrieveKeyGrbit.None);
        }

        /// <summary>
        /// Start an update and cancel the insert.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Start an update and cancel the insert.")]
        public void TestCancelUpdate()
        {
            Assert.IsFalse(Api.TryMoveFirst(this.sesid, this.tableid));
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                update.Cancel();
            }

            // the table should still be empty
            Assert.IsFalse(Api.TryMoveFirst(this.sesid, this.tableid));
        }

        /// <summary>
        /// Start an update and cancel the insert.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Start an update and cancel the insert.")]
        public void TestAutoCancelUpdate()
        {
            Assert.IsFalse(Api.TryMoveFirst(this.sesid, this.tableid));
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
            }

            // the table should still be empty
            Assert.IsFalse(Api.TryMoveFirst(this.sesid, this.tableid));
        }

        /// <summary>
        /// Call Cancel on a disposed object, expecting an exception.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Call Cancel on a disposed object, expecting an exception.")]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void TestCancelThrowsExceptionWhenUpdateIsDisposed()
        {
            var update = new Update(this.sesid, this.tableid, JET_prep.Insert);
            update.Dispose();
            update.Cancel();
        }

        /// <summary>
        /// Call Save on a disposed object, expecting an exception.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Call Save on a disposed object, expecting an exception.")]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void TestSaveThrowsExceptionWhenUpdateIsDisposed()
        {
            var update = new Update(this.sesid, this.tableid, JET_prep.Insert);
            update.Dispose();
            update.Save();
        }

        /// <summary>
        /// Call Save on a cancelled update, expecting an exception.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Call Save on a cancelled update, expecting an exception.")]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestSaveThrowsExceptionWhenUpdateIsCancelled()
        {
            var update = new Update(this.sesid, this.tableid, JET_prep.Insert);
            update.Cancel();
            update.Save();
        }

        /// <summary>
        /// Call Cancel on a cancelled update, expecting an exception.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Call Cancel on a cancelled update, expecting an exception.")]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestCancelThrowsExceptionWhenUpdateIsCancelled()
        {
            var update = new Update(this.sesid, this.tableid, JET_prep.Insert);
            update.Cancel();
            update.Cancel();
        }

        /// <summary>
        /// Call SaveAndGotoBookmark on a disposed object, expecting an exception.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Call SaveAndGotoBookmark on a disposed object, expecting an exception.")]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void TestSaveAndGotoBookmarkThrowsExceptionWhenUpdateIsDisposed()
        {
            var update = new Update(this.sesid, this.tableid, JET_prep.Insert);
            update.Dispose();
            update.SaveAndGotoBookmark();
        }

        /// <summary>
        /// Call SaveAndGotoBookmark on a cancelled update, expecting an exception.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Call SaveAndGotoBookmark on a cancelled update, expecting an exception.")]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestSaveAndGotoBookmarkThrowsExceptionWhenUpdateIsCancelled()
        {
            var update = new Update(this.sesid, this.tableid, JET_prep.Insert);
            update.Cancel();
            update.SaveAndGotoBookmark();
        }
    }
}