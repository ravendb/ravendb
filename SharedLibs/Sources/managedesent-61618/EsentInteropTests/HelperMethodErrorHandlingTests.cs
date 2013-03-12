//-----------------------------------------------------------------------
// <copyright file="HelperMethodErrorHandlingTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Text;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Implementation;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Rhino.Mocks;

    /// <summary>
    /// Test some error handling in helper methods using a
    /// mocked implementation of the API.
    /// </summary>
    [TestClass]
    public class HelperMethodErrorHandlingTests
    {
        /// <summary>
        /// Mock object repository.
        /// </summary>
        private MockRepository mocks;

        /// <summary>
        /// The saved API, replaced when finished.
        /// </summary>
        private IJetApi savedImpl;

        /// <summary>
        /// The mock API.
        /// </summary>
        private IJetApi jetApi;

        /// <summary>
        /// Delegate that matches IJetApi.JetRetrieveColumn.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="data">The data buffer to be retrieved into.</param>
        /// <param name="dataSize">The size of the data buffer.</param>
        /// <param name="actualDataSize">Returns the actual size of the data buffer.</param>
        /// <param name="grbit">Retrieve column options.</param>
        /// <param name="retinfo">
        /// If pretinfo is give as NULL then the function behaves as though an itagSequence
        /// of 1 and an ibLongValue of 0 (zero) were given. This causes column retrieval to
        /// retrieve the first value of a multi-valued column, and to retrieve long data at
        /// offset 0 (zero).
        /// </param>
        /// <returns>An error or warning.</returns>
        private delegate int RetrieveColumnDelegate(
            JET_SESID sesid,
            JET_TABLEID tableid,
            JET_COLUMNID columnid,
            IntPtr data,
            int dataSize,
            out int actualDataSize,
            RetrieveColumnGrbit grbit,
            JET_RETINFO retinfo);

        /// <summary>
        /// Setup the mock object repository.
        /// </summary>
        [TestInitialize]
        [Description("Setup the HelperMethodErrorHandlingTests fixture")]
        public void Setup()
        {
            this.savedImpl = Api.Impl;
            this.mocks = new MockRepository();
            this.jetApi = this.mocks.Stub<IJetApi>();
            Api.Impl = this.jetApi;
        }

        /// <summary>
        /// Cleanup after the test.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup the HelperMethodErrorHandlingTests fixture")]
        public void Teardown()
        {
            Api.Impl = this.savedImpl;
        }

        /// <summary>
        /// Verify an exception is thrown when TryOpenTable gets an unexpected error.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify an exception is thrown when TryOpenTable gets an unexpected error")]
        public void VerifyTryOpenTableThrowsException()
        {
            this.SetupJetOpenTableToReturnError();
            try
            {
                JET_TABLEID tableid;
                Api.TryOpenTable(JET_SESID.Nil, JET_DBID.Nil, "table", OpenTableGrbit.None, out tableid);
                Assert.Fail("Expected an EsentError exception");
            }
            catch (EsentErrorException)
            {
            }
        }

        /// <summary>
        /// Verify an exception is thrown when TryMove gets an unexpected error.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify an exception is thrown when TryMove gets an unexpected error")]
        public void VerifyTryMoveThrowsException()
        {
            this.SetupJetMoveToReturnError();
            try
            {
                Api.TryMove(JET_SESID.Nil, JET_TABLEID.Nil, JET_Move.Next, MoveGrbit.MoveKeyNE);
                Assert.Fail("Expected an EsentError exception");
            }
            catch (EsentErrorException)
            {
            }
        }

        /// <summary>
        /// Verify an exception is thrown when TryMoveFirst gets an unexpected error.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify an exception is thrown when TryMoveFirst gets an unexpected error")]
        public void VerifyTryMoveFirstThrowsException()
        {
            this.SetupJetMoveToReturnError();
            try
            {
                Api.TryMoveFirst(JET_SESID.Nil, JET_TABLEID.Nil);
                Assert.Fail("Expected an EsentError exception");
            }
            catch (EsentErrorException)
            {
            }
        }

        /// <summary>
        /// Verify an exception is thrown when TryMoveLast gets an unexpected error.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify an exception is thrown when TryMoveLast gets an unexpected error")]
        public void VerifyTryMoveLastThrowsException()
        {
            this.SetupJetMoveToReturnError();
            try
            {
                Api.TryMoveLast(JET_SESID.Nil, JET_TABLEID.Nil);
                Assert.Fail("Expected an EsentError exception");
            }
            catch (EsentErrorException)
            {
            }
        }

        /// <summary>
        /// Verify an exception is thrown when TryMoveNext gets an unexpected error.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify an exception is thrown when TryMoveNext gets an unexpected error")]
        public void VerifyTryMoveNextThrowsException()
        {
            this.SetupJetMoveToReturnError();
            try
            {
                Api.TryMoveNext(JET_SESID.Nil, JET_TABLEID.Nil);
                Assert.Fail("Expected an EsentError exception");
            }
            catch (EsentErrorException)
            {
            }
        }

        /// <summary>
        /// Verify an exception is thrown when TryMovePrevious gets an unexpected error.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify an exception is thrown when TryMovePrevious gets an unexpected error")]
        public void VerifyTryMovePreviousThrowsException()
        {
            this.SetupJetMoveToReturnError();
            try
            {
                Api.TryMovePrevious(JET_SESID.Nil, JET_TABLEID.Nil);
                Assert.Fail("Expected an EsentError exception");
            }
            catch (EsentErrorException)
            {
            }
        }

        /// <summary>
        /// Verify an exception is thrown when TrySeek gets an unexpected error.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify an exception is thrown when TrySeek gets an unexpected error")]
        public void VerifyTrySeekThrowsException()
        {
            SetupResult.For(
                this.jetApi.JetSeek(JET_SESID.Nil, JET_TABLEID.Nil, SeekGrbit.SeekEQ))
                .IgnoreArguments()
                .Return((int)JET_err.ReadVerifyFailure);
            this.mocks.ReplayAll();

            try
            {
                Api.TrySeek(JET_SESID.Nil, JET_TABLEID.Nil, SeekGrbit.SeekEQ);
                Assert.Fail("Expected an EsentError exception");
            }
            catch (EsentErrorException)
            {
            }
        }

        /// <summary>
        /// Verify an exception is thrown when TrySetIndexRange gets an unexpected error.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify an exception is thrown when TrySetIndexRange gets an unexpected error")]
        public void VerifyTrySetIndexRangeThrowsException()
        {
            this.SetupJetSetIndexRangeToReturnError();
            try
            {
                Api.TrySetIndexRange(JET_SESID.Nil, JET_TABLEID.Nil, SetIndexRangeGrbit.None);
                Assert.Fail("Expected an EsentError exception");
            }
            catch (EsentErrorException)
            {
            }
        }

        /// <summary>
        /// Verify an exception is thrown when ResetIndexRange gets an unexpected error.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify an exception is thrown when ResetIndexRange gets an unexpected error")]
        public void VerifyResetIndexRangeThrowsException()
        {
            this.SetupJetSetIndexRangeToReturnError();
            try
            {
                Api.ResetIndexRange(JET_SESID.Nil, JET_TABLEID.Nil);
                Assert.Fail("Expected an EsentError exception");
            }
            catch (EsentErrorException)
            {
            }
        }

        /// <summary>
        /// Verify an exception is thrown when GetBookmark gets an unexpected error.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify an exception is thrown when GetBookmark gets an unexpected error")]
        public void VerifyGetBookmarkThrowsException()
        {
            int ignored;
            SetupResult.For(
                this.jetApi.JetGetBookmark(JET_SESID.Nil, JET_TABLEID.Nil, null, 0, out ignored))
                .IgnoreArguments()
                .Return((int)JET_err.OutOfMemory);
            this.mocks.ReplayAll();

            try
            {
                Api.GetBookmark(JET_SESID.Nil, JET_TABLEID.Nil);
                Assert.Fail("Expected an EsentError exception");
            }
            catch (EsentErrorException)
            {
            }
        }

        /// <summary>
        /// Verify an exception is thrown when RetrieveColumn gets a column that grows
        /// when it tries to retrieve it.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify an exception is thrown when RetrieveColumn gets a column that grows when it tries to retrieve it")]
        public void VerifyRetrieveColumnThrowsExceptionWhenColumnSizeGrows()
        {
            this.SetupBadRetrieveColumn();
            try
            {
                Api.RetrieveColumn(JET_SESID.Nil, JET_TABLEID.Nil, JET_COLUMNID.Nil);
                Assert.Fail("Expected an InvalidOperationException exception");
            }
            catch (InvalidOperationException)
            {
            }
        }

        /// <summary>
        /// Verify an exception is thrown when RetrieveColumnAsString gets a column that grows
        /// when it tries to retrieve it.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify an exception is thrown when RetrieveColumnAsString gets a column that grows when it tries to retrieve it")]
        public void VerifyRetrieveColumnAsUnicodeStringThrowsExceptionWhenColumnSizeGrows()
        {
            this.SetupBadRetrieveColumn();
            try
            {
                Api.RetrieveColumnAsString(JET_SESID.Nil, JET_TABLEID.Nil, JET_COLUMNID.Nil, Encoding.Unicode);
                Assert.Fail("Expected an InvalidOperationException exception");
            }
            catch (InvalidOperationException)
            {
            }
        }

        /// <summary>
        /// Verify an exception is thrown when RetrieveColumnAsString gets a column that grows
        /// when it tries to retrieve it.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify an exception is thrown when RetrieveColumnAsString gets a column that grows when it tries to retrieve it")]
        public void VerifyRetrieveColumnAsAsciiStringThrowsExceptionWhenColumnSizeGrows()
        {
            this.SetupBadRetrieveColumn();
            try
            {
                Api.RetrieveColumnAsString(JET_SESID.Nil, JET_TABLEID.Nil, JET_COLUMNID.Nil, Encoding.ASCII);
                Assert.Fail("Expected an InvalidOperationException exception");
            }
            catch (InvalidOperationException)
            {
            }
        }

        /// <summary>
        /// Verify an exception is thrown when TryGetLock gets an unexpected error.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify an exception is thrown when TryGetLock gets an unexpected error")]
        public void VerifyTryGetLockThrowsException()
        {
            this.SetupJetGetLockToReturnError();
            try
            {
                Api.TryGetLock(JET_SESID.Nil, JET_TABLEID.Nil, GetLockGrbit.Read);
                Assert.Fail("Expected an EsentError exception");
            }
            catch (EsentErrorException)
            {
            }
        }

        /// <summary>
        /// A retrieve column function which always claims that the data in the column
        /// is larger than the passed in buffer.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="data">The data buffer to be retrieved into.</param>
        /// <param name="dataSize">The size of the data buffer.</param>
        /// <param name="actualDataSize">Returns the actual size of the data buffer.</param>
        /// <param name="grbit">Retrieve column options.</param>
        /// <param name="retinfo">
        /// Retrieve options.
        /// </param>
        /// <returns>Always returns <see cref="JET_wrn.BufferTruncated"/>.</returns>
        private static int BadRetrieveColumn(
            JET_SESID sesid,
            JET_TABLEID tableid,
            JET_COLUMNID columnid,
            IntPtr data,
            int dataSize,
            out int actualDataSize,
            RetrieveColumnGrbit grbit,
            JET_RETINFO retinfo)
        {
            actualDataSize = (dataSize * 2) + (1024 * 1024);
            return (int)JET_wrn.BufferTruncated;
        }

        /// <summary>
        /// Create a mock implementation and setup the JetRetrieveColumn stub to always
        /// return a cbActual which is greater than the input buffer
        /// </summary>
        private void SetupBadRetrieveColumn()
        {
            int actualDataSize;
            SetupResult.For(
                this.jetApi.JetRetrieveColumn(
                    JET_SESID.Nil,
                    JET_TABLEID.Nil,
                    JET_COLUMNID.Nil,
                    IntPtr.Zero,
                    0,
                    out actualDataSize,
                    RetrieveColumnGrbit.None,
                    null))
                .IgnoreArguments()
                .Do(new RetrieveColumnDelegate(BadRetrieveColumn));
            this.mocks.ReplayAll();
        }

        /// <summary>
        /// Create a mock implementation and setup the JetOpenTable stub to return
        /// an unexpected error.
        /// </summary>
        private void SetupJetOpenTableToReturnError()
        {
            JET_TABLEID tableid;
            SetupResult.For(
                this.jetApi.JetOpenTable(JET_SESID.Nil, JET_DBID.Nil, String.Empty, null, 0, OpenTableGrbit.None, out tableid))
                .IgnoreArguments()
                .OutRef(JET_TABLEID.Nil)
                .Return((int)JET_err.ReadVerifyFailure);
            this.mocks.ReplayAll();
        }

        /// <summary>
        /// Create a mock implementation and setup the JetSetIndexRange stub to return
        /// an unexpected error.
        /// </summary>
        private void SetupJetSetIndexRangeToReturnError()
        {
            SetupResult.For(
                this.jetApi.JetSetIndexRange(JET_SESID.Nil, JET_TABLEID.Nil, SetIndexRangeGrbit.None))
                .IgnoreArguments()
                .Return((int)JET_err.ReadVerifyFailure);
            this.mocks.ReplayAll();
        }

        /// <summary>
        /// Create a mock implementation and setup the JetMove stub to return
        /// an unexpected error.
        /// </summary>
        private void SetupJetMoveToReturnError()
        {
            SetupResult.For(
                this.jetApi.JetMove(JET_SESID.Nil, JET_TABLEID.Nil, 0, MoveGrbit.None))
                .IgnoreArguments()
                .Return((int)JET_err.ReadVerifyFailure);
            this.mocks.ReplayAll();
        }

        /// <summary>
        /// Create a mock implementation and setup the JetGetLock stub to return
        /// an unexpected error.
        /// </summary>
        private void SetupJetGetLockToReturnError()
        {
            SetupResult.For(
                this.jetApi.JetGetLock(JET_SESID.Nil, JET_TABLEID.Nil, GetLockGrbit.Read))
                .IgnoreArguments()
                .Return((int)JET_err.ReadVerifyFailure);
            this.mocks.ReplayAll();
        }
    }
}