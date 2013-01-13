//-----------------------------------------------------------------------
// <copyright file="SqlImplLayerTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Isam.Esent;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Rhino.Mocks;

namespace PixieTests
{
    /// <summary>
    /// Test that the SQL implementation calls the correct methods on
    /// the Connection class.
    /// </summary>
    [TestClass]
    public class SqlImplLayerTests
    {
        /// <summary>
        /// Mock object repository.
        /// </summary>
        private MockRepository mocks;

        /// <summary>
        /// The mock Connection used by the ISqlImpl. This is the 
        /// depended-on component (DOC).
        /// </summary>
        private Connection mockConnection;

        /// <summary>
        /// ISqlImpl. This will use the mock Connection returned by the
        /// ConnectionManager. This is the Subject Under Test (SUT).
        /// </summary>
        private SqlImplBase sqlImpl;

        /// <summary>
        /// Create the mock Connection, setup the stub ConnectionManager to return the
        /// mock Connection and create the ISqlImpl;
        /// </summary>
        [TestInitialize]
        public void Setup()
        {
            this.mocks = new MockRepository();
            this.mockConnection = this.mocks.StrictMock<Connection>();
            SetupResult.For(this.mockConnection.Database).Return("mock.edb");
            SetupResult.For(this.mockConnection.Name).Return("mock_instance");

            // Only the TestSqlImpl provides a method to set the connection
            var sqlImpl = new TestSqlImpl();
            sqlImpl.FTO_SetConnection(this.mockConnection);
            this.sqlImpl = sqlImpl;
        }

        /// <summary>
        /// Dispose of the ISqlImpl that was being tested.
        /// </summary>
        [TestCleanup]
        public void Teardown()
        {
            ((TestSqlImpl)this.sqlImpl).FTO_SetConnection(null);
            this.sqlImpl.Dispose();
        }

        [TestMethod]
        [Priority(1)]
        public void DetachDatabaseClosesConnection()
        {
            // This is the method that must be called
            Expect.Call(() => this.mockConnection.Dispose());

            this.mocks.ReplayAll();
            this.sqlImpl.DetachDatabase();
            this.mocks.VerifyAll();
        }

        [TestMethod]
        [Priority(1)]
        public void BeginTransactionCreatesTransaction()
        {
            var stubTransaction = this.mocks.Stub<Transaction>();

            Expect.Call(this.mockConnection.BeginTransaction()).Return(stubTransaction);
            this.mocks.ReplayAll();
            this.sqlImpl.BeginTransaction();
            this.mocks.VerifyAll();
        }

        [TestMethod]
        [Priority(1)]
        public void BeginSecondTransactionThrowsException()
        {
            var stubTransaction = this.mocks.Stub<Transaction>();

            SetupResult.For(this.mockConnection.BeginTransaction()).Return(stubTransaction);
            this.mocks.ReplayAll();
            this.sqlImpl.BeginTransaction();

            try
            {
                this.sqlImpl.BeginTransaction();
                Assert.Fail("Expected an EsentSqlExecutionException");
            }
            catch (EsentSqlExecutionException)
            {
            }
        }

        [TestMethod]
        [Priority(1)]
        public void CommitTransactionCommitsTransaction()
        {
            var mockTransaction = this.mocks.StrictMock<Transaction>();

            SetupResult.For(this.mockConnection.BeginTransaction()).Return(mockTransaction);
            Expect.Call(() => mockTransaction.Commit());

            this.mocks.ReplayAll();
            this.sqlImpl.BeginTransaction();
            this.sqlImpl.CommitTransaction();
            this.mocks.VerifyAll();
        }

        [TestMethod]
        [Priority(1)]
        public void CommitTransactionTwiceThrowsException()
        {
            var mockTransaction = this.mocks.StrictMock<Transaction>();

            SetupResult.For(this.mockConnection.BeginTransaction()).Return(mockTransaction);
            Expect.Call(() => mockTransaction.Commit());

            this.mocks.ReplayAll();
            this.sqlImpl.BeginTransaction();
            this.sqlImpl.CommitTransaction();

            try
            {
                this.sqlImpl.CommitTransaction();
                Assert.Fail("Expected an EsentSqlExecutionException");
            }
            catch (EsentSqlExecutionException)
            {
            }

            this.mocks.VerifyAll();
        }

        [TestMethod]
        [Priority(1)]
        [ExpectedException(typeof(EsentSqlExecutionException))]
        public void CommitTransactionWithoutBeginThrowsException()
        {
            this.sqlImpl.CommitTransaction();
        }

        [TestMethod]
        [Priority(1)]
        public void SavepointCreatesTransaction()
        {
            var stubTransaction1 = this.mocks.Stub<Transaction>();
            var stubTransaction2 = this.mocks.Stub<Transaction>();

            Expect.Call(this.mockConnection.BeginTransaction()).Return(stubTransaction1);
            Expect.Call(this.mockConnection.BeginTransaction()).Return(stubTransaction2);

            this.mocks.ReplayAll();
            this.sqlImpl.BeginTransaction();
            this.sqlImpl.CreateSavepoint("mysavepoint");
            this.mocks.VerifyAll();
        }

        [TestMethod]
        [Priority(1)]
        [ExpectedException(typeof(EsentSqlExecutionException))]
        public void SavepointWithoutBeginThrowsException()
        {
            this.sqlImpl.CreateSavepoint("mysavepoint");
        }

        [TestMethod]
        [Priority(1)]
        public void RollbackTransactionUndoesTransaction()
        {
            var mockTransaction = this.mocks.StrictMock<Transaction>();

            SetupResult.For(this.mockConnection.BeginTransaction()).Return(mockTransaction);
            Expect.Call(() => mockTransaction.Rollback());

            this.mocks.ReplayAll();
            this.sqlImpl.BeginTransaction();
            this.sqlImpl.RollbackTransaction();
            this.mocks.VerifyAll();
        }

        [TestMethod]
        [Priority(1)]
        [ExpectedException(typeof(EsentSqlExecutionException))]
        public void RollbackWithoutBeginThrowsException()
        {
            this.sqlImpl.RollbackTransaction();
        }

        [TestMethod]
        [Priority(1)]
        public void RollbackTransactionTwiceThrowsException()
        {
            var mockTransaction = this.mocks.StrictMock<Transaction>();

            SetupResult.For(this.mockConnection.BeginTransaction()).Return(mockTransaction);
            Expect.Call(() => mockTransaction.Rollback());

            this.mocks.ReplayAll();
            this.sqlImpl.BeginTransaction();
            this.sqlImpl.RollbackTransaction();

            try
            {
                this.sqlImpl.RollbackTransaction();
                Assert.Fail("Expected an EsentSqlExecutionException");
            }
            catch (EsentSqlExecutionException)
            {
            }

            this.mocks.VerifyAll();
        }

        [TestMethod]
        [Priority(1)]
        public void InsertRecordSetsRecordColumns()
        {
            var stubTransaction = this.mocks.Stub<Transaction>();
            var stubTable = this.mocks.Stub<Table>();
            var mockRecord = this.mocks.DynamicMock<Record>();

            SetupResult.For(this.mockConnection.BeginTransaction()).Return(stubTransaction);
            SetupResult.For(this.mockConnection.OpenTable("stubtable")).Return(stubTable);
            SetupResult.For(stubTable.NewRecord()).Return(mockRecord);
            Expect.Call(mockRecord["mycolumn"] = "somedata");
            Expect.Call(mockRecord["anothercolumn"] = Math.PI);
            Expect.Call(() => mockRecord.Save());

            var data = new KeyValuePair<string, object>[]
            {
                new KeyValuePair<string, object>("mycolumn", "somedata"),
                new KeyValuePair<string, object>("anothercolumn", Math.PI),
            };

            this.mocks.ReplayAll();
            this.sqlImpl.BeginTransaction();
            this.sqlImpl.InsertRecord("stubtable", data);
            this.mocks.VerifyAll();
        }

        [TestMethod]
        [Priority(1)]
        public void InsertSecondRecordDoesNotReopenTable()
        {
            var stubTransaction = this.mocks.Stub<Transaction>();
            var stubTable = this.mocks.Stub<Table>();
            var stubRecord = this.mocks.Stub<Record>();

            Expect.Call(this.mockConnection.BeginTransaction()).Return(stubTransaction);
            Expect.Call(this.mockConnection.OpenTable("stubtable")).Return(stubTable);
            SetupResult.For(stubTable.NewRecord()).Return(stubRecord);
            SetupResult.For(stubTable.TableName).Return("stubtable");

            var data = new KeyValuePair<string, object>[]
            {
                new KeyValuePair<string, object>("mycolumn", "somedata"),
            };

            this.mocks.ReplayAll();
            this.sqlImpl.BeginTransaction();
            this.sqlImpl.InsertRecord("stubtable", data);
            this.sqlImpl.InsertRecord("stubtable", data);
            this.mocks.VerifyAll();
        }
    }
}
