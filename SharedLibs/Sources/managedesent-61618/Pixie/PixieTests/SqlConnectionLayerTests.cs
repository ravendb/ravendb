//-----------------------------------------------------------------------
// <copyright file="SqlConnectionLayerTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Microsoft.Isam.Esent;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Rhino.Mocks;
using Rhino.Mocks.Constraints;

namespace PixieTests
{
    /// <summary>
    /// Test the SqlConnection using a mock ISqlImpl class. This is used to
    /// make sure that the SQL is parsed properly and the correct ISqlImpl
    /// calls are made.
    /// </summary>
    [TestClass]
    public class SqlConnectionLayerTests
    {
        /// <summary>
        /// Mock object repository.
        /// </summary>
        private MockRepository mocks;

        /// <summary>
        /// The mock SQL implementation. This is the depended-on
        /// component (DOC).
        /// </summary>
        private ISqlImpl mockImpl;

        /// <summary>
        /// The SqlConnection that uses the mock implementation. This is
        /// the subject under test (SUT).
        /// </summary>
        private SqlConnection sql;

        /// <summary>
        /// Setup the mock object and SqlConnection.
        /// </summary>
        [TestInitialize]
        public void Setup()
        {
            this.mocks = new MockRepository();
            this.mockImpl = this.mocks.StrictMock<ISqlImpl>();

            // Creating a SqlConnection will cause a ISqlImpl to be created.
            // Inject the mock ISqlImpl into the SqlConnection through the
            // IoC container.
            Dependencies.Container.RegisterInstance<ISqlImpl>(this.mockImpl);
            this.sql = Dependencies.Container.Resolve<SqlConnection>();
        }

        /// <summary>
        /// Cleanup after the test.
        /// </summary>
        [TestCleanup]
        public void Teardown()
        {
            Dependencies.InitializeContainer();
        }

        /// <summary>
        /// CREATE DATABASE
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlCreateDatabaseCallsSqlImpl()
        {
            Expect.Call(() => this.mockImpl.CreateDatabase("new.edb"));
            this.mocks.ReplayAll();
            this.sql.Execute("CREATE DATABASE 'new.edb'");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// ATTACH DATABASE
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlAttachDatabaseCallsSqlImpl()
        {
            Expect.Call(() => this.mockImpl.AttachDatabase("foo.edb"));
            this.mocks.ReplayAll();
            this.sql.Execute("ATTACH DATABASE 'foo.edb'");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// DETACH DATABASE
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlDetachDatabaseCallsSqlImpl()
        {
            Expect.Call(() => this.mockImpl.DetachDatabase());
            this.mocks.ReplayAll();
            this.sql.Execute("DETACH DATABASE");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// BEGIN
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlBeginCallsSqlImpl()
        {
            Expect.Call(() => this.mockImpl.BeginTransaction());
            this.mocks.ReplayAll();
            this.sql.Execute("BEGIN");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// BEGIN TRANSACTION
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlBeginTransactionCallsSqlImpl()
        {
            Expect.Call(() => this.mockImpl.BeginTransaction());
            this.mocks.ReplayAll();
            this.sql.Execute("BEGIN TRANSACTION");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// COMMIT
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlCommitCallsSqlImpl()
        {
            Expect.Call(() => this.mockImpl.CommitTransaction());
            this.mocks.ReplayAll();
            this.sql.Execute("COMMIT");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// COMMIT TRANSACTION
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlCommitTransactionCallsSqlImpl()
        {
            Expect.Call(() => this.mockImpl.CommitTransaction());
            this.mocks.ReplayAll();
            this.sql.Execute("COMMIT TRANSACTION");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// END
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlEndCallsSqlImpl()
        {
            Expect.Call(() => this.mockImpl.CommitTransaction());
            this.mocks.ReplayAll();
            this.sql.Execute("END");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// END TRANSACTION
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlEndTransactionCallsSqlImpl()
        {
            Expect.Call(() => this.mockImpl.CommitTransaction());
            this.mocks.ReplayAll();
            this.sql.Execute("END TRANSACTION");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// ROLLBACK
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlRollbackCallsSqlImpl()
        {
            Expect.Call(() => this.mockImpl.RollbackTransaction());
            this.mocks.ReplayAll();
            this.sql.Execute("ROLLBACK");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// ROLLBACK TRANSACTION
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlRollbackTransactionCallsSqlImpl()
        {
            Expect.Call(() => this.mockImpl.RollbackTransaction());
            this.mocks.ReplayAll();
            this.sql.Execute("ROLLBACK TRANSACTION");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// ROLLBACK TO
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlRollbackToCallsSqlImpl()
        {
            Expect.Call(() => this.mockImpl.RollbackToSavepoint("mysavepoint"));
            this.mocks.ReplayAll();
            this.sql.Execute("ROLLBACK TO mysavepoint");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// ROLLBACK TRANSACTION TO
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlRollbackTransactionToCallsSqlImpl()
        {
            Expect.Call(() => this.mockImpl.RollbackToSavepoint("mysavepoint"));
            this.mocks.ReplayAll();
            this.sql.Execute("ROLLBACK TRANSACTION TO mysavepoint");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// ROLLBACK TO SAVEPOINT
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlRollbackToSavepointCallsSqlImpl()
        {
            Expect.Call(() => this.mockImpl.RollbackToSavepoint("mysavepoint"));
            this.mocks.ReplayAll();
            this.sql.Execute("ROLLBACK TO SAVEPOINT mysavepoint");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// ROLLBACK TRANSACTION TO SAVEPOINT
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlRollbackTransactionToSavepointCallsSqlImpl()
        {
            Expect.Call(() => this.mockImpl.RollbackToSavepoint("mysavepoint"));
            this.mocks.ReplayAll();
            this.sql.Execute("ROLLBACK TRANSACTION TO SAVEPOINT mysavepoint");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// SAVEPOINT
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlSavepointCallsSqlImpl()
        {
            Expect.Call(() => this.mockImpl.CreateSavepoint("mysavepoint"));
            this.mocks.ReplayAll();
            this.sql.Execute("SAVEPOINT mysavepoint");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// RELEASE
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlReleaseCallsSqlImpl()
        {
            Expect.Call(() => this.mockImpl.CommitSavepoint("mysavepoint"));
            this.mocks.ReplayAll();
            this.sql.Execute("RELEASE mysavepoint");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// RELEASE SAVEPOINT
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlReleaseSavepointCallsSqlImpl()
        {
            Expect.Call(() => this.mockImpl.CommitSavepoint("mysavepoint"));
            this.mocks.ReplayAll();
            this.sql.Execute("RELEASE SAVEPOINT mysavepoint");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// CREATE TABLE (boolean column)
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlCreateTableBooleanColumn()
        {
            this.TestCreateSingleColumnTable(ColumnType.Bool, "BOOLEAN");
        }

        /// <summary>
        /// CREATE TABLE (boolean column)
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlCreateTableBoolColumn()
        {
            this.TestCreateSingleColumnTable(ColumnType.Bool, "BOOL");
        }

        /// <summary>
        /// CREATE TABLE (byte column)
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlCreateTableByteColumn()
        {
            this.TestCreateSingleColumnTable(ColumnType.Byte, "BYTE");
        }

        /// <summary>
        /// CREATE TABLE (short column)
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlCreateTableShortColumn()
        {
            this.TestCreateSingleColumnTable(ColumnType.Int16, "SHORT");
        }

        /// <summary>
        /// CREATE TABLE (int column)
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlCreateTableIntColumn()
        {
            this.TestCreateSingleColumnTable(ColumnType.Int32, "INT");
        }

        /// <summary>
        /// CREATE TABLE (long column)
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlCreateTableLongColumn()
        {
            this.TestCreateSingleColumnTable(ColumnType.Int64, "LONG");
        }

        /// <summary>
        /// CREATE TABLE (datetime column)
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlCreateTableDateTimeColumn()
        {
            this.TestCreateSingleColumnTable(ColumnType.DateTime, "DATETIME");
        }

        /// <summary>
        /// CREATE TABLE (guid column)
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlCreateGuidColumn()
        {
            this.TestCreateSingleColumnTable(ColumnType.Guid, "GUID");
        }

        /// <summary>
        /// CREATE TABLE (text column)
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlCreateTableTextColumn()
        {
            this.TestCreateSingleColumnTable(ColumnType.Text, "TEXT");
        }

        /// <summary>
        /// CREATE TABLE (binary column)
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void TestSqlCreateTableBinaryColumn()
        {
            this.TestCreateSingleColumnTable(ColumnType.Binary, "BINARY");
        }

        /// <summary>
        /// INSERT INTO 
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void VerifySqlInsertIntoCallsSqlImpl()
        {
            Expect.Call(() => this.mockImpl.InsertRecord(null, null)).Constraints(Is.Equal("mytable"), List.Count(Is.Equal(3)));
            this.mocks.ReplayAll();
            this.sql.Execute("INSERT INTO mytable (myid, mystring, mydata) VALUES (123, 'somedata', 5.5)");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// INSERT INTO should throw an exception when there are too many columns
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [ExpectedException(typeof(EsentSqlParseException))]
        public void VerifySqlInsertIntoThrowsExceptionWhenTooManyColumns()
        {
            this.sql.Execute("INSERT INTO mytable (myid, mystring, mydata, extra) VALUES (123, 'somedata', 5.5)");
        }

        /// <summary>
        /// INSERT INTO should throw an exception when there are too many values
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [ExpectedException(typeof(EsentSqlParseException))]
        public void VerifySqlInsertIntoThrowsExceptionWhenTooManyValues()
        {
            this.sql.Execute("INSERT INTO mytable (myid, mystring, mydata) VALUES (123, 'somedata', 5.5, 'extra')");
        }

        /// <summary>
        /// Create a table with a column of the given type and make sure the SqlImplementation class
        /// is called with the right arguments.
        /// </summary>
        /// <param name="columnType">The column type to expect.</param>
        /// <param name="sqlType">The string to use as the SQL column type.</param>
        private void TestCreateSingleColumnTable(ColumnType columnType, string sqlType)
        {
            var columndefs = new ColumnDefinition[]
            {
                new ColumnDefinition("mycolumn", columnType),
            };
            Expect.Call(() => this.mockImpl.CreateTable(null, null)).Constraints(Is.Equal("mytable"), List.Equal(columndefs));
            this.mocks.ReplayAll();
            this.sql.Execute(String.Format("CREATE TABLE mytable (mycolumn {0})", sqlType));
            this.mocks.VerifyAll();
        }
    }
}
