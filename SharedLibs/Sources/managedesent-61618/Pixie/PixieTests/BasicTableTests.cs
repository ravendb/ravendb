//-----------------------------------------------------------------------
// <copyright file="BasicTableTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System.IO;
using Microsoft.Isam.Esent;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PixieTests
{
    /// <summary>
    /// Test the basic table operations.
    /// </summary>
    [TestClass]
    public class BasicTableTests
    {
        private string directory;

        private string database;

        [TestInitialize]
        public void Setup()
        {
            this.directory = "basic_table_tests";
            this.database = Path.Combine(this.directory, "test.edb");
            Directory.CreateDirectory(this.directory);
        }

        [TestCleanup]
        public void Cleanup()
        {
            Directory.Delete(this.directory, true);
        }

        #region Transaction tests

        /// <summary>
        /// Connection.InTransaction should return false when not in a transaction.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void InTransactionIsFalseWhenNotInTransaction()
        {
            using (var connection = Esent.CreateDatabase(this.database) as ConnectionBase)
            {
                Assert.IsFalse(connection.InTransaction);
            }
        }

        /// <summary>
        /// Connection.InTransaction should return false when a transaction is committed.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void InTransactionIsFalseWhenTransactionIsCommitted()
        {
            using (var connection = Esent.CreateDatabase(this.database) as ConnectionBase)
            {
                Transaction trx = connection.BeginTransaction();
                trx.Commit();
                Assert.IsFalse(connection.InTransaction);
            }
        }

        /// <summary>
        /// Connection.InTransaction should return false when a transaction is rolled back.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void InTransactionIsFalseWhenTransactionIsRolledBack()
        {
            using (var connection = Esent.CreateDatabase(this.database) as ConnectionBase)
            {
                Transaction trx = connection.BeginTransaction();
                trx.Rollback();
                Assert.IsFalse(connection.InTransaction);
            }
        }

        /// <summary>
        /// Connection.InTransaction should return true when in a transaction.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void InTransactionIsTrueWhenInTransaction()
        {
            using (var connection = Esent.CreateDatabase(this.database) as ConnectionBase)
            using (Transaction transaction = connection.BeginTransaction())
            {
                Assert.IsTrue(connection.InTransaction);
            }
        }

        /// <summary>
        /// Starting two transactions should give two different objects.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyBeginTransactionTwiceReturnsDifferentTransactions()
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            using (Transaction transaction1 = connection.BeginTransaction())
            using (Transaction transaction2 = connection.BeginTransaction())
            {
                Assert.AreNotEqual(transaction1, transaction2);
            }
        }

        /// <summary>
        /// Inserting a record when not in a transaction should generate an exception.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void InsertRecordWithoutTransaction()
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            using (Table table = connection.CreateTable("table"))
            {
                try
                {
                    Record record = table.NewRecord();
                    Assert.Fail("Expected an EsentException");
                }
                catch (EsentException)
                {
                }
            }
        }

        /// <summary>
        /// Replacing a record when not in a transaction should generate an exception.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void ReplaceRecordWithoutTransaction()
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            using (Table table = connection.CreateTable("table"))
            {
                Transaction trx = connection.BeginTransaction();
                table.CreateColumn(new ColumnDefinition("column", ColumnType.Bool));
                Record record = table.NewRecord();
                record.Save();
                trx.Commit();

                try
                {
                    record["column"] = Any.Boolean;
                    Assert.Fail("Expected an EsentException");
                }
                catch (EsentException)
                {
                }
            }
        }

        #endregion Transaction tests

        /// <summary>
        /// Create a second connection.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void CreateTwoConnections()
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            using (Connection anotherconnection = Esent.OpenDatabase(this.database))
            using (Table anothertable = anotherconnection.CreateTable("table"))
            {
                Transaction transaction = anotherconnection.BeginTransaction();
                anothertable.CreateColumn(new ColumnDefinition("column", ColumnType.Bool));
                Record anotherrecord = anothertable.NewRecord();
                anotherrecord["column"] = 1;
                anotherrecord.Save();
                transaction.Commit();

                using (Table table = connection.OpenTable("table"))
                {
                    Record record = table.First();
                    Assert.AreEqual(true, record["column"]);
                }
            }
        }

        #region CreateDatabase tests

        /// <summary>
        /// Creating a database that already exists should generate an exception.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void CreateDatabaseThrowsExceptionWhenDatabaseExists()
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            {
            }

            try
            {
                Connection connection = Esent.CreateDatabase(this.database);
                Assert.Fail("Expected and EsentException");
            }
            catch (EsentException)
            {
            }
        }

        /// <summary>
        /// Overwrite a database that already exists.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void OverwriteDatabase()
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            {
            }

            using (Connection connection = Esent.CreateDatabase(this.database, DatabaseCreationMode.OverwriteExisting))
            {
            }
        }

        #endregion CreateDatabase tests
    }
}
