//-----------------------------------------------------------------------
// <copyright file="SimpleTableFixture.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Isam.Esent;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PixieTests
{
    /// <summary>
    /// Test the basic table operations. This fixture is created once and then reused.
    /// For each test a connection is created and the test is wrapped in a transaction
    /// which is rolled back. Do not add any tests that open new connections or the
    /// database will end up being modified which can affect other tests in the fixture.
    /// </summary>
    [TestClass]
    public class SimpleTableFixture
    {
        private static string directory;
        private static string database;
        private static string tablename;
        private static string intColumn;

        private Connection connection;
        private Table table;
        private Transaction transaction;

        /// <summary>
        /// Create the database, table and column. Called once for a test run.
        /// </summary>
        /// <param name="ignored">Ignored TestContext.</param>
        [ClassInitialize]
        public static void CreateDatabase(TestContext ignored)
        {
            directory = "simple_table_fixture";
            database = Path.Combine(directory, "mydatabase.edb");
            Directory.CreateDirectory(directory);

            tablename = "mytable";
            intColumn = "mycolumn";

            using (Connection connection = Esent.CreateDatabase(database))
            using (Table table = connection.CreateTable(tablename))
            {
                table.CreateColumn(new ColumnDefinition(intColumn, ColumnType.Int32));
            }
        }

        /// <summary>
        /// Delete the database. Called after all tests have run.
        /// </summary>
        [ClassCleanup]
        public static void DeleteDatabase()
        {
            Directory.Delete(directory, true);
        }

        /// <summary>
        /// Create a connection, transaction and open the table. This is called once
        /// per test. The transaction will be rolled-back by the Cleanup() method.
        /// </summary>
        [TestInitialize]
        public void Setup()
        {
            this.connection = Esent.OpenDatabase(database);
            this.transaction = this.connection.BeginTransaction();
            this.table = this.connection.OpenTable(tablename);
        }

        /// <summary>
        /// Rollback the transaction and close the connection. This is called once
        /// per test.
        /// </summary>
        [TestCleanup]
        public void Cleanup()
        {
            this.table.Dispose();
            this.transaction.Rollback();
            this.connection.Dispose();
        }

        /// <summary>
        /// insert a record.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void InsertRecord()
        {
            var value = Any.Int32;

            Record r = this.table.NewRecord();
            r[intColumn] = value;
            r.Save();

            Assert.AreEqual(value, r[intColumn]);
        }

        /// <summary>
        /// Update a record.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void UpdateRecord()
        {
            Record r = this.table.NewRecord();
            r[intColumn] = 1;
            r.Save();

            r[intColumn] = 2;
            r.Save();

            Assert.AreEqual(2, r[intColumn]);
        }

        /// <summary>
        /// insert a record.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyColumnNamesAreCaseInsensitive()
        {
            var value = Any.Int32;

            Record r = this.table.NewRecord();
            r[intColumn.ToUpper()] = value;
            r.Save();

            Assert.AreEqual(value, r[intColumn.ToLower()]);
        }

        /// <summary>
        /// Commit an update.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void CommitTransaction()
        {
            Record r = this.table.NewRecord();
            r[intColumn] = 1;
            r.Save();

            Transaction trx = this.connection.BeginTransaction();
            r[intColumn] = 2;
            r.Save();
            trx.Commit();

            Assert.AreEqual(2, r[intColumn]);
        }

        /// <summary>
        /// Rollback an update.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void RollbackTransaction()
        {
            Record r = this.table.NewRecord();
            r[intColumn] = 1;
            r.Save();

            Transaction trx = this.connection.BeginTransaction();
            r[intColumn] = 2;
            r.Save();
            trx.Rollback();

            Assert.AreEqual(1, r[intColumn]);
        }

        /// <summary>
        /// Automatically rollback an update when the Transaction is disposed.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void AutomaticallyRollbackTransaction()
        {
            Record r = this.table.NewRecord();
            r[intColumn] = 1;
            r.Save();

            using (Transaction trx = this.connection.BeginTransaction())
            {
                r[intColumn] = 2;
                r.Save();
            }

            Assert.AreEqual(1, r[intColumn]);
        }

        /// <summary>
        /// Update a record and retrieve the new value before saving.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void UpdateRecordRetrieveCopy()
        {
            Record r = this.table.NewRecord();
            r[intColumn] = 1;
            r.Save();

            r[intColumn] = 2;
            Assert.AreEqual(2, r[intColumn]);
        }

        /// <summary>
        /// Create a new record and try to delete it before inserting it.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void DeleteNonInsertedRecord()
        {
            Record r1 = this.table.NewRecord();
            r1[intColumn] = 1;
            r1.Save();

            Record r2 = this.table.NewRecord();
            try
            {
                r2.Delete();
                Assert.Fail("Expected EsentError exception");
            }
            catch (EsentException)
            {
            }

            Assert.AreEqual(1, r1[intColumn]);
        }

        /// <summary>
        /// Insert a record and delete it.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void DeleteRecord()
        {
            Record r = this.table.NewRecord();
            r[intColumn] = Any.Int32;
            r.Save();

            r.Delete();

            Assert.AreEqual(0, this.table.Count());
        }

        /// <summary>
        /// Delete a record that is being updated. This should cancel the
        /// update and delete the record.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void DeleteRecordThatIsBeingUpdated()
        {
            Record r = this.table.NewRecord();
            r[intColumn] = Any.Int32;
            r.Save();

            r[intColumn] = Any.Int32;
            r.Delete();

            Assert.AreEqual(0, this.table.Count());
        }

        /// <summary>
        /// Insert several records and retrieve them all.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void RetrieveAllRecords()
        {
            for (int i = 0; i < 10; ++i)
            {
                Record r = this.table.NewRecord();
                r[intColumn] = i;
                r.Save();
            }

            this.AssertTableContains(Enumerable.Range(0, 10));
        }

        /// <summary>
        /// Orphan a transaction and make sure that committing
        /// the previous transaction commits it.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void CommitOrphanedTransaction()
        {
            Transaction trx1 = this.connection.BeginTransaction();
            Transaction trx2 = this.connection.BeginTransaction();

            trx1.Commit();
            try
            {
                trx2.Commit();
                Assert.Fail("Expected InvalidOperationException");
            }
            catch (InvalidOperationException)
            {
            }
        }

        /// <summary>
        /// Orphan a transaction and make sure that committing
        /// the previous transaction commits it.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void RollbackOrphanedTransaction()
        {
            Transaction trx1 = this.connection.BeginTransaction();
            Transaction trx2 = this.connection.BeginTransaction();

            trx1.Rollback();
            try
            {
                trx2.Rollback();
                Assert.Fail("Expected InvalidOperationException");
            }
            catch (InvalidOperationException)
            {
            }
        }

        /// <summary>
        /// Try an invalid conversion.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(EsentInvalidConversionException))]
        public void SetColumnThrowsInvalidConversionException()
        {
            Record r = this.table.NewRecord();
            r[intColumn] = DateTime.Now;
        }

        /// <summary>
        /// Try setting a column that doesn't exist
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(EsentColumnNotFoundException))]
        public void SetUnknownColumnThrowsColumnNotFoundException()
        {
            Record r = this.table.NewRecord();
            r["nosuchcolumn"] = Any.Int32;
        }

        /// <summary>
        /// Try retrieving a column that doesn't exist
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(EsentColumnNotFoundException))]
        public void RetrieveUnknownColumnThrowsColumnNotFoundException()
        {
            Record r = this.table.NewRecord();
            object x = r["nosuchcolumn"];
        }

        /// <summary>
        /// Insert multiple records at the same time.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void MultipleActiveInserts()
        {
            Record r1 = this.table.NewRecord();
            Record r2 = this.table.NewRecord();
            Record r3 = this.table.NewRecord();

            r1[intColumn] = 101;
            r2[intColumn] = 102;
            r3[intColumn] = 103;

            r1.Save();
            r2.Save();
            r3.Save();

            this.AssertTableContains(new int[] { 101, 102, 103 });
        }

        /// <summary>
        /// Insert multiple records at the same time.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void MultipleActiveReplaces()
        {
            var records = new Record[20];
            for (int i = 0; i < records.Length; ++i)
            {
                records[i] = this.table.NewRecord();
                records[i][intColumn] = 9999;
                records[i].Save();
            }

            for (int i = 0; i < records.Length; ++i)
            {
                records[i][intColumn] = i;
                records[i].Save();
            }

            for (int i = 0; i < records.Length; ++i)
            {
                Assert.AreEqual(i, records[i][intColumn]);
            }
        }

        /// <summary>
        /// Cancel an update and make sure the record isn't modified.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void CancelReplace()
        {
            Record r = this.table.NewRecord();
            r[intColumn] = 100;
            r.Save();

            r[intColumn] = 101;
            r.Cancel();

            Assert.AreEqual(100, r[intColumn]);
        }

        /// <summary>
        /// Cancel an insert and make sure the record can't be used
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void CancelInsert()
        {
            Record r = this.table.NewRecord();
            r.Cancel();

            try
            {
                object x = r[intColumn];
                Assert.Fail("Expected ObjectDisposedException");
            }
            catch (ObjectDisposedException)
            {
            }
        }

        /// <summary>
        /// Close a table and make sure it can't be used
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void DisposedTableThrowsExceptionOnUse()
        {
            Table t = this.connection.OpenTable(tablename);
            t.Dispose();
            try
            {
                Record r = t.NewRecord();
                Assert.Fail("The table is closed. Expected an exception");
            }
            catch (InvalidOperationException)
            {
            }
        }

        /// <summary>
        /// Let an enumerator be finalized after the table is closed.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void FinalizeEnumerator()
        {
            Table table = this.connection.OpenTable(tablename);

            IEnumerable<Record> x = from r in table
                    select r;

            table.Dispose();

            try
            {
                int i = x.Count();
                Assert.Fail("Should have thrown InvalidOperationException");
            }
            catch (InvalidOperationException)
            {
            }
        }

        /// <summary>
        /// Create and use multiple enumerators.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void MultipleEnumerators()
        {
            foreach (int i in Enumerable.Range(0, 10))
            {
                Record record = this.table.NewRecord();
                record[intColumn] = i;
                record.Save();
            }

            IEnumerable<Record> x = from r in this.table
                    select r;

            IEnumerable<Record> y = from r in this.table
                    where (int)r[intColumn] > 1
                    select r;

            IEnumerable<int> z = from r in this.table
                    select (int)r[intColumn];

            Assert.AreEqual(10, x.Count());
            Assert.AreEqual(8, y.Count());
            Assert.IsTrue(Enumerable.Range(0, 10).SequenceEqual(z));
        }

        /// <summary>
        /// Create a column with a default value.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void CreateColumnWithDefaultValue()
        {
            var columndef = new ColumnDefinition("defaultcolumn", ColumnType.Int32);
            columndef.DefaultValue = "1234";

            this.table.CreateColumn(columndef);

            Record record = this.table.NewRecord();
            record.Save();

            Assert.AreEqual(1234, record["defaultcolumn"]);
        }

        /// <summary>
        /// Rollback of a transaction closes tables opened in the transaction.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyRollbackClosesTable()
        {
            Transaction transactionToRollback = this.connection.BeginTransaction();
            var tableThatWillBeClosed = this.connection.OpenTable(tablename) as TableBase;

            transactionToRollback.Rollback();

            Assert.IsTrue(tableThatWillBeClosed.IsClosed);
        }

        /// <summary>
        /// Rollback of a transaction closes tables opened in the transaction.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyRollbackOfOuterTransactionClosesTable()
        {
            Transaction transactionToRollback = this.connection.BeginTransaction();
            Transaction transactionToCommit = this.connection.BeginTransaction();
            var tableThatWillBeClosed = this.connection.OpenTable(tablename) as TableBase;
            
            transactionToCommit.Commit();
            transactionToRollback.Rollback();

            Assert.IsTrue(tableThatWillBeClosed.IsClosed);
        }

        /// <summary>
        /// Assert that the table contains records with the given values.
        /// </summary>
        /// <param name="expected">The expected values.</param>
        private void AssertTableContains(IEnumerable<int> expected)
        {
            IEnumerable<int> values = from r in this.table
                         select (int)r[intColumn];

            Assert.IsTrue(values.SequenceEqual(expected));
        }
    }
}
