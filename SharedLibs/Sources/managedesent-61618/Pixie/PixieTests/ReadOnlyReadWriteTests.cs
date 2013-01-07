//-----------------------------------------------------------------------
// <copyright file="ReadOnlyReadWriteTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.IO;
using Microsoft.Isam.Esent;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PixieTests
{
    /// <summary>
    /// Test that ReadOnly and ReadWrite objects are returned appropriately
    /// and that ReadOnly objects can't modify the database
    /// </summary>
    [TestClass]
    public class ReadOnlyReadWriteTests
    {
        private string directory;

        private string database;

        [TestInitialize]
        public void Setup()
        {
            this.directory = "read_only_read_write_tests";
            this.database = Path.Combine(this.directory, "test.edb");
            Directory.CreateDirectory(this.directory);
        }

        [TestCleanup]
        public void Cleanup()
        {
            Directory.Delete(this.directory, true);
        }

        #region ReadOnly/ReadWrite tests

        /// <summary>
        /// Create a read-only connection.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyOpenDatabaseReadOnlyReturnsReadOnlyConnection()
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            using (Connection readOnlyConnection = Esent.OpenDatabase(this.database, DatabaseOpenMode.ReadOnly))
            {
                Assert.IsInstanceOfType(readOnlyConnection, typeof(ReadOnlyConnection));
            }
        }

        /// <summary>
        /// Create a read-only connection.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyOpenDatabaseReturnsReadWriteConnection()
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            {
                Assert.IsInstanceOfType(connection, typeof(ReadWriteConnection));
            }
        }

        /// <summary>
        /// Check that a read-only connection cannot create a table.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(EsentReadOnlyException))]
        public void VerifyReadOnlyConnectionCannotCreateTable()
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            using (Connection readOnlyConnection = Esent.OpenDatabase(this.database, DatabaseOpenMode.ReadOnly))
            {
                readOnlyConnection.CreateTable("foo");
            }
        }

        /// <summary>
        /// Check that creating a table returns a read-write table
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyTableCreationReturnsReadWriteTable()
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            {
                Table table = connection.CreateTable("foo");
                Assert.IsInstanceOfType(table, typeof(ReadWriteTable));
            }
        }

        /// <summary>
        /// Check that a read-write connection returns a read-write table when
        /// opening the table.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyReadWriteConnectionReturnsReadWriteTable()
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            {
                connection.CreateTable("foo").Dispose();
                Table table = connection.OpenTable("foo");
                Assert.IsInstanceOfType(table, typeof(ReadWriteTable));
            }
        }

        /// <summary>
        /// Check that a read-only connection returns a read-only table when
        /// opening the table.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyReadOnlyConnectionReturnsReadOnlyTable()
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            using (Connection readOnlyConnection = Esent.OpenDatabase(this.database, DatabaseOpenMode.ReadOnly))
            {
                connection.CreateTable("foo");
                Table table = readOnlyConnection.OpenTable("foo");
                Assert.IsInstanceOfType(table, typeof(ReadOnlyTable));
            }
        }

        /// <summary>
        /// Check that a column cannot be added to a read-only table.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(EsentReadOnlyException))]
        public void VerifyReadOnlyTableCannotCreateColumn()
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            using (Connection readOnlyConnection = Esent.OpenDatabase(this.database, DatabaseOpenMode.ReadOnly))
            {
                connection.CreateTable("foo");
                Table table = readOnlyConnection.OpenTable("foo");
                table.CreateColumn(DefinedAs.AsciiTextColumn("col"));
            }
        }

        /// <summary>
        /// Check that a read-only table cannot create a new record.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(EsentReadOnlyException))]
        public void VerifyReadOnlyTableCannotCreateNewRecord()
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            using (Connection readOnlyConnection = Esent.OpenDatabase(this.database, DatabaseOpenMode.ReadOnly))
            {
                connection.CreateTable("foo");
                Table table = readOnlyConnection.OpenTable("foo");
                readOnlyConnection.UsingTransaction(() => table.NewRecord());
            }
        }

        /// <summary>
        /// Check that a read-write table returns a read-write record.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyReadWriteTableReturnsReadWriteRecord()
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            using (Transaction transaction = connection.BeginTransaction())
            {
                Table table = connection.CreateTable("foo").CreateColumn(DefinedAs.AsciiTextColumn("text"));
                table.NewRecord().SetColumn("text", Any.String).Save();
                Record record = table.First();
                Assert.IsInstanceOfType(record, typeof(ReadWriteRecord));
            }
        }

        /// <summary>
        /// Check that a read-only table returns a read-only record.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyReadOnlyTableReturnsReadOnlyRecord()
        {
            this.WithReadOnlyRecord(record => Assert.IsInstanceOfType(record, typeof(ReadOnlyRecord)));
        }

        /// <summary>
        /// Check that a read-only record cannot have its column set.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(EsentReadOnlyException))]
        public void VerifyReadOnlyRecordCannotSetColumn()
        {
            this.WithReadOnlyRecord(record => record.SetColumn(Any.String, Any.Bytes));
        }

        /// <summary>
        /// Check that a read-only record cannot be deleted.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(EsentReadOnlyException))]
        public void VerifyReadOnlyRecordCannotBeDeleted()
        {
            this.WithReadOnlyRecord(record => record.Delete());
        }

        /// <summary>
        /// Check that a read-only record throws a ReadOnly exception when saved.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(EsentReadOnlyException))]
        public void VerifyReadOnlyRecordCannotBeSaved()
        {
            this.WithReadOnlyRecord(record => record.Save());
        }

        /// <summary>
        /// Check that a read-only record throws a ReadOnly exception when an update
        /// is cancelled.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(EsentReadOnlyException))]
        public void VerifyReadOnlyRecordCannotBeCancelled()
        {
            this.WithReadOnlyRecord(record => record.Cancel());
        }

        /// <summary>
        /// Perform the specified action on a read-only record.
        /// </summary>
        /// <param name="action">The action to perform</param>
        private void WithReadOnlyRecord(Action<Record> action)
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            using (Connection readOnlyConnection = Esent.OpenDatabase(this.database, DatabaseOpenMode.ReadOnly))
            using (Table table = connection.CreateTable("foo").CreateColumn(DefinedAs.AsciiTextColumn("text")))
            using (Table readOnlyTable = readOnlyConnection.OpenTable("foo"))
            {
                connection.UsingTransaction(() => table.NewRecord().SetColumn("text", Any.String).Save());
                Record record = readOnlyTable.First();
                action(record);
            }
        }

        #endregion ReadOnly/ReadWrite tests
    }
}
