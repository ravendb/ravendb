//-----------------------------------------------------------------------
// <copyright file="RetrieveColumnPerfTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.IO;
using Microsoft.Isam.Esent;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PixieTests
{
    /// <summary>
    /// Test the basic table operations. This fixture is created once and then reused.
    /// For each test a connection is created and the test is wrapped in a transaction
    /// which is rolled back. Do not add any tests that open new connections or the
    /// database will end up being modified which can affect other tests in the fixture.
    /// This fixture has one column of each type.
    /// </summary>
    [TestClass]
    public class RetrieveColumnPerfTests
    {
        private static string directory;
        private static string database;
        private static string tablename;

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
            directory = "retrieve_column_perf";
            database = Path.Combine(directory, "columns.edb");
            Directory.CreateDirectory(directory);

            tablename = "table";

            using (Connection connection = Esent.CreateDatabase(database))
            using (Transaction transaction = connection.BeginTransaction())
            using (Table table = connection.CreateTable(tablename))
            {
                table.CreateColumn(new ColumnDefinition("bool", ColumnType.Bool));
                table.CreateColumn(new ColumnDefinition("byte", ColumnType.Byte));
                table.CreateColumn(new ColumnDefinition("short", ColumnType.Int16));
                table.CreateColumn(new ColumnDefinition("ushort", ColumnType.UInt16));
                table.CreateColumn(new ColumnDefinition("int", ColumnType.Int32));
                table.CreateColumn(new ColumnDefinition("uint", ColumnType.UInt32));
                table.CreateColumn(new ColumnDefinition("long", ColumnType.Int64));
                table.CreateColumn(new ColumnDefinition("float", ColumnType.Float));
                table.CreateColumn(new ColumnDefinition("double", ColumnType.Double));
                table.CreateColumn(new ColumnDefinition("datetime", ColumnType.DateTime));
                table.CreateColumn(new ColumnDefinition("guid", ColumnType.Guid));
                table.CreateColumn(new ColumnDefinition("text", ColumnType.Text));
                table.CreateColumn(new ColumnDefinition("asciitext", ColumnType.AsciiText));
                table.CreateColumn(new ColumnDefinition("binary", ColumnType.Binary));

                table.NewRecord()
                    .SetColumn("bool", Any.Boolean)
                    .SetColumn("byte", Any.Byte)
                    .SetColumn("short", Any.Int16)
                    .SetColumn("ushort", Any.UInt16)
                    .SetColumn("int", Any.Int32)
                    .SetColumn("uint", Any.UInt32)
                    .SetColumn("long", Any.Int64)
                    .SetColumn("float", Any.Float)
                    .SetColumn("double", Any.Double)
                    .SetColumn("datetime", Any.DateTime)
                    .SetColumn("guid", Guid.NewGuid())
                    .SetColumn("text", Any.String)
                    .SetColumn("asciitext", Any.String)
                    .SetColumn("binary", Any.Bytes)
                    .Save();

                transaction.Commit();
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
        /// Measure retrieval performance for Bool columns.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void RetrieveBoolPerf()
        {
            this.TimeColumnRetrieval("bool");
        }

        /// <summary>
        /// Measure retrieval performance for Byte columns.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void RetrieveBytePerf()
        {
            this.TimeColumnRetrieval("byte");
        }

        /// <summary>
        /// Measure retrieval performance for Int16 columns.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void RetrieveInt16Perf()
        {
            this.TimeColumnRetrieval("short");
        }

        /// <summary>
        /// Measure retrieval performance for UInt16 columns.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void RetrieveUInt16Perf()
        {
            this.TimeColumnRetrieval("ushort");
        }

        /// <summary>
        /// Measure retrieval performance for Int32 columns.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void RetrieveInt32Perf()
        {
            this.TimeColumnRetrieval("int");
        }

        /// <summary>
        /// Measure retrieval performance for UInt32 columns.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void RetrieveUInt32Perf()
        {
            this.TimeColumnRetrieval("uint");
        }

        /// <summary>
        /// Measure retrieval performance for Int64 columns.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void RetrieveInt64Perf()
        {
            this.TimeColumnRetrieval("long");
        }

        /// <summary>
        /// Measure retrieval performance for Single columns.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void RetrieveFloatPerf()
        {
            this.TimeColumnRetrieval("float");
        }

        /// <summary>
        /// Measure retrieval performance for Double columns.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void RetrieveDoublePerf()
        {
            this.TimeColumnRetrieval("double");
        }

        /// <summary>
        /// Measure retrieval performance for DateTime columns.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void RetrieveDateTimePerf()
        {
            this.TimeColumnRetrieval("datetime");
        }

        /// <summary>
        /// Measure retrieval performance for Guid columns.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void RetrieveGuidPerf()
        {
            this.TimeColumnRetrieval("guid");
        }

        /// <summary>
        /// Measure retrieval performance for Unicode text columns.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void RetrieveTextPerf()
        {
            this.TimeColumnRetrieval("text");
        }

        /// <summary>
        /// Measure retrieval performance for ASCII text columns.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void RetrieveAsciiTextPerf()
        {
            this.TimeColumnRetrieval("asciitext");
        }

        /// <summary>
        /// Measure retrieval performance for binary columns.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void RetrieveBinaryPerf()
        {
            this.TimeColumnRetrieval("binary");
        }

        /// <summary>
        /// Repeatedly retrieve the same column from the first record in the table.
        /// </summary>
        /// <param name="column">The name of the column to retrieve.</param>
        private void TimeColumnRetrieval(string column)
        {
            const int NumRetrievals = 2000000;

            using (var trx = this.connection.BeginTransaction())
            {
                Record record = this.table.First();

                // Read the column once to force the record to be cached
                var ignored = record[column];

                var stopwatch = Stopwatch.StartNew();
                for (int i = 0; i < NumRetrievals; ++i )
                {
                    ignored = record[column];    
                }
                stopwatch.Stop();

                Console.WriteLine(
                    "Retrieving column '{0}' {1} times took {2}",
                    column,
                    NumRetrievals,
                    stopwatch.Elapsed);
            }
        }
    }
}
