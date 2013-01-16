//-----------------------------------------------------------------------
// <copyright file="SqlConnectionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System.IO;
using Microsoft.Isam.Esent;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PixieTests
{
    /// <summary>
    /// Test the SqlConnection class.
    /// </summary>
    [TestClass]
    public class SqlConnectionTests
    {
        /// <summary>
        /// The directory the database should be placed in.
        /// </summary>
        private string directory;

        /// <summary>
        /// The database to use.
        /// </summary>
        private string database;

        /// <summary>
        /// The object being tested.
        /// </summary>
        private SqlConnection sql;

        [TestInitialize]
        public void Setup()
        {
            this.directory = "sql_connection_tests";
            this.database = Path.Combine(this.directory, "sql.edb");
            this.sql = Esent.CreateSqlConnection();
        }

        [TestCleanup]
        public void Teardown()
        {
            this.sql.Dispose();
            if (Directory.Exists(this.directory))
            {
                Directory.Delete(this.directory, true);
            }
        }

        [TestMethod]
        [Priority(1)]
        [ExpectedException(typeof(EsentSqlParseException))]
        public void BadSqlThrowsParseException()
        {
            this.sql.Execute("not a valid SQL string");
        }

        [TestMethod]
        [Priority(1)]
        [ExpectedException(typeof(EsentSqlExecutionException))]
        public void BeginTransactionWithoutDatabaseThrowsException()
        {
            this.sql.Execute("BEGIN TRANSACTION");
        }

        [TestMethod]
        [Priority(1)]
        public void DetachDatabaseWithoutDatabaseDoesNotThrowException()
        {
            this.sql.Execute("DETACH DATABASE");
        }
    }
}
