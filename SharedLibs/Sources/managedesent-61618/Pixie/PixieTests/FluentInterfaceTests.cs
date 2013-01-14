//-----------------------------------------------------------------------
// <copyright file="FluentInterfaceTests.cs" company="Microsoft Corporation">
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
    /// Test the fluent interface APIs
    /// </summary>
    [TestClass]
    public class FluentInterfaceTests
    {
        private string directory;

        private string database;

        [TestInitialize]
        public void Setup()
        {
            this.directory = "fluent_interface_tests";
            this.database = Path.Combine(this.directory, "test.edb");
            Directory.CreateDirectory(this.directory);
        }

        [TestCleanup]
        public void Cleanup()
        {
            Directory.Delete(this.directory, true);
        }

        /// <summary>
        /// Test the normal interface
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void TestNonFluentUpdateInterface()
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            using (Transaction transaction = connection.BeginTransaction())
            {
                Table table = connection.CreateTable("mytable");
                table.CreateColumn(new ColumnDefinition("autoinc", ColumnType.Int32) { IsAutoincrement = true });
                table.CreateColumn(new ColumnDefinition("myint", ColumnType.Int32));
                table.CreateColumn(new ColumnDefinition("mystring", ColumnType.Text) { MaxSize = 200 });

                Record record = table.NewRecord();
                record["myint"] = 5;
                record["mystring"] = "hello";
                record.Save();

                transaction.Commit();
            }

            using (Connection connection = Esent.OpenDatabase(this.database, DatabaseOpenMode.ReadOnly))
            {
                Table table = connection.OpenTable("mytable");
                Record record = table.First();

                Assert.AreEqual(1, record["autoinc"]);
                Assert.AreEqual(5, record["myint"]);
                Assert.AreEqual("hello", record["mystring"]);
            }
        }        

        /// <summary>
        /// Test the fluent interface using method chaining.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void TestFluentUpdateInterface()
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            {
                connection.UsingTransaction(() =>
                {
                    Table table = connection.CreateTable("mytable")
                        .CreateColumn(DefinedAs.Int32Column("autoinc").AsAutoincrement())
                        .CreateColumn(DefinedAs.Int32Column("myint"))
                        .CreateColumn(DefinedAs.TextColumn("mystring").WithMaxSize(200));

                    table.NewRecord()
                        .SetColumn("myint", 5)
                        .SetColumn("mystring", "hello")
                        .Save();
                });
            }

            using (Connection connection = Esent.OpenDatabase(this.database, DatabaseOpenMode.ReadOnly))
            {
                Table table = connection.OpenTable("mytable");
                Record record = table.First();

                Assert.AreEqual(1, record["autoinc"]);
                Assert.AreEqual(5, record["myint"]);
                Assert.AreEqual("hello", record["mystring"]);
            }
        }

        /// <summary>
        /// Test the fluent interface using method chaining.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void TestFluentUpdateInterfaceWithLazyTransaction()
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            {
                connection.UsingLazyTransaction(() =>
                {
                    Table table = connection.CreateTable("mytable")
                        .CreateColumn(DefinedAs.Int32Column("autoinc").AsAutoincrement())
                        .CreateColumn(DefinedAs.Int32Column("myint"))
                        .CreateColumn(DefinedAs.TextColumn("mystring").WithMaxSize(200));

                    table.NewRecord()
                        .SetColumn("myint", 5)
                        .SetColumn("mystring", "hello")
                        .Save();
                });
            }

            using (Connection connection = Esent.OpenDatabase(this.database, DatabaseOpenMode.ReadOnly))
            {
                Table table = connection.OpenTable("mytable");
                Record record = table.First();

                Assert.AreEqual(1, record["autoinc"]);
                Assert.AreEqual(5, record["myint"]);
                Assert.AreEqual("hello", record["mystring"]);
            }
        }

        /// <summary>
        /// Test the fluent interface using method chaining.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void TestFluentUpdateInterfaceWithRollback()
        {
            using (Connection connection = Esent.CreateDatabase(this.database))
            {
                Table table = null;
            connection.UsingLazyTransaction(() =>
                {
                    table = connection.CreateTable("mytable")
                        .CreateColumn(DefinedAs.Int32Column("autoinc").AsAutoincrement())
                        .CreateColumn(DefinedAs.Int32Column("myint"))
                        .CreateColumn(DefinedAs.TextColumn("mystring").WithMaxSize(200));
                });

                connection.UsingLazyTransaction(() =>
                    table.NewRecord()
                        .SetColumn("myint", 5)
                        .SetColumn("mystring", "hello")
                        .Save());

                try
                {
                    connection.UsingLazyTransaction(() =>
                    {
                        table.First()
                            .SetColumn("myint", 100)
                            .SetColumn("mystring", "somethingelse")
                            .Save();
                        throw new OutOfMemoryException("cancel this update");
                    });
                }
                catch (OutOfMemoryException)
                {
                    // we expected this
                }
            }

            using (Connection connection = Esent.OpenDatabase(this.database, DatabaseOpenMode.ReadOnly))
            {
                Table table = connection.OpenTable("mytable");
                Record record = table.First();

                Assert.AreEqual(1, record["autoinc"]);
                Assert.AreEqual(5, record["myint"]);
                Assert.AreEqual("hello", record["mystring"]);
            }
        }  
    }
}
