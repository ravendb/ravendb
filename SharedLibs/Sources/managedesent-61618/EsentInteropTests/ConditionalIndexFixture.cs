//-----------------------------------------------------------------------
// <copyright file="ConditionalIndexFixture.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test fixture that has conditional columns.
    /// </summary>
    [TestClass]
    public class ConditionalIndexFixture
    {
        /// <summary>
        /// The directory being used for the database and its files.
        /// </summary>
        private string directory;

        /// <summary>
        /// The path to the database being used by the test.
        /// </summary>
        private string database;

        /// <summary>
        /// The name of the table.
        /// </summary>
        private string table;

        /// <summary>
        /// The instance used by the test.
        /// </summary>
        private JET_INSTANCE instance;

        /// <summary>
        /// The session used by the test.
        /// </summary>
        private JET_SESID sesid;

        /// <summary>
        /// Identifies the database used by the test.
        /// </summary>
        private JET_DBID dbid;

        /// <summary>
        /// The tableid being used by the test.
        /// </summary>
        private JET_TABLEID tableid;

        /// <summary>
        /// Columnid of the key column in the table.
        /// </summary>
        private JET_COLUMNID keyColumn;

        /// <summary>
        /// Columnid of the data column in the table.
        /// </summary>
        private JET_COLUMNID dataColumn;

        /// <summary>
        /// Columnid of a conditional column.
        /// </summary>
        private JET_COLUMNID conditionalColumn1;

        /// <summary>
        /// Columnid of a conditional column.
        /// </summary>
        private JET_COLUMNID conditionalColumn2;

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// All DDL should be done in this method.
        /// </summary>
        [TestInitialize]
        [Description("Setup for ConditionalIndexFixture")]
        public void Setup()
        {
            this.directory = SetupHelper.CreateRandomDirectory();
            this.database = Path.Combine(this.directory, "database.edb");
            this.table = "table";
            this.instance = SetupHelper.CreateNewInstance(this.directory);

            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.Recovery, 0, "off");
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.PageTempDBMin, SystemParameters.PageTempDBSmallest, null);
            Api.JetInit(ref this.instance);
            Api.JetBeginSession(this.instance, out this.sesid, String.Empty, String.Empty);
            Api.JetCreateDatabase(this.sesid, this.database, String.Empty, out this.dbid, CreateDatabaseGrbit.None);
            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateTable(this.sesid, this.dbid, this.table, 0, 100, out this.tableid);

            var columndef = new JET_COLUMNDEF()
            {
                coltyp = JET_coltyp.Long,
            };
            Api.JetAddColumn(this.sesid, this.tableid, "key", columndef, null, 0, out this.keyColumn);

            columndef = new JET_COLUMNDEF()
            {
                cp = JET_CP.Unicode,
                coltyp = JET_coltyp.LongText,
            };
            Api.JetAddColumn(this.sesid, this.tableid, "data", columndef, null, 0, out this.dataColumn);

            columndef = new JET_COLUMNDEF()
            {
                coltyp = JET_coltyp.Bit,
            };
            Api.JetAddColumn(this.sesid, this.tableid, "condition1", columndef, null, 0, out this.conditionalColumn1);
            Api.JetAddColumn(this.sesid, this.tableid, "condition2", columndef, null, 0, out this.conditionalColumn2);

            const string PrimaryIndexDescription = "+key\0\0";
            Api.JetCreateIndex(this.sesid, this.tableid, "primary", CreateIndexGrbit.IndexPrimary, PrimaryIndexDescription, PrimaryIndexDescription.Length, 0);

            Api.JetCloseTable(this.sesid, this.tableid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.DenyRead, out this.tableid);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup for MultivalueIndexFixture")]
        public void Teardown()
        {
            Api.JetCloseTable(this.sesid, this.tableid);
            Api.JetEndSession(this.sesid, EndSessionGrbit.None);
            Api.JetTerm(this.instance);
            Cleanup.DeleteDirectoryWithRetry(this.directory);
        }

        #endregion Setup/Teardown

        /// <summary>
        /// Test creating conditional indexes with JetCreateIndex2.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetCreateIndex2 with conditional columns")]
        public void JetCreateIndex2ConditionalColumns()
        {
            const string IndexKey = "+key\0\0";

            var indexes = new[]
            {
                GetIndexCreate("c1", IndexKey, new[] { "condition1" }, new string[0]),
                GetIndexCreate("c2", IndexKey, new[] { "condition2" }, new string[0]),
                GetIndexCreate("c3", IndexKey, new[] { "condition1", "condition2" }, new string[0]),

                GetIndexCreate("c4", IndexKey, new[] { "condition1" }, new[] { "condition2" }),
                GetIndexCreate("c5", IndexKey, new[] { "condition2" }, new[] { "condition1" }),

                GetIndexCreate("c6", IndexKey, new string[0], new[] { "condition1", }),
                GetIndexCreate("c7", IndexKey, new string[0], new[] { "condition2" }),
                GetIndexCreate("c8", IndexKey, new string[0], new[] { "condition1", "condition2" }),
            };

            Api.JetCreateIndex2(this.sesid, this.tableid, indexes, indexes.Length);

            using (var transaction = new Transaction(this.sesid))
            {
                this.InsertRecord(0, "foo", false, false);
                this.InsertRecord(1, "bar", false, true);
                this.InsertRecord(2, "baz", true, false);
                this.InsertRecord(3, "qux", true, true);

                transaction.Commit(CommitTransactionGrbit.LazyFlush);
            }

            this.AssertIndexHasKeys("primary", 0, 1, 2, 3);
            this.AssertIndexHasKeys("c1", 2, 3);
            this.AssertIndexHasKeys("c2", 1, 3);
            this.AssertIndexHasKeys("c3", 3);
            this.AssertIndexHasKeys("c4", 2);
            this.AssertIndexHasKeys("c5", 1);
            this.AssertIndexHasKeys("c6", 0, 1);
            this.AssertIndexHasKeys("c7", 0, 2);
            this.AssertIndexHasKeys("c8", 0);
        }

        #region Helper Methods

        /// <summary>
        /// Create a JET_INDEXCREATE for the specified index.
        /// </summary>
        /// <param name="name">Name of the index.</param>
        /// <param name="key">Index key.</param>
        /// <param name="mustBeTrue">Columns that must be null.</param>
        /// <param name="mustBeFalse">Columns that must not be null.</param>
        /// <returns>A JET_INDEXCREATE describing the index.</returns>
        private static JET_INDEXCREATE GetIndexCreate(string name, string key, string[] mustBeTrue, string[] mustBeFalse)
        {
            var conditionalcolumns = new JET_CONDITIONALCOLUMN[mustBeTrue.Length + mustBeFalse.Length];
            int i = 0;
            foreach (string column in mustBeTrue)
            {
                conditionalcolumns[i++] = new JET_CONDITIONALCOLUMN
                {
                    szColumnName = column,
                    grbit = ConditionalColumnGrbit.ColumnMustBeNonNull,
                };
            }

            foreach (string column in mustBeFalse)
            {
                conditionalcolumns[i++] = new JET_CONDITIONALCOLUMN
                {
                    szColumnName = column,
                    grbit = ConditionalColumnGrbit.ColumnMustBeNull,
                };
            }

            return new JET_INDEXCREATE
            {
                szIndexName = name,
                szKey = key,
                cbKey = key.Length,
                rgconditionalcolumn = conditionalcolumns,
                cConditionalColumn = conditionalcolumns.Length,
            };            
        }

        /// <summary>
        /// Assert that the given index has the given keys.
        /// </summary>
        /// <param name="index">The name of the index.</param>
        /// <param name="expectedKeys">The expected keys.</param>
        private void AssertIndexHasKeys(string index, params int[] expectedKeys)
        {
            int[] actualKeys = this.GetIndexKeys(index).ToArray();
            CollectionAssert.AreEqual(expectedKeys, actualKeys, "Mistmatch on index {0}", index);
        }

        /// <summary>
        /// Return the keys of all the records in the index.
        /// </summary>
        /// <param name="index">The name of the index.</param>
        /// <returns>An enumeration of the keys in the index.</returns>
        private IEnumerable<int> GetIndexKeys(string index)
        {
            Api.JetSetCurrentIndex(this.sesid, this.tableid, index);
            Api.MoveBeforeFirst(this.sesid, this.tableid);
            while (Api.TryMoveNext(this.sesid, this.tableid))
            {
                yield return (int)Api.RetrieveColumnAsInt32(this.sesid, this.tableid, this.keyColumn);
            }
        }

        /// <summary>
        /// Insert a record with the given column values. After the insert the cursor is 
        /// positioned on the record.
        /// </summary>
        /// <param name="key">
        /// The key of the record.
        /// </param>
        /// <param name="data">
        /// Data to insert.
        /// </param>
        /// <param name="condition1">The value of the first condition.</param>
        /// <param name="condition2">The value of the second condition.</param>
        private void InsertRecord(int key, string data, bool condition1, bool condition2)
        {
            byte[] nonNull = new byte[] { 0x1 };

            using (var transaction = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, this.keyColumn, key);
                Api.SetColumn(this.sesid, this.tableid, this.dataColumn, data, Encoding.Unicode);
                Api.SetColumn(this.sesid, this.tableid, this.conditionalColumn1, condition1 ? nonNull : null);
                Api.SetColumn(this.sesid, this.tableid, this.conditionalColumn2, condition2 ? nonNull : null);

                update.Save();
                transaction.Commit(CommitTransactionGrbit.None);
            }
        }

        #endregion HelperMethods
    }
}
