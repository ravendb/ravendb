//-----------------------------------------------------------------------
// <copyright file="MetaDataHelpersTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.IO;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for methods that enumerate meta-data.
    /// </summary>
    [TestClass]
    public class MetaDataHelpersTests
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

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// All DDL should be done in this method.
        /// </summary>
        [TestInitialize]
        [Description("Setup the MetaDataHelpersTests test fixture")]
        public void Setup()
        {
            this.directory = SetupHelper.CreateRandomDirectory();
            this.database = Path.Combine(this.directory, "database.edb");
            this.table = "table";
            this.instance = SetupHelper.CreateNewInstance(this.directory);

            // turn off logging so initialization is faster
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.Recovery, 0, "off");
            Api.JetInit(ref this.instance);
            Api.JetBeginSession(this.instance, out this.sesid, String.Empty, String.Empty);
            Api.JetCreateDatabase(this.sesid, this.database, String.Empty, out this.dbid, CreateDatabaseGrbit.None);
            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateTable(this.sesid, this.dbid, this.table, 0, 100, out this.tableid);

            JET_COLUMNID columnid;

            // These columns are all tagged so they are not present in the default record.
            var columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.Bit, grbit = ColumndefGrbit.ColumnTagged };
            Api.JetAddColumn(this.sesid, this.tableid, "Boolean", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.UnsignedByte, grbit = ColumndefGrbit.ColumnTagged };
            Api.JetAddColumn(this.sesid, this.tableid, "Byte", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.Short, grbit = ColumndefGrbit.ColumnTagged };
            Api.JetAddColumn(this.sesid, this.tableid, "Int16", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnTagged };
            Api.JetAddColumn(this.sesid, this.tableid, "Int32", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.Currency, grbit = ColumndefGrbit.ColumnTagged };
            Api.JetAddColumn(this.sesid, this.tableid, "Int64", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.IEEESingle, grbit = ColumndefGrbit.ColumnTagged };
            Api.JetAddColumn(this.sesid, this.tableid, "Float", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.IEEEDouble, grbit = ColumndefGrbit.ColumnTagged };
            Api.JetAddColumn(this.sesid, this.tableid, "Double", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.DateTime, grbit = ColumndefGrbit.ColumnTagged };
            Api.JetAddColumn(this.sesid, this.tableid, "DateTime", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.LongBinary, grbit = ColumndefGrbit.ColumnTagged };
            Api.JetAddColumn(this.sesid, this.tableid, "Binary", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.LongText, cp = JET_CP.ASCII, grbit = ColumndefGrbit.ColumnTagged };
            Api.JetAddColumn(this.sesid, this.tableid, "ASCII", columndef, null, 0, out columnid);

            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.LongText, cp = JET_CP.Unicode, grbit = ColumndefGrbit.ColumnTagged };
            Api.JetAddColumn(this.sesid, this.tableid, "Unicode", columndef, null, 0, out columnid);

            if (EsentVersion.SupportsVistaFeatures)
            {
                // Starting with windows Vista esent provides support for these columns.) 
                columndef = new JET_COLUMNDEF() { coltyp = VistaColtyp.UnsignedShort, grbit = ColumndefGrbit.ColumnTagged };
                Api.JetAddColumn(this.sesid, this.tableid, "UInt16", columndef, null, 0, out columnid);

                columndef = new JET_COLUMNDEF() { coltyp = VistaColtyp.UnsignedLong, grbit = ColumndefGrbit.ColumnTagged };
                Api.JetAddColumn(this.sesid, this.tableid, "UInt32", columndef, null, 0, out columnid);

                columndef = new JET_COLUMNDEF() { coltyp = VistaColtyp.GUID, grbit = ColumndefGrbit.ColumnTagged };
                Api.JetAddColumn(this.sesid, this.tableid, "Guid", columndef, null, 0, out columnid);
            }
            else
            {
                // Older version of esent don't support these column types natively so we'll just use binary columns.
                columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.Binary, cbMax = 2, grbit = ColumndefGrbit.ColumnTagged };
                Api.JetAddColumn(this.sesid, this.tableid, "UInt16", columndef, null, 0, out columnid);

                columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.Binary, cbMax = 4, grbit = ColumndefGrbit.ColumnTagged };
                Api.JetAddColumn(this.sesid, this.tableid, "UInt32", columndef, null, 0, out columnid);

                columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.Binary, cbMax = 16, grbit = ColumndefGrbit.ColumnTagged };
                Api.JetAddColumn(this.sesid, this.tableid, "Guid", columndef, null, 0, out columnid);
            }

            // Not natively supported by any version of Esent
            columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.Binary, cbMax = 8, grbit = ColumndefGrbit.ColumnTagged };
            Api.JetAddColumn(this.sesid, this.tableid, "UInt64", columndef, null, 0, out columnid);

            Api.JetCloseTable(this.sesid, this.tableid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out this.tableid);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup the MetaDataHelpersTests test fixture")]
        public void Teardown()
        {
            Api.JetCloseTable(this.sesid, this.tableid);
            Api.JetEndSession(this.sesid, EndSessionGrbit.None);
            Api.JetTerm(this.instance);
            Cleanup.DeleteDirectoryWithRetry(this.directory);
        }

        #endregion

        /// <summary>
        /// Make sure TryOpenTable opens the table when it exists.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify TryOpenTable returns true when the table exists")]
        public void VerifyTryOpenTableReturnsTrueWhenTableExists()
        {
            JET_TABLEID t;
            Assert.IsTrue(Api.TryOpenTable(this.sesid, this.dbid, this.table, OpenTableGrbit.ReadOnly, out t));
            Assert.AreNotEqual(t, JET_TABLEID.Nil);
            Api.JetCloseTable(this.sesid, t);
        }

        /// <summary>
        /// Make sure TryOpenTable returns false when the table doesn't exist.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify TryOpenTable returns false when the table doesn't exist")]
        public void VerifyTryOpenTableReturnsFalseWhenTableDoesNotExist()
        {
            JET_TABLEID t;
            Assert.IsFalse(Api.TryOpenTable(this.sesid, this.dbid, "nosuchtable", OpenTableGrbit.ReadOnly, out t));
        }

        /// <summary>
        /// Repeatedly retrieve meta-data. This is looking for bugs where we don't
        /// release the temp table used to retrieve the data.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Repeatedly retrieve table meta-data with helper methods")]
        public void RepeatedlyRetrieveMetaData()
        {
            var testDuration = TimeSpan.FromSeconds(19);
            var startTime = DateTime.UtcNow;
            int iteration = 0;
            while (DateTime.UtcNow < startTime + testDuration)
            {
                Api.GetColumnDictionary(this.sesid, this.tableid);
                foreach (var x in Api.GetTableColumns(this.sesid, this.tableid))
                {
                    foreach (var y in Api.GetTableIndexes(this.sesid, this.tableid))
                    {
                        foreach (var z in Api.GetTableNames(this.sesid, this.dbid))
                        {
                        }
                    }
                }

                iteration++;
            }

            Console.WriteLine("{0:N0} iterations", iteration);
        }
    }
}
