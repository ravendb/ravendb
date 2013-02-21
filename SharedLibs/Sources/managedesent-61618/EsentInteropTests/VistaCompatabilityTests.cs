//-----------------------------------------------------------------------
// <copyright file="VistaCompatabilityTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.IO;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Implementation;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test the Api class functionality when we have an Vista version of Esent.
    /// </summary>
    [TestClass]
    public class VistaCompatibilityTests
    {
        /// <summary>
        /// The saved API, replaced when finished.
        /// </summary>
        private IJetApi savedImpl;

        /// <summary>
        /// Setup the mock object repository.
        /// </summary>
        [TestInitialize]
        [Description("Setup the VistaCompatibilityTests fixture")]
        public void Setup()
        {
            this.savedImpl = Api.Impl;

            // If we aren't running with a version of ESENT that does
            // support Vista features then we can't run these tests.
            if (EsentVersion.SupportsVistaFeatures)
            {
                Api.Impl = new JetApi(Constants.VistaVersion);
            }
        }

        /// <summary>
        /// Cleanup after the test.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup the VistaCompatibilityTests fixture")]
        public void Teardown()
        {
            Api.Impl = this.savedImpl;
        }

        /// <summary>
        /// Verify that the Vista version of ESENT does support
        /// large keys.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the Vista version of ESENT does support large keys")]
        public void VerifyVistaDoesSupportLargeKeys()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                return;
            }

            Assert.IsTrue(EsentVersion.SupportsLargeKeys);
        }

        /// <summary>
        /// Verify that the Vista version of ESENT does support
        /// Windows Server 2003 features.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the Vista version of ESENT does support Windows Server 2003 features")]
        public void VerifyVistaDoesSupportServer2003Features()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                return;
            }

            Assert.IsTrue(EsentVersion.SupportsServer2003Features);
        }

        /// <summary>
        /// Verify that the Vista version of ESENT does support
        /// Unicode paths.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the Vista version of ESENT does support Unicode paths")]
        public void VerifyVistaDoesSupportUnicodePaths()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                return;
            }

            Assert.IsTrue(EsentVersion.SupportsUnicodePaths);
        }

        /// <summary>
        /// Verify that the Vista version of ESENT does support
        /// Windows Vista features.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the Vista version of ESENT does support Windows Vista features")]
        public void VerifyVistaDoesSupportVistaFeatures()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                return;
            }

            Assert.IsTrue(EsentVersion.SupportsVistaFeatures);
        }

        /// <summary>
        /// Verify that the Vista version of ESENT doesn't support
        /// Windows 7 features.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the Vista version of ESENT doesn't support Windows 7 features")]
        public void VerifyVistaDoesNotSupportWindows7Features()
        {
            Assert.IsFalse(EsentVersion.SupportsWindows7Features);
        }

        /// <summary>
        /// Use JetGetDatabaseFileInfo on Vista to test the compatibility path for JET_DBINFOMISC.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Use JetGetDatabaseFileInfo on Vista to test the compatibility path")]
        public void GetDatabaseFileInfoOnVista()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                return;
            }

            string directory = SetupHelper.CreateRandomDirectory();
            string database = Path.Combine(directory, "test.db");

            using (var instance = new Instance("VistaJetGetDatabaseFileInfo"))
            {
                SetupHelper.SetLightweightConfiguration(instance);
                instance.Init();
                using (var session = new Session(instance))
                {
                    JET_DBID dbid;
                    Api.JetCreateDatabase(session, database, String.Empty, out dbid, CreateDatabaseGrbit.None);
                }
            }

            JET_DBINFOMISC dbinfomisc;
            Api.JetGetDatabaseFileInfo(database, out dbinfomisc, JET_DbInfo.Misc);
            Assert.AreEqual(SystemParameters.DatabasePageSize, dbinfomisc.cbPageSize);

            Cleanup.DeleteDirectoryWithRetry(directory);
        }

        /// <summary>
        /// Use JetCreateIndex2 on Vista to test the compatibility path.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Use JetCreateIndex2 on Vista to test the compatibility path")]
        public void CreateIndexesOnVista()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                return;
            }

            string directory = SetupHelper.CreateRandomDirectory();
            string database = Path.Combine(directory, "test.db");

            using (var instance = new Instance("VistaCreateindexes"))
            {
                instance.Parameters.Recovery = false;
                instance.Parameters.NoInformationEvent = true;
                instance.Parameters.MaxTemporaryTables = 0;
                instance.Parameters.TempDirectory = directory;
                instance.Init();
                using (var session = new Session(instance))
                {
                    JET_DBID dbid;
                    Api.JetCreateDatabase(session, database, String.Empty, out dbid, CreateDatabaseGrbit.None);
                    using (var transaction = new Transaction(session))
                    {
                        JET_TABLEID tableid;
                        Api.JetCreateTable(session, dbid, "table", 0, 100, out tableid);
                        JET_COLUMNID columnid;
                        Api.JetAddColumn(
                            session,
                            tableid,
                            "column1",
                            new JET_COLUMNDEF { coltyp = JET_coltyp.Long },
                            null,
                            0,
                            out columnid);

                        var indexcreates = new[]
                        {
                            new JET_INDEXCREATE
                            {
                                szKey = "+column1\0",
                                cbKey = 10,
                                szIndexName = "index1",
                                pidxUnicode = new JET_UNICODEINDEX { lcid = 1033 },
                            },
                        };

                        Api.JetCreateIndex2(session, tableid, indexcreates, indexcreates.Length);
                        transaction.Commit(CommitTransactionGrbit.LazyFlush);
                    }
                }
            }

            Cleanup.DeleteDirectoryWithRetry(directory);
        }

        /// <summary>
        /// Use JetGetRecordSize on Vista to test the compatibility path. This also tests
        /// the handling of the running total option.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Use JetGetRecordSize on Vista to test the compatibility path")]
        public void GetRecordSizeOnVista()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                return;
            }

            string directory = SetupHelper.CreateRandomDirectory();
            string database = Path.Combine(directory, "test.db");

            using (var instance = new Instance("VistaGetRecordSize"))
            {
                instance.Parameters.Recovery = false;
                instance.Parameters.NoInformationEvent = true;
                instance.Parameters.MaxTemporaryTables = 0;
                instance.Init();
                using (var session = new Session(instance))
                {
                    JET_DBID dbid;
                    Api.JetCreateDatabase(session, database, String.Empty, out dbid, CreateDatabaseGrbit.None);
                    using (var transaction = new Transaction(session))
                    {
                        JET_TABLEID tableid;
                        Api.JetCreateTable(session, dbid, "table", 0, 100, out tableid);
                        JET_COLUMNID columnid;
                        Api.JetAddColumn(
                            session,
                            tableid,
                            "column1",
                            new JET_COLUMNDEF { coltyp = JET_coltyp.LongBinary },
                            null,
                            0,
                            out columnid);

                        var size = new JET_RECSIZE();
                        byte[] data = Any.Bytes;

                        using (var update = new Update(session, tableid, JET_prep.Insert))
                        {
                            Api.SetColumn(session, tableid, columnid, data);
                            VistaApi.JetGetRecordSize(session, tableid, ref size, GetRecordSizeGrbit.Local | GetRecordSizeGrbit.InCopyBuffer);
                            update.SaveAndGotoBookmark();
                        }

                        VistaApi.JetGetRecordSize(session, tableid, ref size, GetRecordSizeGrbit.RunningTotal);

                        Assert.AreEqual(data.Length * 2, size.cbData, "cbData");
                        Assert.AreEqual(data.Length * 2, size.cbDataCompressed, "cbDataCompressed");
                        Assert.AreEqual(0, size.cbLongValueData, "cbLongValueData");
                        Assert.AreEqual(0, size.cbLongValueDataCompressed, "cbLongValueDataCompressed");
                        Assert.AreEqual(0, size.cbLongValueOverhead, "cbLongValueOverhead");
                        Assert.AreNotEqual(0, size.cbOverhead, "cbOverhead");
                        Assert.AreEqual(0, size.cCompressedColumns, "cCompressedColumns");
                        Assert.AreEqual(0, size.cLongValues, "cLongValues");
                        Assert.AreEqual(0, size.cMultiValues, "cMultiValues");
                        Assert.AreEqual(0, size.cNonTaggedColumns, "cTaggedColumns");
                        Assert.AreEqual(2, size.cTaggedColumns, "cTaggedColumns");

                        transaction.Commit(CommitTransactionGrbit.LazyFlush);
                    }
                }
            }

            Cleanup.DeleteDirectoryWithRetry(directory);
        }

        /// <summary>
        /// Creates a table with JetCreateTableColumnIndex3 to test the
        /// compatability path.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Create a table with JetCreateTableColumnIndex3")]
        public void CreateTableColumnIndex3OnVista()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                return;
            }

            var columncreates = new JET_COLUMNCREATE[]
            {
                new JET_COLUMNCREATE()
                {
                    szColumnName = "col1_short",
                    coltyp = JET_coltyp.Short,
                    cbMax = 2,
                },
                new JET_COLUMNCREATE()
                {
                    szColumnName = "col2_longtext",
                    coltyp = JET_coltyp.LongText,
                    cp = JET_CP.Unicode,
                },
            };

            const string Index1Name = "firstIndex";
            const string Index1Description = "+col1_short\0-col2_longtext\0";

            const string Index2Name = "secondIndex";
            const string Index2Description = "+col2_longtext\0-col1_short\0";

            var indexcreates = new JET_INDEXCREATE[]
            {
                  new JET_INDEXCREATE
                {
                    szIndexName = Index1Name,
                    szKey = Index1Description,
                    cbKey = Index1Description.Length + 1,
                    grbit = CreateIndexGrbit.None,
                    ulDensity = 99,
                },
                new JET_INDEXCREATE
                {
                    szIndexName = Index2Name,
                    szKey = Index2Description,
                    cbKey = Index2Description.Length + 1,
                    grbit = CreateIndexGrbit.None,
                    ulDensity = 79,
                },
            };

            var tablecreate = new JET_TABLECREATE()
            {
                szTableName = "tableBigBang",
                ulPages = 23,
                ulDensity = 75,
                cColumns = columncreates.Length,
                rgcolumncreate = columncreates,
                rgindexcreate = indexcreates,
                cIndexes = indexcreates.Length,
                cbSeparateLV = 100,
                cbtyp = JET_cbtyp.Null,
                grbit = CreateTableColumnIndexGrbit.None,
            };

            string directory = SetupHelper.CreateRandomDirectory();
            string database = Path.Combine(directory, "test.db");

            using (var instance = new Instance("VistaCreateTableColumnIndex3"))
            {
                instance.Parameters.Recovery = false;
                instance.Parameters.NoInformationEvent = true;
                instance.Parameters.MaxTemporaryTables = 0;
                instance.Init();
                using (var session = new Session(instance))
                {
                    JET_DBID dbid;
                    Api.JetCreateDatabase(session, database, String.Empty, out dbid, CreateDatabaseGrbit.None);
                    using (var transaction = new Transaction(session))
                    {
                        Api.JetCreateTableColumnIndex3(session, dbid, tablecreate);
                        Assert.AreNotEqual(JET_TABLEID.Nil, tablecreate.tableid);

                        // 1 table, 2 columns, 2 indices = 5 objects.
                        Assert.AreEqual(tablecreate.cCreated, 5);
                        Assert.AreNotEqual(tablecreate.rgcolumncreate[0].columnid, JET_COLUMNID.Nil);
                        Assert.AreNotEqual(tablecreate.rgcolumncreate[1].columnid, JET_COLUMNID.Nil);

                        Api.JetCloseTable(session, tablecreate.tableid);
                        transaction.Commit(CommitTransactionGrbit.LazyFlush);
                    }
                }
            }
        }

        /// <summary>
        /// Creates a basic table with JetCreateTableColumnIndex3 to test the
        /// compatability path.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Create a basic table with JetCreateTableColumnIndex3")]
        public void CreateBasicTableColumnIndex3OnVista()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                return;
            }

            var tablecreate = new JET_TABLECREATE { szTableName = "table" };

            string directory = SetupHelper.CreateRandomDirectory();
            string database = Path.Combine(directory, "test.db");

            using (var instance = new Instance("VistaCreateBasicTableColumnIndex3"))
            {
                instance.Parameters.Recovery = false;
                instance.Parameters.NoInformationEvent = true;
                instance.Parameters.MaxTemporaryTables = 0;
                instance.Init();
                using (var session = new Session(instance))
                {
                    JET_DBID dbid;
                    Api.JetCreateDatabase(session, database, String.Empty, out dbid, CreateDatabaseGrbit.None);
                    using (var transaction = new Transaction(session))
                    {
                        Api.JetCreateTableColumnIndex3(session, dbid, tablecreate);
                        Assert.AreNotEqual(JET_TABLEID.Nil, tablecreate.tableid);
                        Assert.AreEqual(tablecreate.cCreated, 1);
                        Api.JetCloseTable(session, tablecreate.tableid);
                        transaction.Commit(CommitTransactionGrbit.LazyFlush);
                    }
                }
            }
        }
    }
}