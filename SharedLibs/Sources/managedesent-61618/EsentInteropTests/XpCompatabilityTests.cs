//-----------------------------------------------------------------------
// <copyright file="XpCompatabilityTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.IO;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Implementation;
    using Microsoft.Isam.Esent.Interop.Server2003;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.Isam.Esent.Interop.Windows7;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test the Api class functionality when we have an XP version of Esent.
    /// </summary>
    [TestClass]
    public class XpCompatibilityTests
    {
        /// <summary>
        /// The saved API, replaced when finished.
        /// </summary>
        private IJetApi savedImpl;

        /// <summary>
        /// Create an implementation with a fixed version.
        /// </summary>
        [TestInitialize]
        [Description("Setup the XpCompatibilityTests fixture")]
        public void Setup()
        {
            this.savedImpl = Api.Impl;
            Api.Impl = new JetApi(Constants.XpVersion);
        }

        /// <summary>
        /// Cleanup after the test.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup the XpCompatibilityTests fixture")]
        public void Teardown()
        {
            Api.Impl = this.savedImpl;
        }

        /// <summary>
        /// Verify the XP version of ESENT doesn't support large keys.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify the XP version of ESENT doesn't support large keys")]
        public void VerifyXpDoesNotSupportLargeKeys()
        {
            Assert.IsFalse(EsentVersion.SupportsLargeKeys);
        }

        /// <summary>
        /// Verify the XP version of ESENT doesn't support Windows Server 2003 features.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify the XP version of ESENT doesn't support Windows Server 2003 features")]
        public void VerifyXpDoesNotSupportServer2003Features()
        {
            Assert.IsFalse(EsentVersion.SupportsServer2003Features);
        }

        /// <summary>
        /// Verify the XP version of ESENT doesn't support Unicode paths.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify the XP version of ESENT doesn't support Unicode paths")]
        public void VerifyXpDoesNotSupportUnicodePaths()
        {
            Assert.IsFalse(EsentVersion.SupportsUnicodePaths);
        }

        /// <summary>
        /// Verify the XP version of ESENT doesn't support Windows Vista features.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify the XP version of ESENT doesn't support Windows Vista features")]
        public void VerifyXpDoesNotSupportVistaFeatures()
        {
            Assert.IsFalse(EsentVersion.SupportsVistaFeatures);
        }

        /// <summary>
        /// Verify the XP version of ESENT doesn't support Windows 7 features.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify the XP version of ESENT doesn't support Windows 7 features")]
        public void VerifyXpDoesNotSupportWindows7Features()
        {
            Assert.IsFalse(EsentVersion.SupportsWindows7Features);
        }

        /// <summary>
        /// Verify that JetGetColumnInfo throws an exception when using the
        /// XP version of ESENT.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JetGetColumnInfo throws an exception when using the XP version of ESENT")]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyXpThrowsExceptionOnJetGetColumnInfo()
        {
            JET_COLUMNBASE columnbase;
            VistaApi.JetGetColumnInfo(JET_SESID.Nil, JET_DBID.Nil, Any.String, JET_COLUMNID.Nil, out columnbase);
        }

        /// <summary>
        /// Verify that JetGetThreadStats throws an exception when using the
        /// XP version of ESENT.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JetGetThreadStats throws an exception when using the XP version of ESENT")]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyXpThrowsExceptionOnJetGetThreadStats()
        {
            JET_THREADSTATS threadstats;
            VistaApi.JetGetThreadStats(out threadstats);
        }

        /// <summary>
        /// Verify that JetGetThreadStats throws an exception when using the
        /// XP version of ESENT.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JetGetInstanceMiscInfo throws an exception when using the XP version of ESENT")]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyXpThrowsExceptionOnJetGetInstanceMiscInfo()
        {
            JET_SIGNATURE signature;
            VistaApi.JetGetInstanceMiscInfo(JET_INSTANCE.Nil, out signature, JET_InstanceMiscInfo.LogSignature);
        }

        /// <summary>
        /// Verify that JetOpenTemporaryTable throws an exception when using the
        /// XP version of ESENT.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JetOpenTemporaryTable throws an exception when using the XP version of ESENT")]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyXpThrowsExceptionOnJetOpenTemporaryTable()
        {
            var sesid = new JET_SESID();
            var temporarytable = new JET_OPENTEMPORARYTABLE();
            VistaApi.JetOpenTemporaryTable(sesid, temporarytable);
        }

        /// <summary>
        /// Verify that JetConfigureCrashDump throws an exception when using the
        /// XP version of ESENT.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JetConfigureCrashDump throws an exception when using the XP version of ESENT")]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyXpThrowsExceptionOnJetConfigureProcessForCrashDump()
        {
            Windows7Api.JetConfigureProcessForCrashDump(CrashDumpGrbit.None);
        }

        /// <summary>
        /// Verify that JetOSSnapshotPrepareInstance throws an exception when using the
        /// XP version of ESENT.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JetOSSnapshotPrepareInstance throws an exception when using the XP version of ESENT")]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyXpThrowsExceptionOnJetOSSnapshotPrepareInstance()
        {
            VistaApi.JetOSSnapshotPrepareInstance(JET_OSSNAPID.Nil, JET_INSTANCE.Nil, SnapshotPrepareInstanceGrbit.None);
        }

        /// <summary>
        /// Verify that JetOSSnapshotGetFreezeInfo throws an exception when using the
        /// XP version of ESENT.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JetOSSnapshotGetFreezeInfo throws an exception when using the XP version of ESENT")]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyXpThrowsExceptionOnJetOSSnapshotGetFreezeInfo()
        {
            int ignored;
            JET_INSTANCE_INFO[] ignored2;
            VistaApi.JetOSSnapshotGetFreezeInfo(JET_OSSNAPID.Nil, out ignored, out ignored2, SnapshotGetFreezeInfoGrbit.None);
        }

        /// <summary>
        /// Verify that JetOSSnapshotTruncateLog throws an exception when using the
        /// XP version of ESENT.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JetOSSnapshotTruncateLog throws an exception when using the XP version of ESENT")]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyXpThrowsExceptionOnJetOSSnapshotTruncateLog()
        {
            VistaApi.JetOSSnapshotTruncateLog(JET_OSSNAPID.Nil, SnapshotTruncateLogGrbit.None);
        }

        /// <summary>
        /// Verify that JetOSSnapshotTruncateLogInstance throws an exception when using the
        /// XP version of ESENT.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JetOSSnapshotTruncateLogInstance throws an exception when using the XP version of ESENT")]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyXpThrowsExceptionOnJetOSSnapshotTruncateLogInstance()
        {
            VistaApi.JetOSSnapshotTruncateLogInstance(JET_OSSNAPID.Nil, JET_INSTANCE.Nil, SnapshotTruncateLogGrbit.None);
        }

        /// <summary>
        /// Verify that JetOSSnapshotEnd throws an exception when using the
        /// XP version of ESENT.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JetOSSnapshotEnd throws an exception when using the XP version of ESENT")]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyXpThrowsExceptionOnJetOSSnapshotEnd()
        {
            VistaApi.JetOSSnapshotEnd(JET_OSSNAPID.Nil, SnapshotEndGrbit.None);
        }

        /// <summary>
        /// Verify that JetOSSnapshotAbort throws an exception when using the
        /// XP version of ESENT.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JetOSSnapshotAbort throws an exception when using the XP version of ESENT")]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyXpThrowsExceptionOnJetOSSnapshotAbort()
        {
            Server2003Api.JetOSSnapshotAbort(JET_OSSNAPID.Nil, SnapshotAbortGrbit.None);
        }

        /// <summary>
        /// Verify that JetUpdate2 throws an exception when using the
        /// XP version of ESENT.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JetUpdate2 throws an exception when using the XP version of ESENT")]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyXpThrowsExceptionOnJetUpdate2()
        {
            int actual;
            Server2003Api.JetUpdate2(JET_SESID.Nil, JET_TABLEID.Nil, null, 0, out actual, UpdateGrbit.None);
        }

        /// <summary>
        /// Verify that JetPrereadKeys throws an exception when using the
        /// XP version of ESENT.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JetPrereadKeys throws an exception when using the XP version of ESENT")]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyXpThrowsExceptionOnJetPrereadKeys()
        {
            int ignored;
            Windows7Api.JetPrereadKeys(JET_SESID.Nil, JET_TABLEID.Nil, null, null, 0, out ignored, PrereadKeysGrbit.Forward);
        }

        /// <summary>
        /// Verify that JetInit3 throws an exception when using the
        /// XP version of ESENT.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JetInit3 throws an exception when using the XP version of ESENT")]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyXpThrowsExceptionOnJetInit3()
        {
            JET_INSTANCE instance = JET_INSTANCE.Nil;
            VistaApi.JetInit3(ref instance, null, InitGrbit.None);
        }

        /// <summary>
        /// Verify that JetGetRecordSize throws an exception when using the
        /// XP version of ESENT.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JetGetRecordSize throws an exception when using the XP version of ESENT")]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyXpThrowsExceptionOnJetGetRecordSize()
        {
            JET_RECSIZE recsize = new JET_RECSIZE();
            VistaApi.JetGetRecordSize(JET_SESID.Nil, JET_TABLEID.Nil, ref recsize, GetRecordSizeGrbit.None);
        }

        /// <summary>
        /// Verify getting the LVChunk size on XP returns a default value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify getting the LVChunk size on XP returns a default value")]
        public void VerifyXpReturnsCorrectLVChunkSize()
        {
            Assert.AreEqual(SystemParameters.DatabasePageSize - 82, SystemParameters.LVChunkSizeMost);
        }

        /// <summary>
        /// Verify getting the cached closed tables system parameter on XP returns 0.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify getting the cached closed tables system parameter on XP returns 0")]
        public void VerifyXpReturns0ForCachedClosedTables()
        {
            using (var instance = new Instance("XPcacheclosedtables"))
            {
                instance.Parameters.CachedClosedTables = 10;
                Assert.AreEqual(0, instance.Parameters.CachedClosedTables);
            }
        }

        /// <summary>
        /// Verify getting the waypoint system parameter on XP returns 0.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify getting the waypoint system parameter on XP returns 0")]
        public void VerifyXpReturns0ForWaypoint()
        {
            using (var instance = new Instance("XPwaypointlatency"))
            {
                instance.Parameters.WaypointLatency = 10;
                Assert.AreEqual(0, instance.Parameters.WaypointLatency);
            }
        }

        /// <summary>
        /// Verify getting the waypoint system parameter on XP returns 0.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify getting the waypoint system parameter on XP returns 0")]
        public void VerifyXpReturnsNullForAlternateRecoveryDirectory()
        {
            using (var instance = new Instance("XPalternaterecovery"))
            {
                instance.Parameters.AlternateDatabaseRecoveryDirectory = @"c:\foo";
                Assert.IsNull(instance.Parameters.AlternateDatabaseRecoveryDirectory);
            }
        }

        /// <summary>
        /// Verify getting the configuration system parameter on XP returns 0.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify getting the configuration system parameter on XP returns 0")]
        public void VerifyXpReturns1ForConfiguration()
        {
            SystemParameters.Configuration = 0;
            Assert.AreEqual(1, SystemParameters.Configuration);
        }

        /// <summary>
        /// Verify getting the enable advanced system parameter on XP returns true.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify getting the enable advanced system parameter on XP returns true")]
        public void VerifyXpReturnsTrueForEnableAdvanced()
        {
            SystemParameters.EnableAdvanced = false;
            Assert.AreEqual(true, SystemParameters.EnableAdvanced);
        }

        /// <summary>
        /// Verify getting the key most system parameter on XP returns 255.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify getting the key most system parameter on XP returns 255")]
        public void VerifyXpReturns255ForKeyMost()
        {
            Assert.AreEqual(255, SystemParameters.KeyMost);
        }

        /// <summary>
        /// Use JetCreateIndex2 on XP to test the compatibility path.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Use JetCreateIndex2 on XP to test the compatibility path")]
        public void CreateIndexesOnXp()
        {
            string directory = SetupHelper.CreateRandomDirectory();
            string database = Path.Combine(directory, "test.db");

            using (var instance = new Instance("XPcreateindexes"))
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
        /// Use JetGetDatabaseFileInfo on XP to test the compatibility path for JET_DBINFOMISC.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Use JetGetDatabaseFileInfo on XP to test the compatibility path")]
        public void GetDatabaseFileInfoOnXp()
        {
            string directory = SetupHelper.CreateRandomDirectory();
            string database = Path.Combine(directory, "test.db");

            using (var instance = new Instance("XPJetGetDatabaseFileInfo"))
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
        /// Use JetGetDatabaseInfo on XP to test the compatibility path for JET_DBINFOMISC.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Use JetGetDatabaseInfo on XP to test the compatibility path")]
        public void GetDatabaseInfoOnXp()
        {
            string directory = SetupHelper.CreateRandomDirectory();
            string database = Path.Combine(directory, "test.db");

            using (var instance = new Instance("XPJetGetDatabaseInfo"))
            {
                SetupHelper.SetLightweightConfiguration(instance);
                instance.Init();
                using (var session = new Session(instance))
                {
                    JET_DBID dbid;
                    Api.JetCreateDatabase(session, database, String.Empty, out dbid, CreateDatabaseGrbit.None);

                    JET_DBINFOMISC dbinfomisc;
                    Api.JetGetDatabaseInfo(session, dbid, out dbinfomisc, JET_DbInfo.Misc);
                    Assert.AreEqual(SystemParameters.DatabasePageSize, dbinfomisc.cbPageSize);
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
        public void CreateTableColumnIndex3OnXp()
        {
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

            using (var instance = new Instance("XpCreateTableColumnIndex3"))
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
        public void CreateBasicTableColumnIndex3OnXp()
        {
            var tablecreate = new JET_TABLECREATE { szTableName = "table" };

            string directory = SetupHelper.CreateRandomDirectory();
            string database = Path.Combine(directory, "test.db");

            using (var instance = new Instance("XpCreateBasicTableColumnIndex3"))
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