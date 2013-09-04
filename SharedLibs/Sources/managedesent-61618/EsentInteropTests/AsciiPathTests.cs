//-----------------------------------------------------------------------
// <copyright file="AsciiPathTests.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.IO;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Implementation;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test files with ASCII paths (forcing the version to the XP version)
    /// </summary>
    [TestClass]
    public class AsciiPathTests
    {
        /// <summary>
        /// The directory used for the database and logfiles.
        /// </summary>
        private string directory;

        /// <summary>
        /// The name of the database.
        /// </summary>
        private string database;

        /// <summary>
        /// The saved API, restored after the test.
        /// </summary>
        private IJetApi savedImpl;

        #region Setup/Teardown

        /// <summary>
        /// Test setup
        /// </summary>
        [TestInitialize]
        [Description("Initialization for AsciiPathTests")]
        public void Setup()
        {
            this.directory = "ascii_directory";
            Cleanup.DeleteDirectoryWithRetry(this.directory);
            this.database = Path.Combine(this.directory, "ascii.edb");
            this.savedImpl = Api.Impl;
            Api.Impl = new JetApi(Constants.XpVersion);
        }

        /// <summary>
        /// Restore the default implementation and delete the test
        /// directory, if it was created.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup for AsciiPathTests")]
        public void Teardown()
        {
            Api.Impl = this.savedImpl;
            Cleanup.DeleteDirectoryWithRetry(this.directory);
            SetupHelper.CheckProcessForInstanceLeaks();
        }

        #endregion

        /// <summary>
        /// Set and retrieve an ASCII system path.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Set and retrieve an ASCII system path")]
        public void SetAndGetAsciiSystemPath()
        {
            using (var instance = new Instance("asciisystempath"))
            {
                instance.Parameters.SystemDirectory = this.directory;
                StringAssert.Contains(instance.Parameters.SystemDirectory, this.directory);
            }
        }

        /// <summary>
        /// Set and retrieve an ASCII log path.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Set and retrieve an ASCII log path")]
        public void SetAndGetAsciiLogPath()
        {
            using (var instance = new Instance("asciilogpath"))
            {
                instance.Parameters.LogFileDirectory = this.directory;
                StringAssert.Contains(instance.Parameters.LogFileDirectory, this.directory);
            }
        }

        /// <summary>
        /// Set and retrieve an ASCII temporary directory.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Set and retrieve an ASCII temporary directory")]
        public void SetAndGetAsciiTempDbPath()
        {
            using (var instance = new Instance("asciitempdir"))
            {
                instance.Parameters.TempDirectory = this.directory;
                StringAssert.Contains(instance.Parameters.TempDirectory, this.directory);
            }
        }

        /// <summary>
        /// Create a database with an ASCII path.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Create a database with an ASCII path")]
        public void CreateDatabaseWithAsciiPath()
        {
            using (var instance = new Instance("asciidbcreate"))
            {
                SetupHelper.SetLightweightConfiguration(instance);
                instance.Parameters.CreatePathIfNotExist = true;
                instance.Init();
                using (var session = new Session(instance))
                {
                    JET_DBID dbid;
                    Api.JetCreateDatabase(session, this.database, String.Empty, out dbid, CreateDatabaseGrbit.None);
                    Assert.IsTrue(File.Exists(this.database));
                }
            }
        }

        /// <summary>
        /// Create a database with an ASCII path.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Call JetCreateDatabase2 with an ASCII path")]
        public void CreateDatabase2WithAsciiPath()
        {
            using (var instance = new Instance("asciidbcreate"))
            {
                SetupHelper.SetLightweightConfiguration(instance);
                instance.Parameters.CreatePathIfNotExist = true;
                instance.Init();
                using (var session = new Session(instance))
                {
                    JET_DBID dbid;
                    Api.JetCreateDatabase2(session, this.database, 1024, out dbid, CreateDatabaseGrbit.None);
                    Assert.IsTrue(File.Exists(this.database));
                }
            }
        }

        /// <summary>
        /// Detach a database with an ASCII path.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Detach a database with an ASCII path")]
        public void DetachDatabaseWithAsciiPath()
        {
            using (var instance = new Instance("asciidbdetach"))
            {
                SetupHelper.SetLightweightConfiguration(instance);
                instance.Parameters.CreatePathIfNotExist = true;
                instance.Init();
                using (var session = new Session(instance))
                {
                    JET_DBID dbid;
                    Api.JetCreateDatabase(session, this.database, String.Empty, out dbid, CreateDatabaseGrbit.None);
                    Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
                    Api.JetDetachDatabase(session, this.database);
                }
            }
        }

        /// <summary>
        /// Attach a database with an ASCII path.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Attach a database with an ASCII path")]
        public void AttachDatabaseWithAsciiPath()
        {
            using (var instance = new Instance("asciidbattach"))
            {
                SetupHelper.SetLightweightConfiguration(instance);
                instance.Parameters.CreatePathIfNotExist = true;
                instance.Init();
                using (var session = new Session(instance))
                {
                    JET_DBID dbid;
                    Api.JetCreateDatabase(session, this.database, String.Empty, out dbid, CreateDatabaseGrbit.None);
                    Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
                    Api.JetDetachDatabase(session, this.database);

                    Api.JetAttachDatabase(session, this.database, AttachDatabaseGrbit.None);
                }
            }
        }

        /// <summary>
        /// Use JetAttachDatabase2 on a database with an ASCII path.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Use JetAttachDatabase2 on a database with an ASCII path")]
        public void AttachDatabaseWithAsciiPath2()
        {
            using (var instance = new Instance("asciidbattach2"))
            {
                SetupHelper.SetLightweightConfiguration(instance);
                instance.Parameters.CreatePathIfNotExist = true;
                instance.Init();
                using (var session = new Session(instance))
                {
                    JET_DBID dbid;
                    Api.JetCreateDatabase(session, this.database, String.Empty, out dbid, CreateDatabaseGrbit.None);
                    Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
                    Api.JetDetachDatabase(session, this.database);

                    Api.JetAttachDatabase2(session, this.database, 512, AttachDatabaseGrbit.None);
                }
            }
        }

        /// <summary>
        /// Open a database with an ASCII path.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Open a database with an ASCII path")]
        public void OpenDatabaseWithAsciiPath()
        {
            using (var instance = new Instance("asciidbopen"))
            {
                SetupHelper.SetLightweightConfiguration(instance);
                instance.Parameters.CreatePathIfNotExist = true;
                instance.Init();
                using (var session = new Session(instance))
                {
                    JET_DBID dbid;
                    Api.JetCreateDatabase(session, this.database, String.Empty, out dbid, CreateDatabaseGrbit.None);
                    Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
                    Api.JetDetachDatabase(session, this.database);

                    Api.JetAttachDatabase(session, this.database, AttachDatabaseGrbit.None);
                    Api.JetOpenDatabase(session, this.database, String.Empty, out dbid, OpenDatabaseGrbit.None);
                }
            }
        }

        /// <summary>
        /// Test JetBackupInstance and JetRestoreInstance with an ASCII path.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetBackupInstance and JetRestoreInstance with an ASCII path")]
        public void BackupRestoreDatabaseWithAsciiPath()
        {
            var test = new DatabaseFileTestHelper("database", "backup", false);
            test.TestBackupRestore();
        }

        /// <summary>
        /// Test JetBackupInstance and JetRestoreInstance with an ASCII path and a status callback.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetBackupInstance and JetRestoreInstance with an ASCII path and a status callback")]
        public void BackupRestoreDatabaseWithAsciiPathCallback()
        {
            var test = new DatabaseFileTestHelper("database", "backup", true);
            test.TestBackupRestore();
        }

        /// <summary>
        /// Test snapshot backups with an ASCII path.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test snapshot backups with an ASCII path")]
        public void SnapshotBackupWithAsciiPath()
        {
            var test = new DatabaseFileTestHelper("database");
            test.TestSnapshotBackup();
        }

        /// <summary>
        /// Test streaming backups with an ASCII path.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test streaming backups with an ASCII path")]
        public void StreamingBackupWithAsciiPath()
        {
            var test = new DatabaseFileTestHelper("database", "backup", false);
            test.TestStreamingBackup();
        }

        /// <summary>
        /// Test streaming backups with an ASCII path and JetEndExternalBackup2.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test streaming backups with an ASCII path and JetEndExternalBackup2")]
        public void StreamingBackupWithAsciiPathEndExternalBackup2()
        {
            var test = new DatabaseFileTestHelper("database", "backup", false);
            test.TestStreamingBackup2();
        }

        /// <summary>
        /// Test JetCompactDatabase with an ASCII path.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetCompactDatabase with an ASCII path")]
        public void TestJetCompactDatabaseWithAsciiPath()
        {
            var test = new DatabaseFileTestHelper("database");
            test.TestCompactDatabase();
        }

        /// <summary>
        /// Test JetSetDatabaseSize with an ASCII path.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetSetDatabaseSize with an ASCII path")]
        public void TestJetSetDatabaseSizeDatabaseWithAsciiPath()
        {
            var test = new DatabaseFileTestHelper("database");
            test.TestSetDatabaseSize();
        }

        /// <summary>
        /// Test JetGetDatabaseFileInfo with an ASCII path.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetGetDatabaseFileInfo with an ASCII path.")]
        public void TestJetGetDatabaseFileInfoWithAsciiPath()
        {
            var test = new DatabaseFileTestHelper(this.directory);
            test.TestGetDatabaseFileInfo();
        }

        /// <summary>
        /// Test JetGetDatabaseInfo with an ASCII path.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetGetDatabaseInfo with an ASCII path.")]
        public void TestJetGetDatabaseInfoWithAsciiPath()
        {
            var test = new DatabaseFileTestHelper("database");
            test.TestGetDatabaseInfo();
        }

        /// <summary>
        /// Test JetGetInstanceInfo with ASCII paths.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetGetInstanceInfo with ASCII paths")]
        public void TestJetGetInstanceInfoWithAsciiPaths()
        {
            const string InstanceName = "AsciiGetInstanceInfo";
            string database1 = Path.GetFullPath(Path.Combine(this.directory, "instanceinfo1.edb"));
            string database2 = Path.GetFullPath(Path.Combine(this.directory, "instanceinfo2.edb"));
            using (var instance = new Instance(InstanceName))
            {
                // Don't turn off logging -- JetGetInstanceInfo only returns information for
                // databases that have logging on.
                instance.Parameters.LogFileSize = 256; // 256Kb
                instance.Parameters.NoInformationEvent = true;
                instance.Parameters.MaxTemporaryTables = 0;
                instance.Parameters.CreatePathIfNotExist = true;
                instance.Parameters.LogFileDirectory = this.directory;
                instance.Parameters.SystemDirectory = this.directory;
                instance.Init();
                using (var session = new Session(instance))
                {
                    JET_DBID dbid;
                    Api.JetCreateDatabase(session, database1, String.Empty, out dbid, CreateDatabaseGrbit.None);
                    Api.JetCreateDatabase(session, database2, String.Empty, out dbid, CreateDatabaseGrbit.None);
                    int numInstances;
                    JET_INSTANCE_INFO[] instances;
                    Api.JetGetInstanceInfo(out numInstances, out instances);

                    Assert.AreEqual(1, numInstances);
                    Assert.AreEqual(numInstances, instances.Length);
                    Assert.AreEqual(InstanceName, instances[0].szInstanceName);

                    Assert.AreEqual(2, instances[0].cDatabases);
                    Assert.AreEqual(instances[0].cDatabases, instances[0].szDatabaseFileName.Count);
                    Assert.AreEqual(instances[0].szDatabaseFileName[0], database1);
                    Assert.AreEqual(instances[0].szDatabaseFileName[1], database2);
                }
            }
        }
    }
}