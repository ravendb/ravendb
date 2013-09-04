//-----------------------------------------------------------------------
// <copyright file="DatabaseFileTestHelper.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.IO;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Server2003;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.Isam.Esent.Interop.Windows7;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Implementation of a backup/restore/compact/setsize test.
    /// </summary>
    internal class DatabaseFileTestHelper
    {
        /// <summary>
        /// The directory containing the database.
        /// </summary>
        private readonly string databaseDirectory;

        /// <summary>
        /// The path to the database.
        /// </summary>
        private readonly string database;

        /// <summary>
        /// The directory that contains the backup.
        /// </summary>
        private readonly string backupDirectory;

        /// <summary>
        /// True if a status callback should be used.
        /// </summary>
        private readonly bool useStatusCallback;

        /// <summary>
        /// Set by the internal status callback.
        /// </summary>
        private bool statusCallbackWasCalled;

        /// <summary>
        /// Initializes a new instance of the DatabaseFileTestHelper class.
        /// </summary>
        /// <param name="databaseDirectory">
        /// The directory to create a database in.
        /// </param>
        public DatabaseFileTestHelper(string databaseDirectory) : this(databaseDirectory, null, false)
        {
        }

        /// <summary>
        /// Initializes a new instance of the DatabaseFileTestHelper class.
        /// </summary>
        /// <param name="databaseDirectory">
        /// The directory to create a database in.
        /// </param>
        /// <param name="useStatusCallback">
        /// True if a status callback should be used.
        /// </param>
        public DatabaseFileTestHelper(string databaseDirectory, bool useStatusCallback)
            : this(databaseDirectory, null, useStatusCallback)
        {
        }

        /// <summary>
        /// Initializes a new instance of the DatabaseFileTestHelper class.
        /// </summary>
        /// <param name="databaseDirectory">
        /// The directory to create a database in.
        /// </param>
        /// <param name="backupDirectory">
        /// The directory to backup the database to.
        /// </param>
        /// <param name="useStatusCallback">
        /// True if a status callback should be used.
        /// </param>
        public DatabaseFileTestHelper(string databaseDirectory, string backupDirectory, bool useStatusCallback)
        {
            this.databaseDirectory = databaseDirectory;
            this.database = Path.Combine(this.databaseDirectory, "database.edb");
            this.backupDirectory = backupDirectory;
            this.useStatusCallback = useStatusCallback;
        }

        /// <summary>
        /// Create a database, back it up to the backup directory and
        /// then restore it.
        /// </summary>
        public void TestBackupRestore()
        {
            try
            {
                this.CreateDatabase();
                this.BackupDatabase();
                this.DeleteDatabaseFiles();
                this.RestoreDatabase();
                this.CheckDatabase();
            }
            finally
            {
                Cleanup.DeleteDirectoryWithRetry(this.databaseDirectory);
                Cleanup.DeleteDirectoryWithRetry(this.backupDirectory);
            }
        }

        /// <summary>
        /// Create a database and then compact it.
        /// </summary>
        public void TestCompactDatabase()
        {
            try
            {
                this.CreateDatabase();
                this.CompactDatabase();
                this.CheckDatabase();
            }
            finally
            {
                Cleanup.DeleteDirectoryWithRetry(this.databaseDirectory);
            }
        }

        /// <summary>
        /// Backup a database and have the status callback throw
        /// an exception during backup.
        /// </summary>
        /// <param name="ex">
        /// The exception to throw from the callback.
        /// </param>
        public void TestBackupCallbackExceptionHandling(Exception ex)
        {
            try
            {
                this.CreateDatabase();
                this.BackupDatabaseWithCallbackException(ex);
            }
            finally
            {
                Cleanup.DeleteDirectoryWithRetry(this.databaseDirectory);
                Cleanup.DeleteDirectoryWithRetry(this.backupDirectory);
            }
        }

        /// <summary>
        /// Backup a database and have the status callback throw
        /// an exception during restore.
        /// </summary>
        /// <param name="ex">
        /// The exception to throw from the callback.
        /// </param>
        public void TestRestoreCallbackExceptionHandling(Exception ex)
        {
            try
            {
                this.CreateDatabase();
                this.BackupDatabase();
                this.DeleteDatabaseFiles();
                this.RestoreDatabaseWithCallbackException(ex);
            }
            finally
            {
                Cleanup.DeleteDirectoryWithRetry(this.databaseDirectory);
                Cleanup.DeleteDirectoryWithRetry(this.backupDirectory);
            }
        }

        /// <summary>
        /// Create a database and then have the status callback throw an
        /// exception during compaction.
        /// </summary>
        /// <param name="ex">The exception to throw.</param>
        public void TestCompactDatabaseCallbackExceptionHandling(Exception ex)
        {
            try
            {
                this.CreateDatabase();
                this.CompactDatabaseWithCallbackException(ex);
                this.CheckDatabase();
            }
            finally
            {
                Cleanup.DeleteDirectoryWithRetry(this.databaseDirectory);
            }
        }

        /// <summary>
        /// Create a database and then set the size of the database.
        /// </summary>
        public void TestSetDatabaseSize()
        {
            try
            {
                this.CreateDatabase();
                this.SetDatabaseSize();
                this.CheckDatabase();
            }
            finally
            {
                Cleanup.DeleteDirectoryWithRetry(this.databaseDirectory);
            }
        }

        /// <summary>
        /// Create a database and do a snapshot backup.
        /// </summary>
        public void TestSnapshotBackup()
        {
            try
            {
                this.CreateDatabase();
                this.SnapshotBackup();
                this.CheckDatabase();
            }
            finally
            {
                Cleanup.DeleteDirectoryWithRetry(this.databaseDirectory);
            }
        }

        /// <summary>
        /// Create a database and do a snapshot backup using 
        /// functionality available in Server 2K3 onwards.
        /// </summary>
        public void TestSnapshotBackupServer2003()
        {
            if (!EsentVersion.SupportsServer2003Features)
            {
                return;
            }

            try
            {
                this.CreateDatabase();
                this.SnapshotBackupServer2003Apis();
                this.CheckDatabase();
            }
            finally
            {
                Cleanup.DeleteDirectoryWithRetry(this.databaseDirectory);
            }
        }

        /// <summary>
        /// Create a database and do a snapshot backup using 
        /// functionality available in Windows Vista onwards.
        /// </summary>
        public void TestSnapshotBackupVista()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                return;
            }

            try
            {
                this.CreateDatabase();
                this.SnapshotBackupVistaApis();
                this.CheckDatabase();
            }
            finally
            {
                Cleanup.DeleteDirectoryWithRetry(this.databaseDirectory);
            }
        }

        /// <summary>
        /// Create a database and do a recovery to a different
        /// path using  
        /// functionality available in Windows Vista onwards.
        /// </summary>
        public void TestJetInit3()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                return;
            }

            try
            {
                this.CreateDatabase();
                this.RecoverAlternatePathWithJetInit3();
                this.CheckDatabase();
            }
            finally
            {
                Cleanup.DeleteDirectoryWithRetry(this.databaseDirectory);
            }
        }

        /// <summary>
        /// Create a database and do a snapshot backup using 
        /// functionality available in Windows 7 onwards.
        /// </summary>
        public void TestSnapshotBackupWin7()
        {
            if (!EsentVersion.SupportsWindows7Features)
            {
                return;
            }

            try
            {
                this.CreateDatabase();
                this.SnapshotBackupWin7Apis();
                this.CheckDatabase();
            }
            finally
            {
                Cleanup.DeleteDirectoryWithRetry(this.databaseDirectory);
            }
        }

        /// <summary>
        /// Create a database and do a streaming backup.
        /// </summary>
        public void TestStreamingBackup()
        {
            try
            {
                this.CreateDatabase();
                this.StreamingBackup();
                this.CheckDatabase();
            }
            finally
            {
                Cleanup.DeleteDirectoryWithRetry(this.databaseDirectory);
            }
        }

        /// <summary>
        /// Create a database and do a streaming backup.
        /// </summary>
        public void TestStreamingBackup2()
        {
            try
            {
                this.CreateDatabase();
                this.StreamingBackup2();
                this.CheckDatabase();
            }
            finally
            {
                Cleanup.DeleteDirectoryWithRetry(this.databaseDirectory);
            }
        }

        /// <summary>
        /// Create a database and call JetGetDatabaseInfo.
        /// </summary>
        public void TestGetDatabaseInfo()
        {
            try
            {
                this.CreateDatabase();
                this.TestJetGetDatabaseInfo();
            }
            finally
            {
                Cleanup.DeleteDirectoryWithRetry(this.databaseDirectory);
            }
        }

        /// <summary>
        /// Create a database and call JetGetDatabaseFileInfo.
        /// </summary>
        public void TestGetDatabaseFileInfo()
        {
            try
            {
                this.CreateDatabase();
                this.TestJetGetDatabaseFileInfo();
            }
            finally
            {
                Cleanup.DeleteDirectoryWithRetry(this.databaseDirectory);
            }
        }

        /// <summary>
        /// Read a file using the JetReadFileInstance API. A backup should be prepared.
        /// </summary>
        /// <param name="instance">The instance to use.</param>
        /// <param name="file">The file to read.</param>
        private static void ReadFile(JET_INSTANCE instance, string file)
        {
            JET_HANDLE handle;
            long fileSizeLow;
            long fileSizeHigh;
            var buffer = new byte[64 * 1024];
            int bytesRead;

            Api.JetOpenFileInstance(instance, file, out handle, out fileSizeLow, out fileSizeHigh);
            do
            {
                Api.JetReadFileInstance(instance, handle, buffer, buffer.Length, out bytesRead);                
            }
            while (bytesRead > 0);

            Api.JetCloseFileInstance(instance, handle);            
        }

        /// <summary>
        /// Generate some logs. This is used by tests that do backups.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to use to generate the logs.</param>
        private static void GenerateSomeLogs(JET_SESID sesid, JET_DBID dbid)
        {
            if (EsentVersion.SupportsWindows7Features)
            {
                for (int i = 0; i < 10; ++i)
                {
                    Api.JetCommitTransaction(sesid, Windows7Grbits.ForceNewLog);
                }
            }
            else
            {
                using (var transaction = new Transaction(sesid))
                {
                    JET_TABLEID junkTable;
                    Api.JetCreateTable(sesid, dbid, "junk", 1, 100, out junkTable);
                    JET_COLUMNID binaryColumn;
                    Api.JetAddColumn(sesid, junkTable, "column", new JET_COLUMNDEF { coltyp = JET_coltyp.LongBinary }, null, 0, out binaryColumn);

                    byte[] data = new byte[1023];
                    for (int i = 0; i < 256; ++i)
                    {
                        using (var update = new Update(sesid, junkTable, JET_prep.Insert))
                        {
                            Api.JetSetColumn(
                                sesid,
                                junkTable,
                                binaryColumn,
                                data,
                                data.Length,
                                SetColumnGrbit.IntrinsicLV,
                                null);
                            update.SaveAndGotoBookmark();
                        }

                        transaction.Commit(CommitTransactionGrbit.LazyFlush);
                        transaction.Begin();
                    }

                    Api.JetCloseTable(sesid, junkTable);
                    Api.JetDeleteTable(sesid, dbid, "junk");
                    transaction.Commit(CommitTransactionGrbit.LazyFlush);
                }
            }
        }

        /// <summary>
        /// Create the database.
        /// </summary>
        private void CreateDatabase()
        {
            Cleanup.DeleteDirectoryWithRetry(this.databaseDirectory);
            using (var instance = this.CreateInstance())
            {
                instance.Parameters.CreatePathIfNotExist = true;
                instance.Init();
                using (var session = new Session(instance))
                {
                    JET_DBID dbid;
                    Api.JetCreateDatabase(session, this.database, String.Empty, out dbid, CreateDatabaseGrbit.None);
                    using (var transaction = new Transaction(session))
                    {
                        JET_TABLEID tableid;
                        Api.JetCreateTable(session, dbid, "table", 1, 100, out tableid);
                        JET_COLUMNID columnid;
                        Api.JetAddColumn(session, tableid, "column", new JET_COLUMNDEF { coltyp = JET_coltyp.Long }, null, 0, out columnid);
                        using (var update = new Update(session, tableid, JET_prep.Insert))
                        {
                            Api.SetColumn(session, tableid, columnid, 17);
                            update.Save();    
                        }

                        transaction.Commit(CommitTransactionGrbit.None);
                    }
                }
            }
        }

        /// <summary>
        /// Backup the database.
        /// </summary>
        private void BackupDatabase()
        {
            using (var instance = this.CreateInstance())
            {
                instance.Init();
                using (var session = new Session(instance))
                {
                    Api.JetAttachDatabase(session, this.database, AttachDatabaseGrbit.None);
                    JET_DBID dbid;
                    Api.JetOpenDatabase(session, this.database, String.Empty, out dbid, OpenDatabaseGrbit.None);
                    if (this.useStatusCallback)
                    {
                        this.statusCallbackWasCalled = false;
                        Api.JetBackupInstance(instance, this.backupDirectory, BackupGrbit.None, this.StatusCallback);
                        Assert.IsTrue(
                            this.statusCallbackWasCalled, "expected the status callback to be called during backup");
                    }
                    else
                    {
                        Api.JetBackupInstance(instance, this.backupDirectory, BackupGrbit.None, null);                        
                    }
                }
            }
        }

        /// <summary>
        /// Backup the database and have the status callback throw an exception.
        /// </summary>
        /// <param name="ex">
        /// The exception to throw from the callback.
        /// </param>
        private void BackupDatabaseWithCallbackException(Exception ex)
        {
            using (var instance = this.CreateInstance())
            {
                instance.Init();
                using (var session = new Session(instance))
                {
                    Api.JetAttachDatabase(session, this.database, AttachDatabaseGrbit.None);
                    JET_DBID dbid;
                    Api.JetOpenDatabase(session, this.database, String.Empty, out dbid, OpenDatabaseGrbit.None);
                    Api.JetBackupInstance(
                        instance,
                        this.backupDirectory,
                        BackupGrbit.None,
                        (sesid, snt, snp, snprog) =>
                        {
                            throw ex;
                        });
                }
            }
        }

        /// <summary>
        /// Perform a snapshot backup.
        /// </summary>
        private void SnapshotBackup()
        {
            using (var instance = this.CreateInstance())
            {
                instance.Init();
                using (var session = new Session(instance))
                {
                    Api.JetAttachDatabase(session, this.database, AttachDatabaseGrbit.None);
                    JET_DBID dbid;
                    Api.JetOpenDatabase(session, this.database, String.Empty, out dbid, OpenDatabaseGrbit.None);

                    JET_OSSNAPID snapshot;
                    SnapshotPrepareGrbit grbit = EsentVersion.SupportsVistaFeatures
                                                     ? VistaGrbits.ContinueAfterThaw
                                                     : SnapshotPrepareGrbit.None;
                    Api.JetOSSnapshotPrepare(out snapshot, grbit);
                    int numInstances;
                    JET_INSTANCE_INFO[] instances;
                    Api.JetOSSnapshotFreeze(snapshot, out numInstances, out instances, SnapshotFreezeGrbit.None);
                    Api.JetOSSnapshotThaw(snapshot, SnapshotThawGrbit.None);

                    if (EsentVersion.SupportsVistaFeatures)
                    {
                        VistaApi.JetOSSnapshotEnd(snapshot, SnapshotEndGrbit.None);
                    }

                    Assert.AreEqual(1, instances.Length);
                    Assert.AreEqual(1, instances[0].cDatabases);
                    Assert.AreEqual(Path.GetFullPath(this.database), instances[0].szDatabaseFileName[0]);
                }
            }
        }

        /// <summary>
        /// Perform a snapshot backup using the W2K3 abort API.
        /// </summary>
        private void SnapshotBackupServer2003Apis()
        {
            using (var instance = this.CreateInstance())
            {
                instance.Init();
                using (var session = new Session(instance))
                {
                    Api.JetAttachDatabase(session, this.database, AttachDatabaseGrbit.None);
                    JET_DBID dbid;
                    Api.JetOpenDatabase(session, this.database, String.Empty, out dbid, OpenDatabaseGrbit.None);

                    JET_OSSNAPID snapshot;
                    Api.JetOSSnapshotPrepare(out snapshot, SnapshotPrepareGrbit.CopySnapshot);
                    int numInstances;
                    JET_INSTANCE_INFO[] instances;
                    Api.JetOSSnapshotFreeze(snapshot, out numInstances, out instances, SnapshotFreezeGrbit.None);

                    Server2003Api.JetOSSnapshotAbort(snapshot, SnapshotAbortGrbit.None);
                }
            }
        }

        /// <summary>
        /// Recovery to an alternate path with JetInit3.
        /// </summary>
        private void RecoverAlternatePathWithJetInit3()
        {
            using (var instance = this.CreateInstance())
            {
                instance.Init();
                using (var session = new Session(instance))
                {
                    Api.JetAttachDatabase(session, this.database, AttachDatabaseGrbit.None);
                    JET_DBID dbid;
                    Api.JetOpenDatabase(session, this.database, String.Empty, out dbid, OpenDatabaseGrbit.None);
                }
            }

            // Delete the database and checkpoint
            File.Delete(this.database);
            File.Delete(Path.Combine(this.databaseDirectory, "edb.chk"));

            // Recovery to a different database
            string newDatabaseName = this.database + ".moved";
            var recoveryOptions = new JET_RSTINFO
            {
                crstmap = 1,
                pfnStatus = this.StatusCallback,
                rgrstmap = new[]
                {
                    new JET_RSTMAP { szDatabaseName = this.database, szNewDatabaseName = newDatabaseName },
                },
            };

            JET_INSTANCE recoveryInstance;
            Api.JetCreateInstance(out recoveryInstance, "JetInit3");
            Api.JetSetSystemParameter(recoveryInstance, JET_SESID.Nil, JET_param.LogFileSize, 128, null);
            Api.JetSetSystemParameter(recoveryInstance, JET_SESID.Nil, JET_param.LogFilePath, 0, this.databaseDirectory);
            Api.JetSetSystemParameter(recoveryInstance, JET_SESID.Nil, JET_param.TempPath, 0, this.databaseDirectory);
            Api.JetSetSystemParameter(recoveryInstance, JET_SESID.Nil, JET_param.SystemPath, 0, this.databaseDirectory);
            VistaApi.JetInit3(ref recoveryInstance, recoveryOptions, InitGrbit.None);
            Api.JetTerm(recoveryInstance);

            Assert.IsTrue(File.Exists(newDatabaseName), "New database ({0}) doesn't exist", newDatabaseName);
            Assert.IsFalse(File.Exists(this.database), "Old database ({0}) still exists", this.database);
            File.Move(newDatabaseName, this.database);
        }

        /// <summary>
        /// Perform a snapshot backup using the extra Vista APIs.
        /// </summary>
        private void SnapshotBackupVistaApis()
        {
            using (var instance = this.CreateInstance())
            {
                instance.Init();
                using (var session = new Session(instance))
                {
                    Api.JetAttachDatabase(session, this.database, AttachDatabaseGrbit.None);
                    JET_DBID dbid;
                    Api.JetOpenDatabase(session, this.database, String.Empty, out dbid, OpenDatabaseGrbit.None);

                    // Prepare
                    JET_OSSNAPID snapshot;
                    Api.JetOSSnapshotPrepare(out snapshot, VistaGrbits.ContinueAfterThaw);

                    int numInstances;
                    JET_INSTANCE_INFO[] instances;

                    // Freeze
                    Api.JetOSSnapshotFreeze(snapshot, out numInstances, out instances, SnapshotFreezeGrbit.None);
                    Assert.AreEqual(1, instances.Length);
                    Assert.AreEqual(1, instances[0].cDatabases);
                    Assert.AreEqual(Path.GetFullPath(this.database), instances[0].szDatabaseFileName[0]);

                    // GetFreezeInfo
                    VistaApi.JetOSSnapshotGetFreezeInfo(snapshot, out numInstances, out instances, SnapshotGetFreezeInfoGrbit.None);
                    Assert.AreEqual(1, instances.Length);
                    Assert.AreEqual(1, instances[0].cDatabases);
                    Assert.AreEqual(Path.GetFullPath(this.database), instances[0].szDatabaseFileName[0]);

                    // Thaw
                    Api.JetOSSnapshotThaw(snapshot, SnapshotThawGrbit.None);

                    // Truncate log
                    VistaApi.JetOSSnapshotTruncateLog(snapshot, SnapshotTruncateLogGrbit.AllDatabasesSnapshot);

                    // End
                    VistaApi.JetOSSnapshotEnd(snapshot, SnapshotEndGrbit.None);
                }
            }
        }

        /// <summary>
        /// Perform a snapshot backup using the extra Windows7 grbits.
        /// </summary>
        private void SnapshotBackupWin7Apis()
        {
            using (var instance = this.CreateInstance())
            {
                instance.Init();
                using (var session = new Session(instance))
                {
                    Api.JetAttachDatabase(session, this.database, AttachDatabaseGrbit.None);
                    JET_DBID dbid;
                    Api.JetOpenDatabase(session, this.database, String.Empty, out dbid, OpenDatabaseGrbit.None);

                    // Prepare
                    JET_OSSNAPID snapshot;
                    Api.JetOSSnapshotPrepare(out snapshot, VistaGrbits.ContinueAfterThaw | Windows7Grbits.ExplicitPrepare);

                    // Prepare instance
                    VistaApi.JetOSSnapshotPrepareInstance(snapshot, instance, SnapshotPrepareInstanceGrbit.None);

                    int numInstances;
                    JET_INSTANCE_INFO[] instances;

                    // Freeze
                    Api.JetOSSnapshotFreeze(snapshot, out numInstances, out instances, SnapshotFreezeGrbit.None);
                    Assert.AreEqual(1, instances.Length);
                    Assert.AreEqual(1, instances[0].cDatabases);
                    Assert.AreEqual(Path.GetFullPath(this.database), instances[0].szDatabaseFileName[0]);

                    // GetFreezeInfo
                    VistaApi.JetOSSnapshotGetFreezeInfo(snapshot, out numInstances, out instances, SnapshotGetFreezeInfoGrbit.None);
                    Assert.AreEqual(1, instances.Length);
                    Assert.AreEqual(1, instances[0].cDatabases);
                    Assert.AreEqual(Path.GetFullPath(this.database), instances[0].szDatabaseFileName[0]);

                    // Thaw
                    Api.JetOSSnapshotThaw(snapshot, SnapshotThawGrbit.None);

                    // Truncate log instance
                    VistaApi.JetOSSnapshotTruncateLogInstance(snapshot, instance, SnapshotTruncateLogGrbit.AllDatabasesSnapshot);

                    // End
                    VistaApi.JetOSSnapshotEnd(snapshot, SnapshotEndGrbit.None);
                }
            }
        }

        /// <summary>
        /// Perform a streaming backup.
        /// </summary>
        private void StreamingBackup()
        {
            using (var instance = this.CreateInstance())
            {
                instance.Init();
                using (var session = new Session(instance))
                {
                    Api.JetAttachDatabase(session, this.database, AttachDatabaseGrbit.None);
                    JET_DBID dbid;
                    Api.JetOpenDatabase(session, this.database, String.Empty, out dbid, OpenDatabaseGrbit.None);

                    // Roll some logs so that we get back strings with embedded nulls.
                    GenerateSomeLogs(session, dbid);

                    // BeginExternalBackup
                    Api.JetBeginExternalBackupInstance(instance, BeginExternalBackupGrbit.None);

                    string filelist;
                    int actualChars;
                    string[] files;

                    // Get list of databases
                    Api.JetGetAttachInfoInstance(instance, out filelist, 4096, out actualChars);

                    // The string length doesn't include the double-null terminator
                    Assert.AreEqual(actualChars, filelist.Length, "actualChars doesn't give the length of filelist");
                    files = filelist.Split(new[] { '\0' }, StringSplitOptions.RemoveEmptyEntries);
                    Assert.AreEqual(1, files.Length, "Expected just one database");
                    StringAssert.Contains(files[0], this.database, "File list didn't contain the database");

                    // Backup the database
                    ReadFile(instance, this.database);

                    // Get list of logs
                    Api.JetGetLogInfoInstance(instance, out filelist, 4096, out actualChars);

                    // The string length doesn't include the double-null terminator
                    Assert.AreEqual(actualChars, filelist.Length, "actualChars doesn't give the length of filelist");
                    files = filelist.Split(new[] { '\0' }, StringSplitOptions.RemoveEmptyEntries);
                    Assert.AreNotEqual(0, files.Length, "Expected at least one log");

                    // Backup the logs
                    foreach (string log in files)
                    {
                        ReadFile(instance, log);
                    }

                    // Get list of logs to truncate (may be empty)
                    Api.JetGetTruncateLogInfoInstance(instance, out filelist, 4096, out actualChars);
                    Assert.AreEqual(actualChars, filelist.Length, "actualChars doesn't give the length of filelist");

                    // Truncate logs
                    Api.JetTruncateLogInstance(instance);

                    Api.JetEndExternalBackupInstance(instance);
                }
            }
        }

        /// <summary>
        /// Perform a streaming backup and use JetEndExternalBackupInstance2.
        /// </summary>
        private void StreamingBackup2()
        {
            using (var instance = this.CreateInstance())
            {
                instance.Init();
                using (var session = new Session(instance))
                {
                    Api.JetAttachDatabase(session, this.database, AttachDatabaseGrbit.None);
                    JET_DBID dbid;
                    Api.JetOpenDatabase(session, this.database, String.Empty, out dbid, OpenDatabaseGrbit.None);

                    Api.JetBeginExternalBackupInstance(instance, BeginExternalBackupGrbit.None);
                    JET_HANDLE handle;
                    long fileSizeLow;
                    long fileSizeHigh;
                    Api.JetOpenFileInstance(instance, this.database, out handle, out fileSizeLow, out fileSizeHigh);
                    var buffer = new byte[64 * 1024];
                    int bytesRead;
                    Api.JetReadFileInstance(instance, handle, buffer, buffer.Length, out bytesRead);
                    Api.JetCloseFileInstance(instance, handle);
                    Api.JetEndExternalBackupInstance2(instance, EndExternalBackupGrbit.Normal);
                }
            }
        }

        /// <summary>
        /// Retrieves various pieces of information with JetGetDatabaseFileInfo.
        /// </summary>
        private void TestJetGetDatabaseFileInfo()
        {
            long databaseFileSizeBytes;
            Api.JetGetDatabaseFileInfo(this.database, out databaseFileSizeBytes, JET_DbInfo.Filesize);
            Assert.AreNotEqual(0, databaseFileSizeBytes);

            int databaseFileInUse;
            Api.JetGetDatabaseFileInfo(this.database, out databaseFileInUse, JET_DbInfo.DBInUse);
            Assert.AreEqual(0, databaseFileInUse);

            int databaseFilePageSize;
            Api.JetGetDatabaseFileInfo(this.database, out databaseFilePageSize, JET_DbInfo.PageSize);
            Assert.AreEqual(SystemParameters.DatabasePageSize, databaseFilePageSize);

            JET_DBINFOMISC dbinfomisc;
            Api.JetGetDatabaseFileInfo(this.database, out dbinfomisc, JET_DbInfo.Misc);
            Assert.AreEqual(SystemParameters.DatabasePageSize, dbinfomisc.cbPageSize);
        }

        /// <summary>
        /// Retrieves various pieces of information with JetGetDatabaseInfo.
        /// </summary>
        private void TestJetGetDatabaseInfo()
        {
            using (var instance = this.CreateInstance())
            {
                instance.Init();
                using (var session = new Session(instance))
                {
                    // Attach the database.
                    Api.JetAttachDatabase(session, this.database, AttachDatabaseGrbit.None);
                    JET_DBID dbid;
                    Api.JetOpenDatabase(session, this.database, String.Empty, out dbid, OpenDatabaseGrbit.None);

                    int databaseSizePages;
                    Api.JetGetDatabaseInfo(session, dbid, out databaseSizePages, JET_DbInfo.Filesize);
                    Assert.AreNotEqual(0, databaseSizePages);

                    int databaseLcid;
                    Api.JetGetDatabaseInfo(session, dbid, out databaseLcid, JET_DbInfo.LCID);
                    Console.WriteLine("databaseLcid is {0}", databaseLcid);
                    Assert.AreEqual(1033, databaseLcid);

                    int databaseOptions;
                    Api.JetGetDatabaseInfo(session, dbid, out databaseOptions, JET_DbInfo.Options);
                    Console.WriteLine("databaseOptions is {0}", databaseOptions);
                    Assert.AreEqual(0, databaseOptions);

                    int databaseTransactions;
                    Api.JetGetDatabaseInfo(session, dbid, out databaseTransactions, JET_DbInfo.Transactions);
                    Console.WriteLine("databaseTransactions is {0}", databaseTransactions);
                    Assert.AreEqual(7, databaseTransactions);

                    int databaseVersion;
                    Api.JetGetDatabaseInfo(session, dbid, out databaseVersion, JET_DbInfo.Version);
                    Console.WriteLine("databaseVersion is {0}", databaseVersion);
                    Assert.AreNotEqual(0, databaseVersion);

                    int databaseSpaceOwned;
                    Api.JetGetDatabaseInfo(session, dbid, out databaseSpaceOwned, JET_DbInfo.SpaceOwned);
                    Console.WriteLine("databaseSpaceOwned is {0}", databaseSpaceOwned);
                    Assert.AreNotEqual(0, databaseSpaceOwned);

                    int databaseSpaceAvailable;
                    Api.JetGetDatabaseInfo(session, dbid, out databaseSpaceAvailable, JET_DbInfo.SpaceAvailable);
                    Console.WriteLine("databaseSpaceAvailable is {0}", databaseSpaceAvailable);
                    Assert.AreNotEqual(0, databaseSpaceAvailable);

                    int databasePageSize;
                    Api.JetGetDatabaseInfo(session, dbid, out databasePageSize, JET_DbInfo.PageSize);
                    Assert.AreEqual(SystemParameters.DatabasePageSize, databasePageSize);

                    JET_DBINFOMISC dbinfomisc;
                    Api.JetGetDatabaseInfo(session, dbid, out dbinfomisc, JET_DbInfo.Misc);
                    Assert.AreEqual(SystemParameters.DatabasePageSize, dbinfomisc.cbPageSize);

                    string path;
                    Api.JetGetDatabaseInfo(session, dbid, out path, JET_DbInfo.Filename);
                    Assert.AreEqual(Path.GetFullPath(this.database), path);
                }
            }
        }

        /// <summary>
        /// Delete the database files from the database directory.
        /// </summary>
        private void DeleteDatabaseFiles()
        {            
            Cleanup.DeleteDirectoryWithRetry(this.databaseDirectory);
            Directory.CreateDirectory(this.databaseDirectory);
        }

        /// <summary>
        /// Restore the database files.
        /// </summary>
        private void RestoreDatabase()
        {
            using (var instance = this.CreateInstance())
            {
                if (this.useStatusCallback)
                {
                    this.statusCallbackWasCalled = false;
                    Api.JetRestoreInstance(instance, this.backupDirectory, null, this.StatusCallback);
                    Assert.IsTrue(
                        this.statusCallbackWasCalled, "expected the status callback to be called during restore");
                }
                else
                {
                    Api.JetRestoreInstance(instance, this.backupDirectory, null, null);
                }
            }
        }

        /// <summary>
        /// Restore the database files and have the status callback throw an exception.
        /// </summary>
        /// <param name="ex">The exception to throw from the status callback.</param>
        private void RestoreDatabaseWithCallbackException(Exception ex)
        {
            using (var instance = this.CreateInstance())
            {
                Api.JetRestoreInstance(
                    instance,
                    this.backupDirectory,
                    this.databaseDirectory,
                    (sesid, snt, snp, snprog) =>
                    {
                        throw ex;
                    });
            }
        }

        /// <summary>
        /// Compact the database.
        /// </summary>
        private void CompactDatabase()
        {
            string defraggedDatabase = Path.Combine(this.databaseDirectory, "defragged.edb");
            using (var instance = this.CreateInstance())
            {
                instance.Init();
                using (var session = new Session(instance))
                {
                    // For JetCompact to work the database has to be attached, but not opened
                    Api.JetAttachDatabase(session, this.database, AttachDatabaseGrbit.None);
                    if (this.useStatusCallback)
                    {
                        this.statusCallbackWasCalled = false;
                        Api.JetCompact(session, this.database, defraggedDatabase, this.StatusCallback, null, CompactGrbit.None);
                        Assert.IsTrue(
                            this.statusCallbackWasCalled, "expected the status callback to be called during compact");
                    }
                    else
                    {
                        Api.JetCompact(session, this.database, defraggedDatabase, null, null, CompactGrbit.None);
                    }
                }
            }

            Assert.IsTrue(File.Exists(defraggedDatabase));
            Cleanup.DeleteFileWithRetry(this.database);
            File.Move(defraggedDatabase, this.database);
        }

        /// <summary>
        /// Compact the database and have the status callback throw an exception.
        /// </summary>
        /// <param name="ex">The exception to throw.</param>
        private void CompactDatabaseWithCallbackException(Exception ex)
        {
            using (var instance = this.CreateInstance())
            {
                instance.Init();
                using (var session = new Session(instance))
                {
                    // For JetCompact to work the database has to be attached, but not opened
                    Api.JetAttachDatabase(session, this.database, AttachDatabaseGrbit.None);
                    Api.JetCompact(
                        session,
                        this.database,
                        this.database,
                        (sesid, snt, snp, snprog) =>
                        {
                            throw ex;
                        },
                        null,
                        CompactGrbit.None);
                }
            }
        }

        /// <summary>
        /// Set the database's size.
        /// </summary>
        private void SetDatabaseSize()
        {
            using (var instance = this.CreateInstance())
            {
                instance.Init();
                using (var session = new Session(instance))
                {
                    const int ExpectedPages = 512;
                    int actualPages;

                    // BUG: this seems to have problems (JET_err.InvalidParameter) on Vista and below
                    Api.JetSetDatabaseSize(session, this.database, ExpectedPages, out actualPages);
                    Assert.IsTrue(actualPages >= ExpectedPages, "Database isn't large enough");
                }
            }
        }

        /// <summary>
        /// Check the database files have been restored.
        /// </summary>
        private void CheckDatabase()
        {
            using (var instance = this.CreateInstance())
            {
                instance.Init();
                using (var session = new Session(instance))
                {
                    Api.JetAttachDatabase(session, this.database, AttachDatabaseGrbit.ReadOnly);
                    JET_DBID dbid;
                    Api.JetOpenDatabase(session, this.database, String.Empty, out dbid, OpenDatabaseGrbit.ReadOnly);

                    JET_TABLEID tableid;
                    Api.JetOpenTable(session, dbid, "table", null, 0, OpenTableGrbit.ReadOnly, out tableid);
                    JET_COLUMNID columnid = Api.GetTableColumnid(session, tableid, "column");
                    Assert.IsTrue(Api.TryMoveFirst(session, tableid));
                    Assert.AreEqual(17, Api.RetrieveColumnAsInt32(session, tableid, columnid));
                }
            }
        }

        /// <summary>
        /// Create a new instance, setting the appropriate system parameters.
        /// </summary>
        /// <returns>A new instance.</returns>
        private Instance CreateInstance()
        {
            Guid guid = Guid.NewGuid();
            var instance = new Instance(guid.ToString(), "DatabaseFileTests_" + guid.ToString());
            instance.Parameters.LogFileSize = 128;
            instance.Parameters.LogFileDirectory = this.databaseDirectory;
            instance.Parameters.TempDirectory = this.databaseDirectory;
            instance.Parameters.SystemDirectory = this.databaseDirectory;
            instance.Parameters.CreatePathIfNotExist = true;
            instance.Parameters.NoInformationEvent = true;
            instance.Parameters.CheckpointDepthMax = 512 * 1024;
            return instance;
        }

        /// <summary>
        /// Progress reporting callback.
        /// </summary>
        /// <param name="sesid">The session performing the operation.</param>
        /// <param name="snp">The operation type.</param>
        /// <param name="snt">The type of the progress report.</param>
        /// <param name="data">Progress info.</param>
        /// <returns>An error code.</returns>
        private JET_err StatusCallback(JET_SESID sesid, JET_SNP snp, JET_SNT snt, object data)
        {
            this.statusCallbackWasCalled = true;

            if (JET_SNT.Progress == snt)
            {
                if (data as JET_SNPROG == null)
                {
                    Assert.Inconclusive(
                    "Not all cases in CallbackDataConverter.GetManagedData() have been implemented. snp={0},snt={1}",
                    snp,
                    snt);
                }

                var snprog = data as JET_SNPROG;
                Assert.IsNotNull(snprog, "Expected an snprog in a progress callback");
                Assert.IsTrue(snprog.cunitDone <= snprog.cunitTotal, "done > total in the snprog");
            }

            return JET_err.Success;
        }
    }
}