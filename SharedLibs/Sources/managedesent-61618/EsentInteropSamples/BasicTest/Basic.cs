//-----------------------------------------------------------------------
// <copyright file="Basic.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

// Test cases to consider adding:
//
//  Hierarchical DDL
//  Multi-values
//      - set unique
//      - indexed
//  Revert to default
//  GetInfo calls
//  GetDatabasePages/GetDatabasePageInfo
//  GetRecordPosition/GotoRecordPosition
//  GetLock
//  Retrieve column at non-zero offset

namespace BasicTest
{
    using System;
    using System.IO;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Windows7;

    /// <summary>
    /// Basic test of Esent functionality.
    /// </summary>
    internal static class BasicClass
    {
        /// <summary>
        /// The basename for the logfiles.
        /// </summary>
        private const string Basename = "E00";

        /// <summary>
        /// The path the files will be in.
        /// </summary>
        private const string DirectoryPath = "Basic";

        /// <summary>
        /// The path of the database.
        /// </summary>
        private const string Database = @"Basic\Database.edb";

        /// <summary>
        /// The location of the checkpoint file.
        /// </summary>
        private const string Checkpoint = @"Basic\" + Basename + @".chk";

        /// <summary>
        /// Assert method that works in debug and retail.
        /// </summary>
        /// <param name="condition">The condition to check.</param>
        /// <param name="message">The error message.</param>
        /// <param name="args">Arguments for message formatting.</param>
        public static void Assert(bool condition, string message, params object[] args)
        {
            if (!condition)
            {
                if (null != args)
                {
                    message = String.Format(message, args);
                }

                throw new Exception(message);
            }
        }

        /// <summary>
        /// Run the basic test.
        /// </summary>
        public static void Main()
        {
            const string Table = "testtable";

            DateTime startTime = DateTime.Now;

            CreateDirectory(DirectoryPath);

            const int CacheSizeInBytes = 32 * 1024 * 0124;
            SystemParameters.DatabasePageSize = 8192;
            SystemParameters.CacheSizeMin = CacheSizeInBytes / SystemParameters.DatabasePageSize;
            SystemParameters.CacheSizeMax = CacheSizeInBytes / SystemParameters.DatabasePageSize;

            // Create an instance, session and database
            JET_INSTANCE instance = CreateInstance();
            JET_SESID sesid;
            JET_DBID dbid;
            Api.JetBeginSession(instance, out sesid, null, null);
            Api.JetCreateDatabase(sesid, Database, null, out dbid, CreateDatabaseGrbit.None);
            Api.JetCloseDatabase(sesid, dbid, CloseDatabaseGrbit.None);
            Api.JetDetachDatabase(sesid, Database);
            Api.JetAttachDatabase(sesid, Database, AttachDatabaseGrbit.None);
            Api.JetOpenDatabase(sesid, Database, null, out dbid, OpenDatabaseGrbit.None);

            // DDL creation
            // Creates the tables/columns/indexes used by the DDLTest
            var ddltests = new DdlTests(sesid, dbid, Table);
            ddltests.Create();

            // DMLTests
            // Create/Retrieve/Update/Delete records
            var dmltests = new DmlTests(instance, Database, Table);
            dmltests.Create();
            dmltests.Term();

            // DDL update
            // Delete a column/index and add a new index
            ddltests.Update();

            // Temp table tests
            var temptabletests = new TempTableTests(instance);
            temptabletests.Run();

            // Run recovery
            Api.JetStopServiceInstance(instance);
            Api.JetEndSession(sesid, EndSessionGrbit.None);
            try
            {
                Api.JetTerm2(instance, Windows7Grbits.Dirty);
            }
            catch (EsentDirtyShutdownException)
            {
            }

            RecoveryTests(Table);

            Directory.Delete(DirectoryPath, true);

            DateTime endTime = DateTime.Now;
            TimeSpan timespan = endTime - startTime;
            Console.WriteLine("Test completed in {0}", timespan);
        }

        /// <summary>
        /// Create a new instance.
        /// </summary>
        /// <returns>A new initialized instance.</returns>
        private static JET_INSTANCE CreateInstance()
        {
            JET_INSTANCE instance;
            Api.JetCreateInstance(out instance, "BasicInstance");
            Api.JetSetSystemParameter(instance, JET_SESID.Nil, JET_param.LogFileSize, 1 * 1024, null);
            Api.JetSetSystemParameter(instance, JET_SESID.Nil, JET_param.SystemPath, 0, DirectoryPath);
            Api.JetSetSystemParameter(instance, JET_SESID.Nil, JET_param.LogFilePath, 0, DirectoryPath);
            Api.JetSetSystemParameter(instance, JET_SESID.Nil, JET_param.TempPath, 0, DirectoryPath);
            Api.JetSetSystemParameter(instance, JET_SESID.Nil, JET_param.BaseName, 0, Basename);
            Api.JetSetSystemParameter(instance, JET_SESID.Nil, Windows7Param.WaypointLatency, 1, null);
            Api.JetInit(ref instance);
            return instance;
        }

        /// <summary>
        /// Run the recovery tests.
        /// </summary>
        /// <param name="table">The name of the table to check.</param>
        private static void RecoveryTests(string table)
        {
            JET_SESID sesid;
            JET_DBID dbid;

            Console.WriteLine("Recovery tests");

            Console.WriteLine("\tCrash recovery");
            JET_INSTANCE instance = CreateInstance();
            Api.JetBeginSession(instance, out sesid, null, null);
            Api.JetAttachDatabase(sesid, Database, AttachDatabaseGrbit.None);
            Api.JetOpenDatabase(sesid, Database, null, out dbid, OpenDatabaseGrbit.None);

            var dmltests = new DmlTests(instance, Database, table);
            dmltests.VerifyRecords();
            dmltests.Term();
            Api.JetEndSession(sesid, EndSessionGrbit.None);
            Api.JetTerm(instance);

            Console.WriteLine("\tRecreating database");
            File.Delete(Database);
            File.Delete(Checkpoint);
            instance = CreateInstance();
            Api.JetBeginSession(instance, out sesid, null, null);
            Api.JetAttachDatabase(sesid, Database, AttachDatabaseGrbit.None);
            Api.JetOpenDatabase(sesid, Database, null, out dbid, OpenDatabaseGrbit.None);

            dmltests = new DmlTests(instance, Database, table);
            dmltests.VerifyRecords();
            dmltests.Term();
            Api.JetEndSession(sesid, EndSessionGrbit.None);
            Api.JetTerm(instance);
        }

        /// <summary>
        /// Creates a directory. If the directory already exists it is deleted and then
        /// recreated.
        /// </summary>
        /// <param name="directory">The directory path.</param>
        private static void CreateDirectory(string directory)
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, true);
            }

            Directory.CreateDirectory(directory);
        }
    }
}