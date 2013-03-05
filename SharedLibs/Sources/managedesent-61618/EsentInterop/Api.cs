//-----------------------------------------------------------------------
// <copyright file="Api.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------
// The Microsoft.Isam.Esent.Interop namespace will be developed with these principles:
//  -   Any program written with this Api should work with the ESENT.dll from either
//      Windows XP, Windows Server 2003, Windows Vista, Windows Server 2008 or
//      Windows 7.
//  -   The Esent.Interop DLL should only require version 2.0 of the .NET Framework.
//  -   Full and complete documentation. Intellisense should be able to
//      provide useful and extensive help.
//  -   Minimal editorialization. Whenever possible the Microsoft.Isam.Esent.Interop Jet* api will
//      exactly match the ESENT Api. In particular the names of structs, types
//      and functions will not be changed. Except for:
//  -   Cleaning up Api constants. Instead of providing the constants from
//      esent.h they will be grouped into useful enumerations. This will
//      eliminate a lot of common Api errors.
//  -   Provide helper methods/objects for common operations. These will be layered
//      on top of the ESENT Api.
//  -   Minimize the interop overhead.
//  Changes that will be made are:
//  -   Convert JET_coltyp etc. into real enumerations
//  -   Removing cbStruct from structures
//  -   Removing unused/reserved entries from structures
//  -   Working around ESENT bugs or variances in API behavior
//  -   Automatically using upgraded/downgraded functionality where possible
//  -   Removing common API confusion where possible (e.g. always setting the columnid
//      in the JET_COLUMNDEF)
//  -   Throwing exceptions instead of returning errors
//  The Api has four layers:
//  -   NativeMethods (internal): this is the P/Invoke interop layer. This layer deals
//      with IntPtr and other basic types as opposed to the managed types
//      such as JET_TABLEID.
//  -   JetApi (internal): this layer turns managed objects into
//      objects which can be passed into the P/Invoke interop layer.
//      Methods at this level return an error instead of throwing an exception.
//      This layer is implemented as an object with an interface. This allows
//      the actual implementation to be replaced at runtime, either for testing
//      or to use a different DLL.
//  -   Api (public): this layer provides error-handling, turning errors
//      returned by lower layers into exceptions and warnings.
//  -   Helper methods (public): this layer provides data conversion and
//      iteration for common API activities. These methods do not start
//      with 'Jet' but are implemented using the Jet methods.
//  -   Disposable objects (public): these disposable object automatically
//      release esent resources (instances, sessions, tables and transactions). 

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics;
    using System.Runtime.InteropServices;
    using System.Security.Permissions;
    using Microsoft.Isam.Esent.Interop.Implementation;
    using Microsoft.Isam.Esent.Interop.Vista;

    /// <summary>
    /// Managed versions of the ESENT Api. This class contains static methods corresponding
    /// with the unmanaged ESENT Api. These methods throw exceptions when errors are returned.
    /// </summary>
    public static partial class Api
    {
        /// <summary>
        /// Initializes static members of the Api class.
        /// </summary>
        static Api()
        {
            Api.Impl = new JetApi();
        }

        /// <summary>
        /// Delegate for error handling code.
        /// </summary>
        /// <param name="error">The error that has been encountered.</param>
        internal delegate void ErrorHandler(JET_err error);

        /// <summary>
        /// Gets or sets the ErrorHandler for all errors. This can
        /// be used for logging or to throw an exception.
        /// </summary>
        internal static event ErrorHandler HandleError;
        
        /// <summary>
        /// Gets or sets the IJetApi this is called for all functions.
        /// </summary>
        internal static IJetApi Impl { get; set; }

        #region Init/Term

        /// <summary>
        /// Allocates a new instance of the database engine.
        /// </summary>
        /// <param name="instance">Returns the new instance.</param>
        /// <param name="name">The name of the instance. Names must be unique.</param>
        public static void JetCreateInstance(out JET_INSTANCE instance, string name)
        {
            Api.Check(Impl.JetCreateInstance(out instance, name));
        }

        /// <summary>
        /// Allocate a new instance of the database engine for use in a single
        /// process, with a display name specified.
        /// </summary>
        /// <param name="instance">Returns the newly create instance.</param>
        /// <param name="name">
        /// Specifies a unique string identifier for the instance to be created.
        /// This string must be unique within a given process hosting the
        /// database engine.
        /// </param>
        /// <param name="displayName">
        /// A display name for the instance to be created. This will be used
        /// in eventlog entries.
        /// </param>
        /// <param name="grbit">Creation options.</param>
        public static void JetCreateInstance2(out JET_INSTANCE instance, string name, string displayName, CreateInstanceGrbit grbit)
        {
            Api.Check(Impl.JetCreateInstance2(out instance, name, displayName, grbit));
        }

        /// <summary>
        /// Initialize the ESENT database engine.
        /// </summary>
        /// <param name="instance">
        /// The instance to initialize. If an instance hasn't been
        /// allocated then a new one is created and the engine
        /// will operate in single-instance mode.
        /// </param>
        public static void JetInit(ref JET_INSTANCE instance)
        {
            Api.Check(Impl.JetInit(ref instance));
        }

        /// <summary>
        /// Initialize the ESENT database engine.
        /// </summary>
        /// <param name="instance">
        /// The instance to initialize. If an instance hasn't been
        /// allocated then a new one is created and the engine
        /// will operate in single-instance mode.
        /// </param>
        /// <param name="grbit">
        /// Initialization options.
        /// </param>
        /// <returns>
        /// A warning code.
        /// </returns>
        public static JET_wrn JetInit2(ref JET_INSTANCE instance, InitGrbit grbit)
        {
            return Api.Check(Impl.JetInit2(ref instance, grbit));
        }

        /// <summary>
        /// Retrieves information about the instances that are running.
        /// </summary>
        /// <param name="numInstances">
        /// Returns the number of instances.
        /// </param>
        /// <param name="instances">
        /// Returns an array of instance info objects, one for each running
        /// instance.
        /// </param>
        public static void JetGetInstanceInfo(out int numInstances, out JET_INSTANCE_INFO[] instances)
        {
            Api.Check(Impl.JetGetInstanceInfo(out numInstances, out instances));
        }

        /// <summary>
        /// Prevents streaming backup-related activity from continuing on a
        /// specific running instance, thus ending the streaming backup in
        /// a predictable way.
        /// </summary>
        /// <param name="instance">The instance to use.</param>
        public static void JetStopBackupInstance(JET_INSTANCE instance)
        {
            Api.Check(Impl.JetStopBackupInstance(instance));
        }

        /// <summary>
        /// Prepares an instance for termination.
        /// </summary>
        /// <param name="instance">The (running) instance to use.</param>
        public static void JetStopServiceInstance(JET_INSTANCE instance)
        {
            Api.Check(Impl.JetStopServiceInstance(instance));            
        }

        /// <summary>
        /// Terminate an instance that was created with <see cref="JetInit"/> or
        /// <see cref="JetCreateInstance"/>.
        /// </summary>
        /// <param name="instance">The instance to terminate.</param>
        public static void JetTerm(JET_INSTANCE instance)
        {
            Api.Check(Impl.JetTerm(instance));
        }

        /// <summary>
        /// Terminate an instance that was created with <see cref="JetInit"/> or
        /// <see cref="JetCreateInstance"/>.
        /// </summary>
        /// <param name="instance">The instance to terminate.</param>
        /// <param name="grbit">Termination options.</param>
        public static void JetTerm2(JET_INSTANCE instance, TermGrbit grbit)
        {
            Api.Check(Impl.JetTerm2(instance, grbit));
        }

        /// <summary>
        /// Sets database configuration options.
        /// </summary>
        /// <param name="instance">
        /// The instance to set the option on or <see cref="JET_INSTANCE.Nil"/>
        /// to set the option on all instances.
        /// </param>
        /// <param name="sesid">The session to use.</param>
        /// <param name="paramid">The parameter to set.</param>
        /// <param name="paramValue">The value of the parameter to set, if the parameter is an integer type.</param>
        /// <param name="paramString">The value of the parameter to set, if the parameter is a string type.</param>
        /// <returns>An ESENT warning code.</returns>
        public static JET_wrn JetSetSystemParameter(JET_INSTANCE instance, JET_SESID sesid, JET_param paramid, int paramValue, string paramString)
        {
            return Api.Check(Impl.JetSetSystemParameter(instance, sesid, paramid, new IntPtr(paramValue), paramString));
        }

        /// <summary>
        /// Sets database configuration options.
        /// </summary>
        /// <param name="instance">
        /// The instance to set the option on or <see cref="JET_INSTANCE.Nil"/>
        /// to set the option on all instances.
        /// </param>
        /// <param name="sesid">The session to use.</param>
        /// <param name="paramid">The parameter to set.</param>
        /// <param name="paramValue">The value of the parameter to set, if the parameter is a JET_CALLBACK.</param>
        /// <param name="paramString">The value of the parameter to set, if the parameter is a string type.</param>
        /// <returns>An ESENT warning code.</returns>
        public static JET_wrn JetSetSystemParameter(JET_INSTANCE instance, JET_SESID sesid, JET_param paramid, JET_CALLBACK paramValue, string paramString)
        {
            return Api.Check(Impl.JetSetSystemParameter(instance, sesid, paramid, paramValue, paramString));
        }

        /// <summary>
        /// Gets database configuration options.
        /// </summary>
        /// <param name="instance">The instance to retrieve the options from.</param>
        /// <param name="sesid">The session to use.</param>
        /// <param name="paramid">The parameter to get.</param>
        /// <param name="paramValue">Returns the value of the parameter, if the value is an integer.</param>
        /// <param name="paramString">Returns the value of the parameter, if the value is a string.</param>
        /// <param name="maxParam">The maximum size of the parameter string.</param>
        /// <returns>An ESENT warning code.</returns>
        /// <remarks>
        /// <see cref="JET_param.ErrorToString"/> passes in the error number in the paramValue, which is why it is
        /// a ref parameter and not an out parameter.
        /// </remarks>
        public static JET_wrn JetGetSystemParameter(JET_INSTANCE instance, JET_SESID sesid, JET_param paramid, ref int paramValue, out string paramString, int maxParam)
        {
            var intValue = new IntPtr(paramValue);
            JET_wrn wrn = Api.Check(Impl.JetGetSystemParameter(instance, sesid, paramid, ref intValue, out paramString, maxParam));
            paramValue = intValue.ToInt32();

            return wrn;
        }

        /// <summary>
        /// Retrieves the version of the database engine.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="version">Returns the version number of the database engine.</param>
        [CLSCompliant(false)]
        public static void JetGetVersion(JET_SESID sesid, out uint version)
        {
            Api.Check(Impl.JetGetVersion(sesid, out version));
        }

        #endregion

        #region Databases

        /// <summary>
        /// Creates and attaches a database file.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="database">The path to the database file to create.</param>
        /// <param name="connect">The parameter is not used.</param>
        /// <param name="dbid">Returns the dbid of the new database.</param>
        /// <param name="grbit">Database creation options.</param>
        public static void JetCreateDatabase(JET_SESID sesid, string database, string connect, out JET_DBID dbid, CreateDatabaseGrbit grbit)
        {
            Api.Check(Impl.JetCreateDatabase(sesid, database, connect, out dbid, grbit));
        }

        /// <summary>
        /// Creates and attaches a database file with a maximum database size specified.
        /// <seealso cref="JetAttachDatabase2"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="database">The path to the database file to create.</param>
        /// <param name="maxPages">
        /// The maximum size, in database pages, of the database. Passing 0 means there is
        /// no enforced maximum.
        /// </param>
        /// <param name="dbid">Returns the dbid of the new database.</param>
        /// <param name="grbit">Database creation options.</param>
        public static void JetCreateDatabase2(JET_SESID sesid, string database, int maxPages, out JET_DBID dbid, CreateDatabaseGrbit grbit)
        {
            Api.Check(Impl.JetCreateDatabase2(sesid, database, maxPages, out dbid, grbit));            
        }

        /// <summary>
        /// Attaches a database file for use with a database instance. In order to use the
        /// database, it will need to be subsequently opened with <see cref="JetOpenDatabase"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="database">The database to attach.</param>
        /// <param name="grbit">Attach options.</param>
        /// <returns>An ESENT warning code.</returns>
        public static JET_wrn JetAttachDatabase(JET_SESID sesid, string database, AttachDatabaseGrbit grbit)
        {
            return Api.Check(Impl.JetAttachDatabase(sesid, database, grbit));
        }

        /// <summary>
        /// Attaches a database file for use with a database instance. In order to use the
        /// database, it will need to be subsequently opened with <see cref="JetOpenDatabase"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="database">The database to attach.</param>
        /// <param name="maxPages">
        /// The maximum size, in database pages, of the database. Passing 0 means there is
        /// no enforced maximum.
        /// </param>
        /// <param name="grbit">Attach options.</param>
        /// <returns>An ESENT warning code.</returns>
        public static JET_wrn JetAttachDatabase2(JET_SESID sesid, string database, int maxPages, AttachDatabaseGrbit grbit)
        {
            return Api.Check(Impl.JetAttachDatabase2(sesid, database, maxPages, grbit));
        }

        /// <summary>
        /// Opens a database previously attached with <see cref="JetAttachDatabase"/>,
        /// for use with a database session. This function can be called multiple times
        /// for the same database.
        /// </summary>
        /// <param name="sesid">The session that is opening the database.</param>
        /// <param name="database">The database to open.</param>
        /// <param name="connect">Reserved for future use.</param>
        /// <param name="dbid">Returns the dbid of the attached database.</param>
        /// <param name="grbit">Open database options.</param>
        /// <returns>An ESENT warning code.</returns>
        public static JET_wrn JetOpenDatabase(JET_SESID sesid, string database, string connect, out JET_DBID dbid, OpenDatabaseGrbit grbit)
        {
            return Api.Check(Impl.JetOpenDatabase(sesid, database, connect, out dbid, grbit));
        }

        /// <summary>
        /// Closes a database file that was previously opened with <see cref="JetOpenDatabase"/> or
        /// created with <see cref="JetCreateDatabase"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to close.</param>
        /// <param name="grbit">Close options.</param>
        public static void JetCloseDatabase(JET_SESID sesid, JET_DBID dbid, CloseDatabaseGrbit grbit)
        {
            Api.Check(Impl.JetCloseDatabase(sesid, dbid, grbit));
        }

        /// <summary>
        /// Releases a database file that was previously attached to a database session.
        /// </summary>
        /// <param name="sesid">The database session to use.</param>
        /// <param name="database">The database to detach.</param>
        public static void JetDetachDatabase(JET_SESID sesid, string database)
        {
            Api.Check(Impl.JetDetachDatabase(sesid, database));
        }

#pragma warning disable 618,612 // Disable warning that JET_CONVERT is obsolete
        /// <summary>
        /// Makes a copy of an existing database. The copy is compacted to a
        /// state optimal for usage. Data in the copied data will be packed
        /// according to the measures chosen for the indexes at index create.
        /// In this way, compacted data may be stored as densely as possible.
        /// Alternatively, compacted data may reserve space for subsequent
        /// record growth or index insertions.
        /// </summary>
        /// <param name="sesid">The session to use for the call.</param>
        /// <param name="sourceDatabase">The source database that will be compacted.</param>
        /// <param name="destinationDatabase">The name to use for the compacted database.</param>
        /// <param name="statusCallback">
        /// A callback function that can be called periodically through the
        /// database compact operation to report progress.
        /// </param>
        /// <param name="ignored">
        /// This parameter is ignored and should be null.
        /// </param>
        /// <param name="grbit">Compact options.</param>
        public static void JetCompact(
            JET_SESID sesid,
            string sourceDatabase,
            string destinationDatabase,
            JET_PFNSTATUS statusCallback,
            JET_CONVERT ignored,
            CompactGrbit grbit)
        {
            Api.Check(
                Impl.JetCompact(sesid, sourceDatabase, destinationDatabase, statusCallback, ignored, grbit));
        }
#pragma warning restore 618,612

        /// <summary>
        /// Extends the size of a database that is currently open.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to grow.</param>
        /// <param name="desiredPages">The desired size of the database, in pages.</param>
        /// <param name="actualPages">
        /// The size of the database, in pages, after the call.
        /// </param>
        public static void JetGrowDatabase(JET_SESID sesid, JET_DBID dbid, int desiredPages, out int actualPages)
        {
            Api.Check(Impl.JetGrowDatabase(sesid, dbid, desiredPages, out actualPages));
        }

        /// <summary>
        /// Sets the size of an unopened database file.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="database">The name of the database.</param>
        /// <param name="desiredPages">The desired size of the database, in pages.</param>
        /// <param name="actualPages">
        /// The size of the database, in pages, after the call.
        /// </param>
        public static void JetSetDatabaseSize(JET_SESID sesid, string database, int desiredPages, out int actualPages)
        {
            Api.Check(Impl.JetSetDatabaseSize(sesid, database, desiredPages, out actualPages));
        }

        /// <summary>
        /// Retrieves certain information about the given database.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database identifier.</param>
        /// <param name="value">The value to be retrieved.</param>
        /// <param name="infoLevel">The specific data to retrieve.</param>
        public static void JetGetDatabaseInfo(
            JET_SESID sesid,
            JET_DBID dbid,
            out int value,
            JET_DbInfo infoLevel)
        {
            Api.Check(Impl.JetGetDatabaseInfo(sesid, dbid, out value, infoLevel));
        }

        /// <summary>
        /// Retrieves certain information about the given database.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database identifier.</param>
        /// <param name="dbinfomisc">The value to be retrieved.</param>
        /// <param name="infoLevel">The specific data to retrieve.</param>
        public static void JetGetDatabaseInfo(
            JET_SESID sesid,
            JET_DBID dbid,
            out JET_DBINFOMISC dbinfomisc,
            JET_DbInfo infoLevel)
        {
            Api.Check(Impl.JetGetDatabaseInfo(sesid, dbid, out dbinfomisc, infoLevel));
        }

        /// <summary>
        /// Retrieves certain information about the given database.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database identifier.</param>
        /// <param name="value">The value to be retrieved.</param>
        /// <param name="infoLevel">The specific data to retrieve.</param>
        public static void JetGetDatabaseInfo(
            JET_SESID sesid,
            JET_DBID dbid,
            out string value,
            JET_DbInfo infoLevel)
        {
            Api.Check(Impl.JetGetDatabaseInfo(sesid, dbid, out value, infoLevel));
        }

        /// <summary>
        /// Retrieves certain information about the given database.
        /// </summary>
        /// <param name="databaseName">The file name of the database.</param>
        /// <param name="value">The value to be retrieved.</param>
        /// <param name="infoLevel">The specific data to retrieve.</param>
        public static void JetGetDatabaseFileInfo(
            string databaseName,
            out int value,
            JET_DbInfo infoLevel)
        {
            Api.Check(Impl.JetGetDatabaseFileInfo(databaseName, out value, infoLevel));
        }

        /// <summary>
        /// Retrieves certain information about the given database.
        /// </summary>
        /// <param name="databaseName">The file name of the database.</param>
        /// <param name="value">The value to be retrieved.</param>
        /// <param name="infoLevel">The specific data to retrieve.</param>
        public static void JetGetDatabaseFileInfo(
            string databaseName,
            out long value,
            JET_DbInfo infoLevel)
        {
            Api.Check(Impl.JetGetDatabaseFileInfo(databaseName, out value, infoLevel));
        }

        /// <summary>
        /// Retrieves certain information about the given database.
        /// </summary>
        /// <param name="databaseName">The file name of the database.</param>
        /// <param name="dbinfomisc">The value to be retrieved.</param>
        /// <param name="infoLevel">The specific data to retrieve.</param>
        public static void JetGetDatabaseFileInfo(
            string databaseName,
            out JET_DBINFOMISC dbinfomisc,
            JET_DbInfo infoLevel)
        {
            Api.Check(Impl.JetGetDatabaseFileInfo(databaseName, out dbinfomisc, infoLevel));
        }

        #endregion

        #region Backup/Restore

        /// <summary>
        /// Performs a streaming backup of an instance, including all the attached
        /// databases, to a directory. With multiple backup methods supported by
        /// the engine, this is the simplest and most encapsulated function.
        /// </summary>
        /// <param name="instance">The instance to backup.</param>
        /// <param name="destination">
        /// The directory where the backup is to be stored. If the backup path is
        /// null to use the function will truncate the logs, if possible.
        /// </param>
        /// <param name="grbit">Backup options.</param>
        /// <param name="statusCallback">
        /// Optional status notification callback.
        /// </param>
        public static void JetBackupInstance(JET_INSTANCE instance, string destination, BackupGrbit grbit, JET_PFNSTATUS statusCallback)
        {
            Api.Check(Impl.JetBackupInstance(instance, destination, grbit, statusCallback));
        }

        /// <summary>
        /// Restores and recovers a streaming backup of an instance including all
        /// the attached databases. It is designed to work with a backup created
        /// with the <see cref="Api.JetBackupInstance"/> function. This is the
        /// simplest and most encapsulated restore function. 
        /// </summary>
        /// <param name="instance">
        /// The instance to use. The instance should not be initialized.
        /// Restoring the files will initialize the instance.
        /// </param>
        /// <param name="source">
        /// Location of the backup. The backup should have been created with
        /// <see cref="Api.JetBackupInstance"/>.
        /// </param>
        /// <param name="destination">
        /// Name of the folder where the database files from the backup set will
        /// be copied and recovered. If this is set to null, the database files
        /// will be copied and recovered to their original location.
        /// </param>
        /// <param name="statusCallback">
        /// Optional status notification callback.
        /// </param>
        public static void JetRestoreInstance(JET_INSTANCE instance, string source, string destination, JET_PFNSTATUS statusCallback)
        {
            Api.Check(Impl.JetRestoreInstance(instance, source, destination, statusCallback));
        }

        #endregion

        #region Snapshot Backup

        /// <summary>
        /// Starts a snapshot. While the snapshot is in progress, no
        /// write-to-disk activity by the engine can take place.
        /// </summary>
        /// <param name="snapshot">The snapshot session.</param>
        /// <param name="numInstances">
        /// Returns the number of instances that are part of the snapshot session.
        /// </param>
        /// <param name="instances">
        /// Returns information about the instances that are part of the snapshot session.
        /// </param>
        /// <param name="grbit">
        /// Snapshot freeze options.
        /// </param>
        public static void JetOSSnapshotFreeze(JET_OSSNAPID snapshot, out int numInstances, out JET_INSTANCE_INFO[] instances, SnapshotFreezeGrbit grbit)
        {
            Api.Check(Impl.JetOSSnapshotFreeze(snapshot, out numInstances, out instances, grbit));
        }

        /// <summary>
        /// Begins the preparations for a snapshot session. A snapshot session
        /// is a short time interval in which the engine does not issue any
        /// write IOs to disk, so that the engine can participate in a volume
        /// snapshot session (when driven by a snapshot writer).
        /// </summary>
        /// <param name="snapshot">Returns the ID of the snapshot session.</param>
        /// <param name="grbit">Snapshot options.</param>
        public static void JetOSSnapshotPrepare(out JET_OSSNAPID snapshot, SnapshotPrepareGrbit grbit)
        {
            Api.Check(Impl.JetOSSnapshotPrepare(out snapshot, grbit));
        }

        /// <summary>
        /// Notifies the engine that it can resume normal IO operations after a
        /// freeze period and a successful snapshot.
        /// </summary>
        /// <param name="snapshot">The ID of the snapshot.</param>
        /// <param name="grbit">Thaw options.</param>
        public static void JetOSSnapshotThaw(JET_OSSNAPID snapshot, SnapshotThawGrbit grbit)
        {
            Api.Check(Impl.JetOSSnapshotThaw(snapshot, grbit));
        }

        #endregion

        #region Streaming Backup/Restore

        /// <summary>
        /// Initiates an external backup while the engine and database are online and active. 
        /// </summary>
        /// <param name="instance">The instance prepare for backup.</param>
        /// <param name="grbit">Backup options.</param>
        public static void JetBeginExternalBackupInstance(JET_INSTANCE instance, BeginExternalBackupGrbit grbit)
        {
            Api.Check(Impl.JetBeginExternalBackupInstance(instance, grbit));
        }

        /// <summary>
        /// Closes a file that was opened with JetOpenFileInstance after the
        /// data from that file has been extracted using JetReadFileInstance.
        /// </summary>
        /// <param name="instance">The instance to use.</param>
        /// <param name="handle">The handle to close.</param>
        public static void JetCloseFileInstance(JET_INSTANCE instance, JET_HANDLE handle)
        {
            Api.Check(Impl.JetCloseFileInstance(instance, handle));
        }

        /// <summary>
        /// Ends an external backup session. This API is the last API in a series
        /// of APIs that must be called to execute a successful online
        /// (non-VSS based) backup.
        /// </summary>
        /// <param name="instance">The instance to end the backup for.</param>
        public static void JetEndExternalBackupInstance(JET_INSTANCE instance)
        {
            Api.Check(Impl.JetEndExternalBackupInstance(instance));  
        }

        /// <summary>
        /// Ends an external backup session. This API is the last API in a series
        /// of APIs that must be called to execute a successful online
        /// (non-VSS based) backup.
        /// </summary>
        /// <param name="instance">The instance to end the backup for.</param>
        /// <param name="grbit">Options that specify how the backup ended.</param>
        public static void JetEndExternalBackupInstance2(JET_INSTANCE instance, EndExternalBackupGrbit grbit)
        {
            Api.Check(Impl.JetEndExternalBackupInstance2(instance, grbit));
        }

        /// <summary>
        /// Used during a backup initiated by <see cref="JetBeginExternalBackupInstance"/>
        /// to query an instance for the names of database files that should become part of
        /// the backup file set. Only databases that are currently attached to the instance
        /// using <see cref="JetAttachDatabase"/> will be considered. These files may
        /// subsequently be opened using <see cref="JetOpenFileInstance"/> and read
        /// using <see cref="JetReadFileInstance"/>.
        /// </summary>
        /// <remarks>
        /// It is important to note that this API does not return an error or warning if
        /// the output buffer is too small to accept the full list of files that should be
        /// part of the backup file set. 
        /// </remarks>
        /// <param name="instance">The instance to get the information for.</param>
        /// <param name="files">
        /// Returns a list of null terminated strings describing the set of database files
        /// that should be a part of the backup file set. The list of strings returned in
        /// this buffer is in the same format as a multi-string used by the registry. Each
        /// null-terminated string is returned in sequence followed by a final null terminator.
        /// </param>
        /// <param name="maxChars">
        /// Maximum number of characters to retrieve.
        /// </param>
        /// <param name="actualChars">
        /// Actual size of the file list. If this is greater than maxChars
        /// then the list has been truncated.
        /// </param>
        public static void JetGetAttachInfoInstance(JET_INSTANCE instance, out string files, int maxChars, out int actualChars)
        {
            Api.Check(Impl.JetGetAttachInfoInstance(instance, out files, maxChars, out actualChars));
        }

        /// <summary>
        /// Used during a backup initiated by <see cref="JetBeginExternalBackupInstance"/>
        /// to query an instance for the names of database patch files and logfiles that 
        /// should become part of the backup file set. These files may subsequently be 
        /// opened using <see cref="JetOpenFileInstance"/> and read using <see cref="JetReadFileInstance"/>.
        /// </summary>
        /// <remarks>
        /// It is important to note that this API does not return an error or warning if
        /// the output buffer is too small to accept the full list of files that should be
        /// part of the backup file set. 
        /// </remarks>
        /// <param name="instance">The instance to get the information for.</param>
        /// <param name="files">
        /// Returns a list of null terminated strings describing the set of database patch files
        /// and log files that should be a part of the backup file set. The list of strings returned in
        /// this buffer is in the same format as a multi-string used by the registry. Each
        /// null-terminated string is returned in sequence followed by a final null terminator.
        /// </param>
        /// <param name="maxChars">
        /// Maximum number of characters to retrieve.
        /// </param>
        /// <param name="actualChars">
        /// Actual size of the file list. If this is greater than maxChars
        /// then the list has been truncated.
        /// </param>
        public static void JetGetLogInfoInstance(JET_INSTANCE instance, out string files, int maxChars, out int actualChars)
        {
            Api.Check(Impl.JetGetLogInfoInstance(instance, out files, maxChars, out actualChars));
        }

        /// <summary>
        /// Used during a backup initiated by <see cref="JetBeginExternalBackupInstance"/>
        /// to query an instance for the names of the transaction log files that can be safely
        /// deleted after the backup has successfully completed.
        /// </summary>
        /// <remarks>
        /// It is important to note that this API does not return an error or warning if
        /// the output buffer is too small to accept the full list of files that should be
        /// part of the backup file set. 
        /// </remarks>
        /// <param name="instance">The instance to get the information for.</param>
        /// <param name="files">
        /// Returns a list of null terminated strings describing the set of database log files
        /// that can be safely deleted after the backup completes. The list of strings returned in
        /// this buffer is in the same format as a multi-string used by the registry. Each
        /// null-terminated string is returned in sequence followed by a final null terminator.
        /// </param>
        /// <param name="maxChars">
        /// Maximum number of characters to retrieve.
        /// </param>
        /// <param name="actualChars">
        /// Actual size of the file list. If this is greater than maxChars
        /// then the list has been truncated.
        /// </param>
        public static void JetGetTruncateLogInfoInstance(JET_INSTANCE instance, out string files, int maxChars, out int actualChars)
        {
            Api.Check(Impl.JetGetTruncateLogInfoInstance(instance, out files, maxChars, out actualChars));
        }

        /// <summary>
        /// Opens an attached database, database patch file, or transaction log
        /// file of an active instance for the purpose of performing a streaming
        /// fuzzy backup. The data from these files can subsequently be read
        /// through the returned handle using JetReadFileInstance. The returned
        /// handle must be closed using JetCloseFileInstance. An external backup
        /// of the instance must have been previously initiated using
        /// JetBeginExternalBackupInstance.
        /// </summary>
        /// <param name="instance">The instance to use.</param>
        /// <param name="file">The file to open.</param>
        /// <param name="handle">Returns a handle to the file.</param>
        /// <param name="fileSizeLow">Returns the least significant 32 bits of the file size.</param>
        /// <param name="fileSizeHigh">Returns the most significant 32 bits of the file size.</param>
        public static void JetOpenFileInstance(JET_INSTANCE instance, string file, out JET_HANDLE handle, out long fileSizeLow, out long fileSizeHigh)
        {
            Api.Check(Impl.JetOpenFileInstance(instance, file, out handle, out fileSizeLow, out fileSizeHigh));
        }

        /// <summary>
        /// Retrieves the contents of a file opened with <see cref="Api.JetOpenFileInstance"/>.
        /// </summary>
        /// <param name="instance">The instance to use.</param>
        /// <param name="file">The file to read from.</param>
        /// <param name="buffer">The buffer to read into.</param>
        /// <param name="bufferSize">The size of the buffer.</param>
        /// <param name="bytesRead">Returns the amount of data read into the buffer.</param>
        public static void JetReadFileInstance(JET_INSTANCE instance, JET_HANDLE file, byte[] buffer, int bufferSize, out int bytesRead)
        {
            Api.Check(Impl.JetReadFileInstance(instance, file, buffer, bufferSize, out bytesRead));
        }

        /// <summary>
        /// Used during a backup initiated by JetBeginExternalBackup to delete
        /// any transaction log files that will no longer be needed once the
        /// current backup completes successfully.
        /// </summary>
        /// <param name="instance">The instance to truncate.</param>
        public static void JetTruncateLogInstance(JET_INSTANCE instance)
        {
            Api.Check(Impl.JetTruncateLogInstance(instance));
        }

        #endregion

        #region Sessions

        /// <summary>
        /// Initialize a new ESENT session.
        /// </summary>
        /// <param name="instance">The initialized instance to create the session in.</param>
        /// <param name="sesid">Returns the created session.</param>
        /// <param name="username">The parameter is not used.</param>
        /// <param name="password">The parameter is not used.</param>
        public static void JetBeginSession(JET_INSTANCE instance, out JET_SESID sesid, string username, string password)
        {
            Api.Check(Impl.JetBeginSession(instance, out sesid, username, password));
        }

        /// <summary>
        /// Associates a session with the current thread using the given context
        /// handle. This association overrides the default engine requirement
        /// that a transaction for a given session must occur entirely on the
        /// same thread. Use <see cref="JetResetSessionContext"/> to remove the
        /// association.
        /// </summary>
        /// <param name="sesid">The session to set the context on.</param>
        /// <param name="context">The context to set.</param>
        public static void JetSetSessionContext(JET_SESID sesid, IntPtr context)
        {
            Api.Check(Impl.JetSetSessionContext(sesid, context));
        }

        /// <summary>
        /// Disassociates a session from the current thread. This should be
        /// used in conjunction with <see cref="JetSetSessionContext"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        public static void JetResetSessionContext(JET_SESID sesid)
        {
            Api.Check(Impl.JetResetSessionContext(sesid));
        }

        /// <summary>
        /// Ends a session.
        /// </summary>
        /// <param name="sesid">The session to end.</param>
        /// <param name="grbit">This parameter is not used.</param>
        public static void JetEndSession(JET_SESID sesid, EndSessionGrbit grbit)
        {
            Api.Check(Impl.JetEndSession(sesid, grbit));
        }

        /// <summary>
        /// Initialize a new ESE session in the same instance as the given sesid.
        /// </summary>
        /// <param name="sesid">The session to duplicate.</param>
        /// <param name="newSesid">Returns the new session.</param>
        public static void JetDupSession(JET_SESID sesid, out JET_SESID newSesid)
        {
            Api.Check(Impl.JetDupSession(sesid, out newSesid));
        }

        #endregion

        #region Tables

        /// <summary>
        /// Opens a cursor on a previously created table.
        /// </summary>
        /// <param name="sesid">The database session to use.</param>
        /// <param name="dbid">The database to open the table in.</param>
        /// <param name="tablename">The name of the table to open.</param>
        /// <param name="parameters">The parameter is not used.</param>
        /// <param name="parametersSize">The parameter is not used.</param>
        /// <param name="grbit">Table open options.</param>
        /// <param name="tableid">Returns the opened table.</param>
        public static void JetOpenTable(JET_SESID sesid, JET_DBID dbid, string tablename, byte[] parameters, int parametersSize, OpenTableGrbit grbit, out JET_TABLEID tableid)
        {
            Api.Check(Impl.JetOpenTable(sesid, dbid, tablename, parameters, parametersSize, grbit, out tableid));
        }

        /// <summary>
        /// Close an open table.
        /// </summary>
        /// <param name="sesid">The session which opened the table.</param>
        /// <param name="tableid">The table to close.</param>
        public static void JetCloseTable(JET_SESID sesid, JET_TABLEID tableid)
        {
            Api.Check(Impl.JetCloseTable(sesid, tableid));
        }

        /// <summary>
        /// Duplicates an open cursor and returns a handle to the duplicated cursor.
        /// If the cursor that was duplicated was a read-only cursor then the
        /// duplicated cursor is also a read-only cursor.
        /// Any state related to constructing a search key or updating a record is
        /// not copied into the duplicated cursor. In addition, the location of the
        /// original cursor is not duplicated into the duplicated cursor. The
        /// duplicated cursor is always opened on the clustered index and its
        /// location is always on the first row of the table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to duplicate.</param>
        /// <param name="newTableid">The duplicated cursor.</param>
        /// <param name="grbit">Reserved for future use.</param>
        public static void JetDupCursor(JET_SESID sesid, JET_TABLEID tableid, out JET_TABLEID newTableid, DupCursorGrbit grbit)
        {
            Api.Check(Impl.JetDupCursor(sesid, tableid, out newTableid, grbit));
        }

        /// <summary>
        /// Walks each index of a table to exactly compute the number of entries
        /// in an index, and the number of distinct keys in an index. This
        /// information, together with the number of database pages allocated
        /// for an index and the current time of the computation is stored in
        /// index metadata in the database. This data can be subsequently retrieved
        /// with information operations.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table that the statistics will be computed on.</param>
        public static void JetComputeStats(JET_SESID sesid, JET_TABLEID tableid)
        {
            Api.Check(Impl.JetComputeStats(sesid, tableid));
        }

        /// <summary>
        /// Enables the application to associate a context handle known as
        /// Local Storage with a cursor or the table associated with that
        /// cursor. This context handle can be used by the application to
        /// store auxiliary data that is associated with a cursor or table.
        /// The application is later notified using a runtime callback when
        /// the context handle must be released. This makes it possible to
        /// associate dynamically allocated state with a cursor or table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to use.</param>
        /// <param name="ls">The context handle to be associated with the session or cursor.</param>
        /// <param name="grbit">Set options.</param>
        public static void JetSetLS(JET_SESID sesid, JET_TABLEID tableid, JET_LS ls, LsGrbit grbit)
        {
            Api.Check(Impl.JetSetLS(sesid, tableid, ls, grbit));
        }

        /// <summary>
        /// Enables the application to retrieve the context handle known
        /// as Local Storage that is associated with a cursor or the table
        /// associated with that cursor. This context handle must have been
        /// previously set using <see cref="JetSetLS"/>. JetGetLS can also
        /// be used to simultaneously fetch the current context handle for
        /// a cursor or table and reset that context handle.  
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to use.</param>
        /// <param name="ls">Returns the retrieved context handle.</param>
        /// <param name="grbit">Retrieve options.</param>
        public static void JetGetLS(JET_SESID sesid, JET_TABLEID tableid, out JET_LS ls, LsGrbit grbit)
        {
            Api.Check(Impl.JetGetLS(sesid, tableid, out ls, grbit));
        }

        /// <summary>
        /// Determine whether an update of the current record of a cursor
        /// will result in a write conflict, based on the current update
        /// status of the record. It is possible that a write conflict will
        /// ultimately be returned even if JetGetCursorInfo returns successfully.
        /// because another session may update the record before the current
        /// session is able to update the same record.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to check.</param>
        public static void JetGetCursorInfo(JET_SESID sesid, JET_TABLEID tableid)
        {
            Api.Check(Impl.JetGetCursorInfo(sesid, tableid));
        }

        #endregion

        #region Transactions

        /// <summary>
        /// Causes a session to enter a transaction or create a new save point in an existing
        /// transaction.
        /// </summary>
        /// <param name="sesid">The session to begin the transaction for.</param>
        public static void JetBeginTransaction(JET_SESID sesid)
        {
            Api.Check(Impl.JetBeginTransaction(sesid));
        }

        /// <summary>
        /// Causes a session to enter a transaction or create a new save point in an existing
        /// transaction.
        /// </summary>
        /// <param name="sesid">The session to begin the transaction for.</param>
        /// <param name="grbit">Transaction options.</param>
        public static void JetBeginTransaction2(JET_SESID sesid, BeginTransactionGrbit grbit)
        {
            Api.Check(Impl.JetBeginTransaction2(sesid, grbit));
        }

        /// <summary>
        /// Commits the changes made to the state of the database during the current save point
        /// and migrates them to the previous save point. If the outermost save point is committed
        /// then the changes made during that save point will be committed to the state of the
        /// database and the session will exit the transaction.
        /// </summary>
        /// <param name="sesid">The session to commit the transaction for.</param>
        /// <param name="grbit">Commit options.</param>
        public static void JetCommitTransaction(JET_SESID sesid, CommitTransactionGrbit grbit)
        {
            Api.Check(Impl.JetCommitTransaction(sesid, grbit));
        }

        /// <summary>
        /// Undoes the changes made to the state of the database
        /// and returns to the last save point. JetRollback will also close any cursors
        /// opened during the save point. If the outermost save point is undone, the
        /// session will exit the transaction.
        /// </summary>
        /// <param name="sesid">The session to rollback the transaction for.</param>
        /// <param name="grbit">Rollback options.</param>
        public static void JetRollback(JET_SESID sesid, RollbackTransactionGrbit grbit)
        {
            Api.Check(Impl.JetRollback(sesid, grbit));
        }

        #endregion

        #region DDL

        /// <summary>
        /// Create an empty table. The newly created table is opened exclusively.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to create the table in.</param>
        /// <param name="table">The name of the table to create.</param>
        /// <param name="pages">Initial number of pages in the table.</param>
        /// <param name="density">
        /// The default density of the table. This is used when doing sequential inserts.
        /// </param>
        /// <param name="tableid">Returns the tableid of the new table.</param>
        public static void JetCreateTable(JET_SESID sesid, JET_DBID dbid, string table, int pages, int density, out JET_TABLEID tableid)
        {
            Api.Check(Impl.JetCreateTable(sesid, dbid, table, pages, density, out tableid));
        }

        /// <summary>
        /// Add a new column to an existing table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to add the column to.</param>
        /// <param name="column">The name of the column.</param>
        /// <param name="columndef">The definition of the column.</param>
        /// <param name="defaultValue">The default value of the column.</param>
        /// <param name="defaultValueSize">The size of the default value.</param>
        /// <param name="columnid">Returns the columnid of the new column.</param>
        public static void JetAddColumn(JET_SESID sesid, JET_TABLEID tableid, string column, JET_COLUMNDEF columndef, byte[] defaultValue, int defaultValueSize, out JET_COLUMNID columnid)
        {
            Api.Check(Impl.JetAddColumn(sesid, tableid, column, columndef, defaultValue, defaultValueSize, out columnid));
        }

        /// <summary>
        /// Deletes a column from a database table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">A cursor on the table to delete the column from.</param>
        /// <param name="column">The name of the column to be deleted.</param>
        public static void JetDeleteColumn(JET_SESID sesid, JET_TABLEID tableid, string column)
        {
            Api.Check(Impl.JetDeleteColumn(sesid, tableid, column));
        }

        /// <summary>
        /// Deletes a column from a database table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">A cursor on the table to delete the column from.</param>
        /// <param name="column">The name of the column to be deleted.</param>
        /// <param name="grbit">Column deletion options.</param>
        public static void JetDeleteColumn2(JET_SESID sesid, JET_TABLEID tableid, string column, DeleteColumnGrbit grbit)
        {
            Api.Check(Impl.JetDeleteColumn2(sesid, tableid, column, grbit));
        }

        /// <summary>
        /// Deletes an index from a database table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">A cursor on the table to delete the index from.</param>
        /// <param name="index">The name of the index to be deleted.</param>
        public static void JetDeleteIndex(JET_SESID sesid, JET_TABLEID tableid, string index)
        {
            Api.Check(Impl.JetDeleteIndex(sesid, tableid, index));
        }

        /// <summary>
        /// Deletes a table from a database.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to delete the table from.</param>
        /// <param name="table">The name of the table to delete.</param>
        public static void JetDeleteTable(JET_SESID sesid, JET_DBID dbid, string table)
        {
            Api.Check(Impl.JetDeleteTable(sesid, dbid, table));
        }

        /// <summary>
        /// Creates an index over data in an ESE database. An index can be used to locate
        /// specific data quickly.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to create the index on.</param>
        /// <param name="indexName">
        /// Pointer to a null-terminated string that specifies the name of the index to create. 
        /// </param>
        /// <param name="grbit">Index creation options.</param>
        /// <param name="keyDescription">
        /// Pointer to a double null-terminated string of null-delimited tokens.
        /// </param>
        /// <param name="keyDescriptionLength">
        /// The length, in characters, of szKey including the two terminating nulls.
        /// </param>
        /// <param name="density">Initial B+ tree density.</param>
        public static void JetCreateIndex(
            JET_SESID sesid,
            JET_TABLEID tableid,
            string indexName,
            CreateIndexGrbit grbit,
            string keyDescription,
            int keyDescriptionLength,
            int density)
        {
            Api.Check(Impl.JetCreateIndex(sesid, tableid, indexName, grbit, keyDescription, keyDescriptionLength, density));
        }

        /// <summary>
        /// Creates indexes over data in an ESE database.
        /// </summary>
        /// <remarks>
        /// When creating multiple indexes (i.e. with numIndexCreates
        /// greater than 1) this method MUST be called
        /// outside of any transactions and with exclusive access to the
        /// table. The JET_TABLEID returned by <see cref="JetCreateTable"/>
        /// will have exlusive access or the table can be opened for
        /// exclusive access by passing <see cref="OpenTableGrbit.DenyRead"/>
        /// to <see cref="JetOpenTable"/>.
        /// </remarks>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to create the index on.</param>
        /// <param name="indexcreates">Array of objects describing the indexes to be created.</param>
        /// <param name="numIndexCreates">Number of index description objects.</param>
        public static void JetCreateIndex2(
            JET_SESID sesid,
            JET_TABLEID tableid,
            JET_INDEXCREATE[] indexcreates,
            int numIndexCreates)
        {
            Api.Check(Impl.JetCreateIndex2(sesid, tableid, indexcreates, numIndexCreates));            
        }

        /// <summary>
        /// Creates a temporary table with a single index. A temporary table
        /// stores and retrieves records just like an ordinary table created
        /// using JetCreateTableColumnIndex. However, temporary tables are
        /// much faster than ordinary tables due to their volatile nature.
        /// They can also be used to very quickly sort and perform duplicate
        /// removal on record sets when accessed in a purely sequential manner.
        /// Also see
        /// <seealso cref="Api.JetOpenTempTable2"/>,
        /// <seealso cref="Api.JetOpenTempTable3"/>.
        /// <seealso cref="VistaApi.JetOpenTemporaryTable"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="columns">
        /// Column definitions for the columns created in the temporary table.
        /// </param>
        /// <param name="numColumns">Number of column definitions.</param>
        /// <param name="grbit">Table creation options.</param>
        /// <param name="tableid">
        /// Returns the tableid of the temporary table. Closing this tableid
        /// with <see cref="JetCloseTable"/> frees the resources associated
        /// with the temporary table.
        /// </param>
        /// <param name="columnids">
        /// The output buffer that receives the array of column IDs generated
        /// during the creation of the temporary table. The column IDs in this
        /// array will exactly correspond to the input array of column definitions.
        /// As a result, the size of this buffer must correspond to the size of
        /// the input array.
        /// </param>
        public static void JetOpenTempTable(
            JET_SESID sesid,
            JET_COLUMNDEF[] columns,
            int numColumns,
            TempTableGrbit grbit,
            out JET_TABLEID tableid,
            JET_COLUMNID[] columnids)
        {
            Api.Check(Impl.JetOpenTempTable(sesid, columns, numColumns, grbit, out tableid, columnids));            
        }

        /// <summary>
        /// Creates a temporary table with a single index. A temporary table
        /// stores and retrieves records just like an ordinary table created
        /// using JetCreateTableColumnIndex. However, temporary tables are
        /// much faster than ordinary tables due to their volatile nature.
        /// They can also be used to very quickly sort and perform duplicate
        /// removal on record sets when accessed in a purely sequential manner.
        /// Also see
        /// <seealso cref="Api.JetOpenTempTable"/>,
        /// <seealso cref="Api.JetOpenTempTable3"/>.
        /// <seealso cref="VistaApi.JetOpenTemporaryTable"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="columns">
        /// Column definitions for the columns created in the temporary table.
        /// </param>
        /// <param name="numColumns">Number of column definitions.</param>
        /// <param name="lcid">
        /// The locale ID to use to compare any Unicode key column data in the temporary table.
        /// Any locale may be used as long as the appropriate language pack has been installed
        /// on the machine. 
        /// </param>
        /// <param name="grbit">Table creation options.</param>
        /// <param name="tableid">
        /// Returns the tableid of the temporary table. Closing this tableid
        /// with <see cref="JetCloseTable"/> frees the resources associated
        /// with the temporary table.
        /// </param>
        /// <param name="columnids">
        /// The output buffer that receives the array of column IDs generated
        /// during the creation of the temporary table. The column IDs in this
        /// array will exactly correspond to the input array of column definitions.
        /// As a result, the size of this buffer must correspond to the size of
        /// the input array.
        /// </param>
        public static void JetOpenTempTable2(
            JET_SESID sesid,
            JET_COLUMNDEF[] columns,
            int numColumns,
            int lcid,
            TempTableGrbit grbit,
            out JET_TABLEID tableid,
            JET_COLUMNID[] columnids)
        {
            Api.Check(Impl.JetOpenTempTable2(sesid, columns, numColumns, lcid, grbit, out tableid, columnids));
        }

        /// <summary>
        /// Creates a temporary table with a single index. A temporary table
        /// stores and retrieves records just like an ordinary table created
        /// using JetCreateTableColumnIndex. However, temporary tables are
        /// much faster than ordinary tables due to their volatile nature.
        /// They can also be used to very quickly sort and perform duplicate
        /// removal on record sets when accessed in a purely sequential manner.
        /// Also see
        /// <seealso cref="Api.JetOpenTempTable"/>,
        /// <seealso cref="Api.JetOpenTempTable2"/>,
        /// <seealso cref="VistaApi.JetOpenTemporaryTable"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="columns">
        /// Column definitions for the columns created in the temporary table.
        /// </param>
        /// <param name="numColumns">Number of column definitions.</param>
        /// <param name="unicodeindex">
        /// The Locale ID and normalization flags that will be used to compare
        /// any Unicode key column data in the temporary table. When this 
        /// is not present then the default options are used. 
        /// </param>
        /// <param name="grbit">Table creation options.</param>
        /// <param name="tableid">
        /// Returns the tableid of the temporary table. Closing this tableid
        /// with <see cref="JetCloseTable"/> frees the resources associated
        /// with the temporary table.
        /// </param>
        /// <param name="columnids">
        /// The output buffer that receives the array of column IDs generated
        /// during the creation of the temporary table. The column IDs in this
        /// array will exactly correspond to the input array of column definitions.
        /// As a result, the size of this buffer must correspond to the size of
        /// the input array.
        /// </param>
        public static void JetOpenTempTable3(
            JET_SESID sesid,
            JET_COLUMNDEF[] columns,
            int numColumns,
            JET_UNICODEINDEX unicodeindex,
            TempTableGrbit grbit,
            out JET_TABLEID tableid,
            JET_COLUMNID[] columnids)
        {
            Api.Check(Impl.JetOpenTempTable3(sesid, columns, numColumns, unicodeindex, grbit, out tableid, columnids));            
        }

        /// <summary>
        /// Creates a table, adds columns, and indices on that table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to which to add the new table.</param>
        /// <param name="tablecreate">Object describing the table to create.</param>
        public static void JetCreateTableColumnIndex3(
            JET_SESID sesid,
            JET_DBID dbid,
            JET_TABLECREATE tablecreate)
        {
            Api.Check(Impl.JetCreateTableColumnIndex3(sesid, dbid, tablecreate));
        }

        #region JetGetTableColumnInfo overloads

        /// <summary>
        /// Retrieves information about a table column.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table containing the column.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="columndef">Filled in with information about the column.</param>
        public static void JetGetTableColumnInfo(
                JET_SESID sesid,
                JET_TABLEID tableid,
                string columnName,
                out JET_COLUMNDEF columndef)
        {
            Api.Check(Impl.JetGetTableColumnInfo(sesid, tableid, columnName, out columndef));
        }

        /// <summary>
        /// Retrieves information about a table column.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table containing the column.</param>
        /// <param name="columnid">The columnid of the column.</param>
        /// <param name="columndef">Filled in with information about the column.</param>
        public static void JetGetTableColumnInfo(
                JET_SESID sesid,
                JET_TABLEID tableid,
                JET_COLUMNID columnid,
                out JET_COLUMNDEF columndef)
        {
            Api.Check(Impl.JetGetTableColumnInfo(sesid, tableid, columnid, out columndef));
        }

        /// <summary>
        /// Retrieves information about all columns in the table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table containing the column.</param>
        /// <param name="columnName">The parameter is ignored.</param>
        /// <param name="columnlist">Filled in with information about the columns in the table.</param>
        public static void JetGetTableColumnInfo(
                JET_SESID sesid,
                JET_TABLEID tableid,
                string columnName,
                out JET_COLUMNLIST columnlist)
        {
            Api.Check(Impl.JetGetTableColumnInfo(sesid, tableid, columnName, out columnlist));
        }

        #endregion

        #region JetGetColumnInfo overloads

        /// <summary>
        /// Retrieves information about a table column.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database that contains the table.</param>
        /// <param name="tablename">The name of the table containing the column.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="columndef">Filled in with information about the column.</param>
        public static void JetGetColumnInfo(
                JET_SESID sesid,
                JET_DBID dbid,
                string tablename,
                string columnName,
                out JET_COLUMNDEF columndef)
        {
            Api.Check(Impl.JetGetColumnInfo(sesid, dbid, tablename, columnName, out columndef));
        }

        /// <summary>
        /// Retrieves information about all columns in a table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database that contains the table.</param>
        /// <param name="tablename">The name of the table containing the column.</param>
        /// <param name="columnName">This parameter is ignored.</param>
        /// <param name="columnlist">Filled in with information about the columns in the table.</param>
        public static void JetGetColumnInfo(
                JET_SESID sesid,
                JET_DBID dbid,
                string tablename,
                string columnName,
                out JET_COLUMNLIST columnlist)
        {
            Api.Check(Impl.JetGetColumnInfo(sesid, dbid, tablename, columnName, out columnlist));
        }

        /// <summary>
        /// Retrieves information about a column in a table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database that contains the table.</param>
        /// <param name="tablename">The name of the table containing the column.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="columnbase">Filled in with information about the columns in the table.</param>
        public static void JetGetColumnInfo(
                JET_SESID sesid,
                JET_DBID dbid,
                string tablename,
                string columnName,
                out JET_COLUMNBASE columnbase)
        {
            Api.Check(Impl.JetGetColumnInfo(sesid, dbid, tablename, columnName, out columnbase));
        }

        #endregion

        #region JetGetObjectInfo overloads

        /// <summary>
        /// Retrieves information about database objects.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to use.</param>
        /// <param name="objectlist">Filled in with information about the objects in the database.</param>
        public static void JetGetObjectInfo(JET_SESID sesid, JET_DBID dbid, out JET_OBJECTLIST objectlist)
        {
            Api.Check(Impl.JetGetObjectInfo(sesid, dbid, out objectlist));
        }

        /// <summary>
        /// Retrieves information about database objects.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to use.</param>
        /// <param name="objtyp">The type of the object.</param>
        /// <param name="szObjectName">The object name about which to retrieve information.</param>
        /// <param name="objectinfo">Filled in with information about the objects in the database.</param>
        public static void JetGetObjectInfo(
            JET_SESID sesid,
            JET_DBID dbid,
            JET_objtyp objtyp,
            string szObjectName,
            out JET_OBJECTINFO objectinfo)
        {
            Api.Check(Impl.JetGetObjectInfo(sesid, dbid, objtyp, szObjectName, out objectinfo));
        }

        #endregion

        /// <summary>
        /// Ddetermines the name of the current
        /// index of a given cursor. This name is also used to later re-select
        /// that index as the current index using <see cref="JetSetCurrentIndex"/>.
        /// It can also be used to discover the properties of that index using
        /// JetGetTableIndexInfo.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to get the index name for.</param>
        /// <param name="indexName">Returns the name of the index.</param>
        /// <param name="maxNameLength">
        /// The maximum length of the index name. Index names are no more than 
        /// <see cref="SystemParameters.NameMost"/> characters.
        /// </param>
        public static void JetGetCurrentIndex(JET_SESID sesid, JET_TABLEID tableid, out string indexName, int maxNameLength)
        {
            Api.Check(Impl.JetGetCurrentIndex(sesid, tableid, out indexName, maxNameLength));
        }

        #region JetGetTableInfo overloads

        /// <summary>
        /// Retrieves various pieces of information about a table in a database.
        /// </summary>
        /// <remarks>
        /// This overload is used with <see cref="JET_TblInfo.Default"/>.
        /// </remarks>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to retrieve information about.</param>
        /// <param name="result">Retrieved information.</param>
        /// <param name="infoLevel">The type of information to retrieve.</param>       
        public static void JetGetTableInfo(JET_SESID sesid, JET_TABLEID tableid, out JET_OBJECTINFO result, JET_TblInfo infoLevel)
        {
            Api.Check(Impl.JetGetTableInfo(sesid, tableid, out result, infoLevel));
        }

        /// <summary>
        /// Retrieves various pieces of information about a table in a database.
        /// </summary>
        /// <remarks>
        /// This overload is used with <see cref="JET_TblInfo.Name"/> and
        /// <see cref="JET_TblInfo.TemplateTableName"/>.
        /// </remarks>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to retrieve information about.</param>
        /// <param name="result">Retrieved information.</param>
        /// <param name="infoLevel">The type of information to retrieve.</param>
        public static void JetGetTableInfo(JET_SESID sesid, JET_TABLEID tableid, out string result, JET_TblInfo infoLevel)
        {
            Api.Check(Impl.JetGetTableInfo(sesid, tableid, out result, infoLevel));
        }

        /// <summary>
        /// Retrieves various pieces of information about a table in a database.
        /// </summary>
        /// <remarks>
        /// This overload is used with <see cref="JET_TblInfo.Dbid"/>.
        /// </remarks>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to retrieve information about.</param>
        /// <param name="result">Retrieved information.</param>
        /// <param name="infoLevel">The type of information to retrieve.</param>
        public static void JetGetTableInfo(JET_SESID sesid, JET_TABLEID tableid, out JET_DBID result, JET_TblInfo infoLevel)
        {
            Api.Check(Impl.JetGetTableInfo(sesid, tableid, out result, infoLevel));
        }

        /// <summary>
        /// Retrieves various pieces of information about a table in a database.
        /// </summary>
        /// <remarks>
        /// This overload is used with <see cref="JET_TblInfo.SpaceUsage"/> and
        /// <see cref="JET_TblInfo.SpaceAlloc"/>.
        /// </remarks>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to retrieve information about.</param>
        /// <param name="result">Retrieved information.</param>
        /// <param name="infoLevel">The type of information to retrieve.</param>
        public static void JetGetTableInfo(JET_SESID sesid, JET_TABLEID tableid, int[] result, JET_TblInfo infoLevel)
        {
            Api.Check(Impl.JetGetTableInfo(sesid, tableid, result, infoLevel));
        }

        /// <summary>
        /// Retrieves various pieces of information about a table in a database.
        /// </summary>
        /// <remarks>
        /// This overload is used with <see cref="JET_TblInfo.SpaceOwned"/> and
        /// <see cref="JET_TblInfo.SpaceAvailable"/>.
        /// </remarks>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to retrieve information about.</param>
        /// <param name="result">Retrieved information.</param>
        /// <param name="infoLevel">The type of information to retrieve.</param>
        public static void JetGetTableInfo(JET_SESID sesid, JET_TABLEID tableid, out int result, JET_TblInfo infoLevel)
        {
            Api.Check(Impl.JetGetTableInfo(sesid, tableid, out result, infoLevel));
        }

        #endregion

        #region JetGetIndexInfo overloads

        /// <summary>
        /// Retrieves information about indexes on a table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to use.</param>
        /// <param name="tablename">The name of the table to retrieve index information about.</param>
        /// <param name="indexname">The name of the index to retrieve information about.</param>
        /// <param name="result">Filled in with information about indexes on the table.</param>
        /// <param name="infoLevel">The type of information to retrieve.</param>
        [CLSCompliant(false)]
        public static void JetGetIndexInfo(
                JET_SESID sesid,
                JET_DBID dbid,
                string tablename,
                string indexname,
                out ushort result,
                JET_IdxInfo infoLevel)
        {
            Api.Check(Impl.JetGetIndexInfo(sesid, dbid, tablename, indexname, out result, infoLevel));
        }

        /// <summary>
        /// Retrieves information about indexes on a table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to use.</param>
        /// <param name="tablename">The name of the table to retrieve index information about.</param>
        /// <param name="indexname">The name of the index to retrieve information about.</param>
        /// <param name="result">Filled in with information about indexes on the table.</param>
        /// <param name="infoLevel">The type of information to retrieve.</param>
        public static void JetGetIndexInfo(
                JET_SESID sesid,
                JET_DBID dbid,
                string tablename,
                string indexname,
                out int result,
                JET_IdxInfo infoLevel)
        {
            Api.Check(Impl.JetGetIndexInfo(sesid, dbid, tablename, indexname, out result, infoLevel));
        }

        /// <summary>
        /// Retrieves information about indexes on a table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to use.</param>
        /// <param name="tablename">The name of the table to retrieve index information about.</param>
        /// <param name="indexname">The name of the index to retrieve information about.</param>
        /// <param name="result">Filled in with information about indexes on the table.</param>
        /// <param name="infoLevel">The type of information to retrieve.</param>
        public static void JetGetIndexInfo(
                JET_SESID sesid,
                JET_DBID dbid,
                string tablename,
                string indexname,
                out JET_INDEXID result,
                JET_IdxInfo infoLevel)
        {
            Api.Check(Impl.JetGetIndexInfo(sesid, dbid, tablename, indexname, out result, infoLevel));
        }

        /// <summary>
        /// Retrieves information about indexes on a table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to use.</param>
        /// <param name="tablename">The name of the table to retrieve index information about.</param>
        /// <param name="indexname">The name of the index to retrieve information about.</param>
        /// <param name="result">Filled in with information about indexes on the table.</param>
        /// <param name="infoLevel">The type of information to retrieve.</param>
        public static void JetGetIndexInfo(
                JET_SESID sesid,
                JET_DBID dbid,
                string tablename,
                string indexname,
                out JET_INDEXLIST result,
                JET_IdxInfo infoLevel)
        {
            Api.Check(Impl.JetGetIndexInfo(sesid, dbid, tablename, indexname, out result, infoLevel));
        }

        #endregion

        #region JetGetTableIndexInfo overloads

        /// <summary>
        /// Retrieves information about indexes on a table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to retrieve index information about.</param>
        /// <param name="indexname">The name of the index.</param>
        /// <param name="result">Filled in with information about indexes on the table.</param>
        /// <param name="infoLevel">The type of information to retrieve.</param>
        [CLSCompliant(false)]
        public static void JetGetTableIndexInfo(
                JET_SESID sesid,
                JET_TABLEID tableid,
                string indexname,
                out ushort result,
                JET_IdxInfo infoLevel)
        {
            Api.Check(Impl.JetGetTableIndexInfo(sesid, tableid, indexname, out result, infoLevel));
        }

        /// <summary>
        /// Retrieves information about indexes on a table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to retrieve index information about.</param>
        /// <param name="indexname">The name of the index.</param>
        /// <param name="result">Filled in with information about indexes on the table.</param>
        /// <param name="infoLevel">The type of information to retrieve.</param>
        public static void JetGetTableIndexInfo(
                JET_SESID sesid,
                JET_TABLEID tableid,
                string indexname,
                out int result,
                JET_IdxInfo infoLevel)
        {
            Api.Check(Impl.JetGetTableIndexInfo(sesid, tableid, indexname, out result, infoLevel));
        }

        /// <summary>
        /// Retrieves information about indexes on a table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to retrieve index information about.</param>
        /// <param name="indexname">The name of the index.</param>
        /// <param name="result">Filled in with information about indexes on the table.</param>
        /// <param name="infoLevel">The type of information to retrieve.</param>
        public static void JetGetTableIndexInfo(
                JET_SESID sesid,
                JET_TABLEID tableid,
                string indexname,
                out JET_INDEXID result,
                JET_IdxInfo infoLevel)
        {
            Api.Check(Impl.JetGetTableIndexInfo(sesid, tableid, indexname, out result, infoLevel));
        }

        /// <summary>
        /// Retrieves information about indexes on a table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to retrieve index information about.</param>
        /// <param name="indexname">The name of the index.</param>
        /// <param name="result">Filled in with information about indexes on the table.</param>
        /// <param name="infoLevel">The type of information to retrieve.</param>
        public static void JetGetTableIndexInfo(
                JET_SESID sesid,
                JET_TABLEID tableid,
                string indexname,
                out JET_INDEXLIST result,
                JET_IdxInfo infoLevel)
        {
            Api.Check(Impl.JetGetTableIndexInfo(sesid, tableid, indexname, out result, infoLevel));
        }

        #endregion

        /// <summary>
        /// Changes the name of an existing table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database containing the table.</param>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="newTableName">The new name of the table.</param>
        public static void JetRenameTable(JET_SESID sesid, JET_DBID dbid, string tableName, string newTableName)
        {
            Api.Check(Impl.JetRenameTable(sesid, dbid, tableName, newTableName));
        }

        /// <summary>
        /// Changes the name of an existing column.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table containing the column.</param>
        /// <param name="name">The name of the column.</param>
        /// <param name="newName">The new name of the column.</param>
        /// <param name="grbit">Column rename options.</param>
        public static void JetRenameColumn(JET_SESID sesid, JET_TABLEID tableid, string name, string newName, RenameColumnGrbit grbit)
        {
            Api.Check(Impl.JetRenameColumn(sesid, tableid, name, newName, grbit));
        }

        /// <summary>
        /// Changes the default value of an existing column.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database containing the column.</param>
        /// <param name="tableName">The name of the table containing the column.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="data">The new default value.</param>
        /// <param name="dataSize">Size of the new default value.</param>
        /// <param name="grbit">Column default value options.</param>
        public static void JetSetColumnDefaultValue(
            JET_SESID sesid, JET_DBID dbid, string tableName, string columnName, byte[] data, int dataSize, SetColumnDefaultValueGrbit grbit)
        {
            Api.Check(Impl.JetSetColumnDefaultValue(sesid, dbid, tableName, columnName, data, dataSize, grbit));
        }

        #endregion

        #region Navigation

        /// <summary>
        /// Positions a cursor to an index entry for the record that is associated with
        /// the specified bookmark. The bookmark can be used with any index defined over
        /// a table. The bookmark for a record can be retrieved using <see cref="JetGetBookmark"/>. 
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to position.</param>
        /// <param name="bookmark">The bookmark used to position the cursor.</param>
        /// <param name="bookmarkSize">The size of the bookmark.</param>
        public static void JetGotoBookmark(JET_SESID sesid, JET_TABLEID tableid, byte[] bookmark, int bookmarkSize)
        {
            Api.Check(Impl.JetGotoBookmark(sesid, tableid, bookmark, bookmarkSize));
        }

        /// <summary>
        /// Positions a cursor to an index entry that is associated with the
        /// specified secondary index bookmark. The secondary index bookmark
        /// must be used with the same index over the same table from which it
        /// was originally retrieved. The secondary index bookmark for an index
        /// entry can be retrieved using <see cref="JetGotoSecondaryIndexBookmark"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table cursor to position.</param>
        /// <param name="secondaryKey">The buffer that contains the secondary key.</param>
        /// <param name="secondaryKeySize">The size of the secondary key.</param>
        /// <param name="primaryKey">The buffer that contains the primary key.</param>
        /// <param name="primaryKeySize">The size of the primary key.</param>
        /// <param name="grbit">Options for positioning the bookmark.</param>
        public static void JetGotoSecondaryIndexBookmark(
            JET_SESID sesid,
            JET_TABLEID tableid,
            byte[] secondaryKey,
            int secondaryKeySize,
            byte[] primaryKey,
            int primaryKeySize,
            GotoSecondaryIndexBookmarkGrbit grbit)
        {
            Api.Check(
                Impl.JetGotoSecondaryIndexBookmark(
                    sesid, tableid, secondaryKey, secondaryKeySize, primaryKey, primaryKeySize, grbit));
        }

        /// <summary>
        /// Navigate through an index. The cursor can be positioned at the start or
        /// end of the index and moved backwards and forwards by a specified number
        /// of index entries. Also see
        /// <seealso cref="TryMoveFirst"/>, <seealso cref="TryMoveLast"/>,
        /// <seealso cref="TryMoveNext"/>, <seealso cref="TryMovePrevious"/>.
        /// </summary>
        /// <param name="sesid">The session to use for the call.</param>
        /// <param name="tableid">The cursor to position.</param>
        /// <param name="numRows">An offset which indicates how far to move the cursor.</param>
        /// <param name="grbit">Move options.</param>
        public static void JetMove(JET_SESID sesid, JET_TABLEID tableid, int numRows, MoveGrbit grbit)
        {
            Api.Check(Impl.JetMove(sesid, tableid, numRows, grbit));
        }

        /// <summary>
        /// Navigate through an index. The cursor can be positioned at the start or
        /// end of the index and moved backwards and forwards by a specified number
        /// of index entries. Also see
        /// <seealso cref="TryMoveFirst"/>, <seealso cref="TryMoveLast"/>,
        /// <seealso cref="TryMoveNext"/>, <seealso cref="TryMovePrevious"/>.
        /// </summary>
        /// <param name="sesid">The session to use for the call.</param>
        /// <param name="tableid">The cursor to position.</param>
        /// <param name="numRows">An offset which indicates how far to move the cursor.</param>
        /// <param name="grbit">Move options.</param>
        public static void JetMove(JET_SESID sesid, JET_TABLEID tableid, JET_Move numRows, MoveGrbit grbit)
        {
            Api.Check(Impl.JetMove(sesid, tableid, (int)numRows, grbit));
        }

        /// <summary>
        /// Constructs search keys that may then be used by <see cref="JetSeek"/> and <see cref="JetSetIndexRange"/>.
        /// </summary>
        /// <remarks>
        /// The MakeKey functions provide datatype-specific make key functionality.
        /// </remarks>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="dataSize">Size of the data.</param>
        /// <param name="grbit">Key options.</param>
        public static void JetMakeKey(JET_SESID sesid, JET_TABLEID tableid, byte[] data, int dataSize, MakeKeyGrbit grbit)
        {
            if ((null == data && 0 != dataSize) || (null != data && dataSize > data.Length))
            {
                throw new ArgumentOutOfRangeException(
                    "dataSize",
                    dataSize,
                    "cannot be greater than the length of the data");
            }

            unsafe
            {
                fixed (byte* pointer = data)
                {
                    Api.JetMakeKey(sesid, tableid, new IntPtr(pointer), dataSize, grbit);
                }
            }
        }

        /// <summary>
        /// Efficiently positions a cursor to an index entry that matches the search
        /// criteria specified by the search key in that cursor and the specified
        /// inequality. A search key must have been previously constructed using 
        /// <see cref="JetMakeKey(JET_SESID,JET_TABLEID,byte[],int,MakeKeyGrbit)"/>.
        /// Also see <seealso cref="TrySeek"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to position.</param>
        /// <param name="grbit">Seek options.</param>
        /// <returns>An ESENT warning.</returns>
        public static JET_wrn JetSeek(JET_SESID sesid, JET_TABLEID tableid, SeekGrbit grbit)
        {
            return Api.Check(Impl.JetSeek(sesid, tableid, grbit));
        }

        /// <summary>
        /// Temporarily limits the set of index entries that the cursor can walk using
        /// <see cref="JetMove(JET_SESID,JET_TABLEID,int,MoveGrbit)"/> to those starting
        /// from the current index entry and ending at the index entry that matches the
        /// search criteria specified by the search key in that cursor and the specified
        /// bound criteria. A search key must have been previously constructed using
        /// <see cref="JetMakeKey(JET_SESID,JET_TABLEID,byte[],int,MakeKeyGrbit)"/>.
        /// Also see <seealso cref="TrySetIndexRange"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to set the index range on.</param>
        /// <param name="grbit">Index range options.</param>
        public static void JetSetIndexRange(JET_SESID sesid, JET_TABLEID tableid, SetIndexRangeGrbit grbit)
        {
            Api.Check(Impl.JetSetIndexRange(sesid, tableid, grbit));
        }

        /// <summary>
        /// Computes the intersection between multiple sets of index entries from different secondary
        /// indices over the same table. This operation is useful for finding the set of records in a
        /// table that match two or more criteria that can be expressed using index ranges. Also see
        /// <seealso cref="IntersectIndexes"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="ranges">
        /// An the index ranges to intersect. The tableids in the ranges
        /// must have index ranges set on them. Use <see cref="JetSetIndexRange"/>
        /// to create an index range.
        /// </param>
        /// <param name="numRanges">
        /// The number of index ranges.
        /// </param>
        /// <param name="recordlist">
        /// Returns information about the temporary table containing the intersection results.
        /// </param>
        /// <param name="grbit">Intersection options.</param>
        public static void JetIntersectIndexes(
            JET_SESID sesid,
            JET_INDEXRANGE[] ranges,
            int numRanges,
            out JET_RECORDLIST recordlist,
            IntersectIndexesGrbit grbit)
        {
            Api.Check(Impl.JetIntersectIndexes(sesid, ranges, numRanges, out recordlist, grbit));
        }

        /// <summary>
        /// Set the current index of a cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to set the index on.</param>
        /// <param name="index">
        /// The name of the index to be selected. If this is null or empty the primary
        /// index will be selected.
        /// </param>
        public static void JetSetCurrentIndex(JET_SESID sesid, JET_TABLEID tableid, string index)
        {
            Api.Check(Impl.JetSetCurrentIndex(sesid, tableid, index));
        }

        /// <summary>
        /// Set the current index of a cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to set the index on.</param>
        /// <param name="index">
        /// The name of the index to be selected. If this is null or empty the primary
        /// index will be selected.
        /// </param>
        /// <param name="grbit">
        /// Set index options.
        /// </param>
        public static void JetSetCurrentIndex2(JET_SESID sesid, JET_TABLEID tableid, string index, SetCurrentIndexGrbit grbit)
        {
            Api.Check(Impl.JetSetCurrentIndex2(sesid, tableid, index, grbit));
        }

        /// <summary>
        /// Set the current index of a cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to set the index on.</param>
        /// <param name="index">
        /// The name of the index to be selected. If this is null or empty the primary
        /// index will be selected.
        /// </param>
        /// <param name="grbit">
        /// Set index options.
        /// </param>
        /// <param name="itagSequence">
        /// Sequence number of the multi-valued column value which will be used
        /// to position the cursor on the new index. This parameter is only used
        /// in conjunction with <see cref="SetCurrentIndexGrbit.NoMove"/>. When
        /// this parameter is not present or is set to zero, its value is presumed
        /// to be 1.
        /// </param>
        public static void JetSetCurrentIndex3(JET_SESID sesid, JET_TABLEID tableid, string index, SetCurrentIndexGrbit grbit, int itagSequence)
        {
            Api.Check(Impl.JetSetCurrentIndex3(sesid, tableid, index, grbit, itagSequence));
        }

        /// <summary>
        /// Set the current index of a cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to set the index on.</param>
        /// <param name="index">
        /// The name of the index to be selected. If this is null or empty the primary
        /// index will be selected.
        /// </param>
        /// <param name="indexid">
        /// The id of the index to select. This id can be obtained using JetGetIndexInfo
        /// or JetGetTableIndexInfo with the <see cref="JET_IdxInfo.IndexId"/> option.
        /// </param>
        /// <param name="grbit">
        /// Set index options.
        /// </param>
        /// <param name="itagSequence">
        /// Sequence number of the multi-valued column value which will be used
        /// to position the cursor on the new index. This parameter is only used
        /// in conjunction with <see cref="SetCurrentIndexGrbit.NoMove"/>. When
        /// this parameter is not present or is set to zero, its value is presumed
        /// to be 1.
        /// </param>
        public static void JetSetCurrentIndex4(
            JET_SESID sesid,
            JET_TABLEID tableid,
            string index,
            JET_INDEXID indexid,
            SetCurrentIndexGrbit grbit,
            int itagSequence)
        {
            Api.Check(Impl.JetSetCurrentIndex4(sesid, tableid, index, indexid, grbit, itagSequence));
        }

        /// <summary>
        /// Counts the number of entries in the current index from the current position forward.
        /// The current position is included in the count. The count can be greater than the
        /// total number of records in the table if the current index is over a multi-valued
        /// column and instances of the column have multiple-values. If the table is empty,
        /// then 0 will be returned for the count. 
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to count the records in.</param>
        /// <param name="numRecords">Returns the number of records.</param>
        /// <param name="maxRecordsToCount">
        /// The maximum number of records to count. A value of 0 indicates that the count
        /// is unlimited.
        /// </param>
        public static void JetIndexRecordCount(JET_SESID sesid, JET_TABLEID tableid, out int numRecords, int maxRecordsToCount)
        {
            if (0 == maxRecordsToCount)
            {
                // BUG: Older versions of esent (e.g. Windows XP) don't use 0 as an unlimited count,
                // instead they simply count zero records (which isn't very useful). To make
                // sure this API works as advertised we will increase the maximum record count.
                maxRecordsToCount = int.MaxValue;
            }

            Api.Check(Impl.JetIndexRecordCount(sesid, tableid, out numRecords, maxRecordsToCount));
        }

        /// <summary>
        /// Notifies the database engine that the application is scanning the entire
        /// index that the cursor is positioned on. Consequently, the methods that
        /// are used to access the index data will be tuned to make this scenario as
        /// fast as possible. 
        /// Also see <seealso cref="JetResetTableSequential"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor that will be accessing the data.</param>
        /// <param name="grbit">Reserved for future use.</param>
        public static void JetSetTableSequential(JET_SESID sesid, JET_TABLEID tableid, SetTableSequentialGrbit grbit)
        {
            Api.Check(Impl.JetSetTableSequential(sesid, tableid, grbit));
        }

        /// <summary>
        /// Notifies the database engine that the application is no longer scanning the
        /// entire index the cursor is positioned on. This call reverses a notification
        /// sent by <see cref="JetSetTableSequential"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor that was accessing the data.</param>
        /// <param name="grbit">Reserved for future use.</param>
        public static void JetResetTableSequential(JET_SESID sesid, JET_TABLEID tableid, ResetTableSequentialGrbit grbit)
        {
            Api.Check(Impl.JetResetTableSequential(sesid, tableid, grbit));
        }

        /// <summary>
        /// Returns the fractional position of the current record in the current index
        /// in the form of a <see cref="JET_RECPOS"/> structure.
        /// Also see <seealso cref="JetGotoPosition"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor positioned on the record.</param>
        /// <param name="recpos">Returns the approximate fractional position of the record.</param>
        public static void JetGetRecordPosition(JET_SESID sesid, JET_TABLEID tableid, out JET_RECPOS recpos)
        {
            Api.Check(Impl.JetGetRecordPosition(sesid, tableid, out recpos));
        }

        /// <summary>
        /// Moves a cursor to a new location that is a fraction of the way through
        /// the current index. 
        /// Also see <seealso cref="JetGetRecordPosition"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to position.</param>
        /// <param name="recpos">The approximate position to move to.</param>
        public static void JetGotoPosition(JET_SESID sesid, JET_TABLEID tableid, JET_RECPOS recpos)
        {
            Api.Check(Impl.JetGotoPosition(sesid, tableid, recpos));
        }

        #endregion

        #region Data Retrieval

        /// <summary>
        /// Retrieves the bookmark for the record that is associated with the index entry
        /// at the current position of a cursor. This bookmark can then be used to
        /// reposition that cursor back to the same record using <see cref="JetGotoBookmark"/>. 
        /// The bookmark will be no longer than <see cref="SystemParameters.BookmarkMost"/>
        /// bytes.
        /// Also see <seealso cref="GetBookmark"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the bookmark from.</param>
        /// <param name="bookmark">Buffer to contain the bookmark.</param>
        /// <param name="bookmarkSize">Size of the bookmark buffer.</param>
        /// <param name="actualBookmarkSize">Returns the actual size of the bookmark.</param>
        public static void JetGetBookmark(JET_SESID sesid, JET_TABLEID tableid, byte[] bookmark, int bookmarkSize, out int actualBookmarkSize)
        {
            Api.Check(Impl.JetGetBookmark(sesid, tableid, bookmark, bookmarkSize, out actualBookmarkSize));
        }

        /// <summary>
        /// Retrieves a special bookmark for the secondary index entry at the
        /// current position of a cursor. This bookmark can then be used to
        /// efficiently reposition that cursor back to the same index entry
        /// using JetGotoSecondaryIndexBookmark. This is most useful when
        /// repositioning on a secondary index that contains duplicate keys or
        /// that contains multiple index entries for the same record.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the bookmark from.</param>
        /// <param name="secondaryKey">Output buffer for the secondary key.</param>
        /// <param name="secondaryKeySize">Size of the secondary key buffer.</param>
        /// <param name="actualSecondaryKeySize">Returns the size of the secondary key.</param>
        /// <param name="primaryKey">Output buffer for the primary key.</param>
        /// <param name="primaryKeySize">Size of the primary key buffer.</param>
        /// <param name="actualPrimaryKeySize">Returns the size of the primary key.</param>
        /// <param name="grbit">Options for the call.</param>
        public static void JetGetSecondaryIndexBookmark(
            JET_SESID sesid,
            JET_TABLEID tableid,
            byte[] secondaryKey,
            int secondaryKeySize,
            out int actualSecondaryKeySize,
            byte[] primaryKey,
            int primaryKeySize,
            out int actualPrimaryKeySize,
            GetSecondaryIndexBookmarkGrbit grbit)
        {
            Api.Check(
                Impl.JetGetSecondaryIndexBookmark(
                    sesid,
                    tableid,
                    secondaryKey,
                    secondaryKeySize,
                    out actualSecondaryKeySize,
                    primaryKey,
                    primaryKeySize,
                    out actualPrimaryKeySize,
                    grbit));
        }

        /// <summary>
        /// Retrieves the key for the index entry at the current position of a cursor.
        /// Also see <seealso cref="RetrieveKey"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the key from.</param>
        /// <param name="data">The buffer to retrieve the key into.</param>
        /// <param name="dataSize">The size of the buffer.</param>
        /// <param name="actualDataSize">Returns the actual size of the data.</param>
        /// <param name="grbit">Retrieve key options.</param>
        public static void JetRetrieveKey(JET_SESID sesid, JET_TABLEID tableid, byte[] data, int dataSize, out int actualDataSize, RetrieveKeyGrbit grbit)
        {
            Api.Check(Impl.JetRetrieveKey(sesid, tableid, data, dataSize, out actualDataSize, grbit));
        }

        /// <summary>
        /// Retrieves a single column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// Alternatively, this function can retrieve a column from a record being created
        /// in the cursor copy buffer. This function can also retrieve column data from an
        /// index entry that references the current record. In addition to retrieving the
        /// actual column value, JetRetrieveColumn can also be used to retrieve the size
        /// of a column, before retrieving the column data itself so that application
        /// buffers can be sized appropriately.  
        /// </summary>
        /// <remarks>
        /// The RetrieveColumnAs functions provide datatype-specific retrieval functions.
        /// </remarks>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="data">The data buffer to be retrieved into.</param>
        /// <param name="dataSize">The size of the data buffer.</param>
        /// <param name="actualDataSize">Returns the actual size of the data buffer.</param>
        /// <param name="grbit">Retrieve column options.</param>
        /// <param name="retinfo">
        /// If pretinfo is give as NULL then the function behaves as though an itagSequence
        /// of 1 and an ibLongValue of 0 (zero) were given. This causes column retrieval to
        /// retrieve the first value of a multi-valued column, and to retrieve long data at
        /// offset 0 (zero).
        /// </param>
        /// <returns>An ESENT warning code.</returns>
        public static JET_wrn JetRetrieveColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, byte[] data, int dataSize, out int actualDataSize, RetrieveColumnGrbit grbit, JET_RETINFO retinfo)
        {
            return Api.JetRetrieveColumn(sesid, tableid, columnid, data, dataSize, 0, out actualDataSize, grbit, retinfo);
        }

        /// <summary>
        /// Retrieves multiple column values from the current record in a
        /// single operation. An array of JET_RETRIEVECOLUMN structures is
        /// used to describe the set of column values to be retrieved, and
        /// to describe output buffers for each column value to be retrieved.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the data from.</param>
        /// <param name="retrievecolumns">
        /// An array of one or more <see cref="JET_RETRIEVECOLUMN"/> objects
        /// describing the data to be retrieved.
        /// </param>
        /// <param name="numColumns">
        /// The number of entries in the columns array.
        /// </param>
        /// <returns>
        /// If any column retrieved is truncated due to an insufficient
        /// length buffer, then the API will return
        /// <see cref="JET_wrn.BufferTruncated"/>. However other errors
        /// JET_wrnColumnNull are returned only in the error field of
        /// the <see cref="JET_RETRIEVECOLUMN"/> object.
        /// </returns>
        public static JET_wrn JetRetrieveColumns(JET_SESID sesid, JET_TABLEID tableid, JET_RETRIEVECOLUMN[] retrievecolumns, int numColumns)
        {
            if (null == retrievecolumns)
            {
                throw new ArgumentNullException("retrievecolumns");
            }

            if (numColumns < 0 || numColumns > retrievecolumns.Length)
            {
                throw new ArgumentOutOfRangeException("numColumns", numColumns, "cannot be negative or greater than retrievecolumns.Length");
            }

            unsafe
            {
                NATIVE_RETRIEVECOLUMN* nativeretrievecolumns = stackalloc NATIVE_RETRIEVECOLUMN[numColumns];

                int err = Api.PinColumnsAndRetrieve(sesid, tableid, nativeretrievecolumns, retrievecolumns, numColumns, 0);
                for (int i = 0; i < numColumns; ++i)
                {
                    retrievecolumns[i].UpdateFromNativeRetrievecolumn(ref nativeretrievecolumns[i]);
                }

                return Api.Check(err);
            }
        }

        /// <summary>
        /// Efficiently retrieves a set of columns and their values from the
        /// current record of a cursor or the copy buffer of that cursor. The
        /// columns and values retrieved can be restricted by a list of
        /// column IDs, itagSequence numbers, and other characteristics. This
        /// column retrieval API is unique in that it returns information in
        /// dynamically allocated memory that is obtained using a
        /// user-provided realloc compatible callback. This new flexibility
        /// permits the efficient retrieval of column data with specific
        /// characteristics (such as size and multiplicity) that are unknown
        /// to the caller. This eliminates the need for the use of the discovery
        /// modes of JetRetrieveColumn to determine those
        /// characteristics in order to setup a final call to
        /// JetRetrieveColumn that will successfully retrieve
        /// the desired data.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve data from.</param>
        /// <param name="numColumnids">The numbers of JET_ENUMCOLUMNIDS.</param>
        /// <param name="columnids">
        /// An optional array of column IDs, each with an optional array of itagSequence
        /// numbers to enumerate.
        /// </param>
        /// <param name="numColumnValues">
        /// Returns the number of column values retrieved.
        /// </param>
        /// <param name="columnValues">
        /// Returns the enumerated column values.
        /// </param>
        /// <param name="allocator">
        /// Callback used to allocate memory.
        /// </param>
        /// <param name="allocatorContext">
        /// Context for the allocation callback.
        /// </param>
        /// <param name="maxDataSize">
        /// Sets a cap on the amount of data to return from a long text or long
        /// binary column. This parameter can be used to prevent the enumeration
        /// of an extremely large column value.
        /// </param>
        /// <param name="grbit">Retrieve options.</param>
        /// <returns>A warning or success.</returns>
        [CLSCompliant(false)]
        public static JET_wrn JetEnumerateColumns(
            JET_SESID sesid,
            JET_TABLEID tableid,
            int numColumnids,
            JET_ENUMCOLUMNID[] columnids,
            out int numColumnValues,
            out JET_ENUMCOLUMN[] columnValues,
            JET_PFNREALLOC allocator,
            IntPtr allocatorContext,
            int maxDataSize,
            EnumerateColumnsGrbit grbit)
        {
            return Api.Check(
                Impl.JetEnumerateColumns(
                    sesid,
                    tableid,
                    numColumnids,
                    columnids,
                    out numColumnValues,
                    out columnValues,
                    allocator,
                    allocatorContext,
                    maxDataSize,
                    grbit));
        }

        #endregion

        #region DML

        /// <summary>
        /// Deletes the current record in a database table.
        /// </summary>
        /// <param name="sesid">The session that opened the cursor.</param>
        /// <param name="tableid">The cursor on a database table. The current row will be deleted.</param>
        public static void JetDelete(JET_SESID sesid, JET_TABLEID tableid)
        {
            Api.Check(Impl.JetDelete(sesid, tableid));
        }

        /// <summary>
        /// Prepare a cursor for update.
        /// </summary>
        /// <param name="sesid">The session which is starting the update.</param>
        /// <param name="tableid">The cursor to start the update for.</param>
        /// <param name="prep">The type of update to prepare.</param>
        public static void JetPrepareUpdate(JET_SESID sesid, JET_TABLEID tableid, JET_prep prep)
        {
            Api.Check(Impl.JetPrepareUpdate(sesid, tableid, prep));
        }

        /// <summary>
        /// The JetUpdate function performs an update operation including inserting a new row into
        /// a table or updating an existing row. Deleting a table row is performed by calling
        /// <see cref="JetDelete"/>.
        /// </summary>
        /// <param name="sesid">The session which started the update.</param>
        /// <param name="tableid">The cursor to update. An update should be prepared.</param>
        /// <param name="bookmark">Returns the bookmark of the updated record. This can be null.</param>
        /// <param name="bookmarkSize">The size of the bookmark buffer.</param>
        /// <param name="actualBookmarkSize">Returns the actual size of the bookmark.</param>
        /// <remarks>
        /// JetUpdate is the final step in performing an insert or an update. The update is begun by
        /// calling <see cref="JetPrepareUpdate"/> and then by calling
        /// <see cref="JetSetColumn(JET_SESID,JET_TABLEID,JET_COLUMNID,byte[],int,SetColumnGrbit,JET_SETINFO)"/>
        /// one or more times to set the record state. Finally, <see cref="JetUpdate(JET_SESID,JET_TABLEID,byte[],int,out int)"/>
        /// is called to complete the update operation. Indexes are updated only by JetUpdate or and not during JetSetColumn.
        /// </remarks>
        public static void JetUpdate(JET_SESID sesid, JET_TABLEID tableid, byte[] bookmark, int bookmarkSize, out int actualBookmarkSize)
        {
            Api.Check(Impl.JetUpdate(sesid, tableid, bookmark, bookmarkSize, out actualBookmarkSize));
        }

        /// <summary>
        /// The JetUpdate function performs an update operation including inserting a new row into
        /// a table or updating an existing row. Deleting a table row is performed by calling
        /// <see cref="JetDelete"/>.
        /// </summary>
        /// <param name="sesid">The session which started the update.</param>
        /// <param name="tableid">The cursor to update. An update should be prepared.</param>
        /// <remarks>
        /// JetUpdate is the final step in performing an insert or an update. The update is begun by
        /// calling <see cref="JetPrepareUpdate"/> and then by calling
        /// <see cref="JetSetColumn(JET_SESID,JET_TABLEID,JET_COLUMNID,byte[],int,SetColumnGrbit,JET_SETINFO)"/>
        /// one or more times to set the record state. Finally, <see cref="JetUpdate(JET_SESID,JET_TABLEID,byte[],int,out int)"/>
        /// is called to complete the update operation. Indexes are updated only by JetUpdate or and not during JetSetColumn.
        /// </remarks>
        public static void JetUpdate(JET_SESID sesid, JET_TABLEID tableid)
        {
            int ignored;
            Api.Check(Impl.JetUpdate(sesid, tableid, null, 0, out ignored));
        }

        /// <summary>
        /// The JetSetColumn function modifies a single column value in a modified record to be inserted or to
        /// update the current record. It can overwrite an existing value, add a new value to a sequence of
        /// values in a multi-valued column, remove a value from a sequence of values in a multi-valued column,
        /// or update all or part of a long value (a column of type <see cref="JET_coltyp.LongText"/>
        /// or <see cref="JET_coltyp.LongBinary"/>). 
        /// </summary>
        /// <remarks>
        /// The SetColumn methods provide datatype-specific overrides which may be more efficient.
        /// </remarks>
        /// <param name="sesid">The session which is performing the update.</param>
        /// <param name="tableid">The cursor to update. An update should be prepared.</param>
        /// <param name="columnid">The columnid to set.</param>
        /// <param name="data">The data to set.</param>
        /// <param name="dataSize">The size of data to set.</param>
        /// <param name="grbit">SetColumn options.</param>
        /// <param name="setinfo">Used to specify itag or long-value offset.</param>
        /// <returns>A warning code.</returns>
        public static JET_wrn JetSetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, byte[] data, int dataSize, SetColumnGrbit grbit, JET_SETINFO setinfo)
        {
            return Api.JetSetColumn(sesid, tableid, columnid, data, dataSize, 0, grbit, setinfo);
        }

        /// <summary>
        /// Allows an application to set multiple column values in a single
        /// operation. An array of <see cref="JET_SETCOLUMN"/> structures is
        /// used to describe the set of column values to be set, and to describe
        /// input buffers for each column value to be set.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to set the columns on.</param>
        /// <param name="setcolumns">
        /// An array of <see cref="JET_SETCOLUMN"/> structures describing the
        /// data to set.
        /// </param>
        /// <param name="numColumns">
        /// Number of entries in the setcolumns parameter.
        /// </param>
        /// <returns>
        /// A warning. If the last column set has a warning, then
        /// this warning will be returned from JetSetColumns itself.
        /// </returns>
        [SecurityPermissionAttribute(SecurityAction.LinkDemand)]
        public static JET_wrn JetSetColumns(JET_SESID sesid, JET_TABLEID tableid, JET_SETCOLUMN[] setcolumns, int numColumns)
        {
            if (null == setcolumns)
            {
                throw new ArgumentNullException("setcolumns");
            }

            if (numColumns < 0 || numColumns > setcolumns.Length)
            {
                throw new ArgumentOutOfRangeException("numColumns", numColumns, "cannot be negative or greater than setcolumns.Length");
            }

            using (var gchandles = new GCHandleCollection())
            {
                unsafe
                {
                    NATIVE_SETCOLUMN* nativeSetColumns = stackalloc NATIVE_SETCOLUMN[numColumns];

                    // For performance, copy small amounts of data into a local buffer instead
                    // of pinning the data.
                    const int BufferSize = 128;
                    byte* buffer = stackalloc byte[BufferSize];
                    int bufferRemaining = BufferSize;

                    for (int i = 0; i < numColumns; ++i)
                    {
                        setcolumns[i].CheckDataSize();
                        nativeSetColumns[i] = setcolumns[i].GetNativeSetcolumn();
                        if (null == setcolumns[i].pvData)
                        {
                            nativeSetColumns[i].pvData = IntPtr.Zero;
                        }
                        else if (bufferRemaining >= setcolumns[i].cbData)
                        {
                            nativeSetColumns[i].pvData = new IntPtr(buffer);
                            Marshal.Copy(setcolumns[i].pvData, setcolumns[i].ibData, nativeSetColumns[i].pvData, setcolumns[i].cbData);
                            buffer += setcolumns[i].cbData;
                            bufferRemaining -= setcolumns[i].cbData;
                            Debug.Assert(bufferRemaining >= 0, "Buffer remaining is negative");
                        }
                        else
                        {
                            byte* pinnedBuffer = (byte*)gchandles.Add(setcolumns[i].pvData).ToPointer();
                            nativeSetColumns[i].pvData = new IntPtr(pinnedBuffer + setcolumns[i].ibData);
                        }
                    }

                    int err = Impl.JetSetColumns(sesid, tableid, nativeSetColumns, numColumns);
                    for (int i = 0; i < numColumns; ++i)
                    {
                        setcolumns[i].err = (JET_wrn)nativeSetColumns[i].err;
                    }

                    return Api.Check(err);
                }
            }
        }

        /// <summary>
        /// Explicitly reserve the ability to update a row, write lock, or to explicitly prevent a row from
        /// being updated by any other session, read lock. Normally, row write locks are acquired implicitly as a
        /// result of updating rows. Read locks are usually not required because of record versioning. However,
        /// in some cases a transaction may desire to explicitly lock a row to enforce serialization, or to ensure
        /// that a subsequent operation will succeed. 
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to use. A lock will be acquired on the current record.</param>
        /// <param name="grbit">Lock options, use this to specify which type of lock to obtain.</param>
        public static void JetGetLock(JET_SESID sesid, JET_TABLEID tableid, GetLockGrbit grbit)
        {
            Api.Check(Impl.JetGetLock(sesid, tableid, grbit));
        }

        /// <summary>
        /// Performs an atomic addition operation on one column. This function allows
        /// multiple sessions to update the same record concurrently without conflicts.
        /// Also see <seealso cref="EscrowUpdate"/>.
        /// </summary>
        /// <param name="sesid">
        /// The session to use. The session must be in a transaction.
        /// </param>
        /// <param name="tableid">The cursor to update.</param>
        /// <param name="columnid">
        /// The column to update. This must be an escrow updatable column.
        /// </param>
        /// <param name="delta">The buffer containing the addend.</param>
        /// <param name="deltaSize">The size of the addend.</param>
        /// <param name="previousValue">
        /// An output buffer that will recieve the current value of the column. This buffer
        /// can be null.
        /// </param>
        /// <param name="previousValueLength">The size of the previousValue buffer.</param>
        /// <param name="actualPreviousValueLength">Returns the actual size of the previousValue.</param>
        /// <param name="grbit">Escrow update options.</param>
        public static void JetEscrowUpdate(
            JET_SESID sesid,
            JET_TABLEID tableid,
            JET_COLUMNID columnid,
            byte[] delta,
            int deltaSize,
            byte[] previousValue,
            int previousValueLength,
            out int actualPreviousValueLength,
            EscrowUpdateGrbit grbit)
        {
            Api.Check(Impl.JetEscrowUpdate(
                sesid,
                tableid,
                columnid,
                delta,
                deltaSize,
                previousValue,
                previousValueLength,
                out actualPreviousValueLength,
                grbit));
        }

        #endregion

        #region Callbacks

        /// <summary>
        /// Allows the application to configure the database engine to issue
        /// notifications to the application for specific events. These
        /// notifications are associated with a specific table and remain in
        /// effect only until the instance containing the table is shut down
        /// using <see cref="JetTerm"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">
        /// A cursor opened on the table that the callback should be
        /// registered on.
        /// </param>
        /// <param name="cbtyp">
        /// The callback reasons for which the application wishes to receive notifications.
        /// </param>
        /// <param name="callback">The callback function.</param>
        /// <param name="context">A context that will be given to the callback.</param>
        /// <param name="callbackId">
        /// A handle that can later be used to cancel the registration of the given
        /// callback function using <see cref="JetUnregisterCallback"/>.
        /// </param>
        public static void JetRegisterCallback(
            JET_SESID sesid,
            JET_TABLEID tableid,
            JET_cbtyp cbtyp,
            JET_CALLBACK callback,
            IntPtr context,
            out JET_HANDLE callbackId)
        {
            Api.Check(Impl.JetRegisterCallback(sesid, tableid, cbtyp, callback, context, out callbackId));
        }

        /// <summary>
        /// Configures the database engine to stop issuing notifications to the
        /// application as previously requested through
        /// <see cref="JetRegisterCallback"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">
        /// A cursor opened on the table that the callback should be
        /// registered on.
        /// </param>
        /// <param name="cbtyp">
        /// The callback reasons for which the application no longer wishes to receive notifications.
        /// </param>
        /// <param name="callbackId">
        /// The handle of the registered callback that was returned by <see cref="JetRegisterCallback"/>.
        /// </param>
        public static void JetUnregisterCallback(JET_SESID sesid, JET_TABLEID tableid, JET_cbtyp cbtyp, JET_HANDLE callbackId)
        {
            Api.Check(Impl.JetUnregisterCallback(sesid, tableid, cbtyp, callbackId));
        }

        #endregion

        #region Online Maintenance

        /// <summary>
        /// Starts and stops database defragmentation tasks that improves data
        /// organization within a database.
        /// </summary>
        /// <param name="sesid">The session to use for the call.</param>
        /// <param name="dbid">The database to be defragmented.</param>
        /// <param name="tableName">
        /// Unused parameter. Defragmentation is performed for the entire database described by the given database ID.
        /// </param>
        /// <param name="passes">
        /// When starting an online defragmentation task, this parameter sets the maximum number of defragmentation
        /// passes. When stopping an online defragmentation task, this parameter is set to the number of passes
        /// performed.
        /// </param>
        /// <param name="seconds">
        /// When starting an online defragmentation task, this parameter sets
        /// the maximum time for defragmentation. When stopping an online
        /// defragmentation task, this output buffer is set to the length of
        /// time used for defragmentation.
        /// </param>
        /// <param name="grbit">Defragmentation options.</param>
        /// <returns>A warning code.</returns>
        public static JET_wrn JetDefragment(
            JET_SESID sesid,
            JET_DBID dbid,
            string tableName,
            ref int passes,
            ref int seconds,
            DefragGrbit grbit)
        {
            return Api.Check(Impl.JetDefragment(sesid, dbid, tableName, ref passes, ref seconds, grbit));
        }

        /// <summary>
        /// Starts and stops database defragmentation tasks that improves data
        /// organization within a database.
        /// </summary>
        /// <remarks>
        /// The callback passed to JetDefragment2 can be executed asynchronously.
        /// The GC doesn't know that the unmanaged code has a reference to the callback
        /// so it is important to make sure the callback isn't collected.
        /// </remarks>
        /// <param name="sesid">The session to use for the call.</param>
        /// <param name="dbid">The database to be defragmented.</param>
        /// <param name="tableName">
        /// Unused parameter. Defragmentation is performed for the entire database described by the given database ID.
        /// </param>
        /// <param name="passes">
        /// When starting an online defragmentation task, this parameter sets the maximum number of defragmentation
        /// passes. When stopping an online defragmentation task, this parameter is set to the number of passes
        /// performed.
        /// </param>
        /// <param name="seconds">
        /// When starting an online defragmentation task, this parameter sets
        /// the maximum time for defragmentation. When stopping an online
        /// defragmentation task, this output buffer is set to the length of
        /// time used for defragmentation.
        /// </param>
        /// <param name="callback">Callback function that defrag uses to report progress.</param>
        /// <param name="grbit">Defragmentation options.</param>
        /// <returns>A warning code.</returns>
        public static JET_wrn JetDefragment2(
            JET_SESID sesid, 
            JET_DBID dbid, 
            string tableName, 
            ref int passes, 
            ref int seconds, 
            JET_CALLBACK callback, 
            DefragGrbit grbit)
        {
            return Api.Check(Impl.JetDefragment2(sesid, dbid, tableName, ref passes, ref seconds, callback, grbit));
        }

        /// <summary>
        /// Performs idle cleanup tasks or checks the version store status in ESE.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="grbit">A combination of JetIdleGrbit flags.</param>
        /// <returns>An error code if the operation fails.</returns>
        public static JET_wrn JetIdle(JET_SESID sesid, IdleGrbit grbit)
        {
            return Api.Check(Impl.JetIdle(sesid, grbit));
        }

        #endregion

        #region Misc

        /// <summary>
        /// Frees memory that was allocated by a database engine call.
        /// </summary>
        /// <remarks>
        /// This method is internal because we never expose the memory
        /// allocated by ESENT to our callers.
        /// </remarks>
        /// <param name="buffer">
        /// The buffer allocated by a call to the database engine.
        /// <see cref="IntPtr.Zero"/> is acceptable, and will be ignored.
        /// </param>
        public static void JetFreeBuffer(IntPtr buffer)
        {
            Api.Check(Impl.JetFreeBuffer(buffer));
        }

        #endregion

        #region Error Handling

        /// <summary>
        /// Throw an exception if the parameter is an ESE error,
        /// returns a <see cref="JET_wrn"/> otherwise.
        /// </summary>
        /// <param name="err">The error code to check.</param>
        /// <returns>An ESENT warning code (possibly success).</returns>
        internal static JET_wrn Check(int err)
        {
            if (err < 0)
            {
                throw CreateErrorException(err);
            }

            return unchecked((JET_wrn)err);
        }

        /// <summary>
        /// Create an error exception that should be thrown for a failure.
        /// </summary>
        /// <param name="err">The error code.</param>
        /// <returns>A failure exception.</returns>
        private static Exception CreateErrorException(int err)
        {
            Debug.Assert(err < 0, "expected a negative error code");

            JET_err error = unchecked((JET_err)err);

            var handler = Api.HandleError;
            if (handler != null)
            {
                handler(error);
            }

            // We didn't throw an exception from the handler, so
            // generate the default exception.
            return EsentExceptionHelper.JetErrToException(error);            
        }

        #endregion Error Handling
    }
}
