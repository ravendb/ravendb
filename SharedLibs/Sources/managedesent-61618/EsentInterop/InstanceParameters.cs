//-----------------------------------------------------------------------
// <copyright file="InstanceParameters.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Globalization;
    using System.IO;
    using Microsoft.Isam.Esent.Interop.Server2003;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.Isam.Esent.Interop.Windows7;

    /// <summary>
    /// This class provides properties to set and get system parameters
    /// on an ESENT instance.
    /// </summary>
    public partial class InstanceParameters
    {
        /// <summary>
        /// The instance to set parameters on.
        /// </summary>
        private readonly JET_INSTANCE instance;

        /// <summary>
        /// The session to set parameters with.
        /// </summary>
        private readonly JET_SESID sesid;

        /// <summary>
        /// Initializes a new instance of the InstanceParameters class.
        /// </summary>
        /// <param name="instance">
        /// The instance to set parameters on. If this is JET_INSTANCE.Nil,
        /// then the settings affect the default settings of future instances.
        /// </param>
        public InstanceParameters(JET_INSTANCE instance)
        {
            this.instance = instance;
            this.sesid = JET_SESID.Nil;
        }

        /// <summary>
        /// Gets or sets the relative or absolute file system path of the
        /// folder that will contain the checkpoint file for the instance.
        /// </summary>
        public string SystemDirectory
        {
            get
            {
                return Util.AddTrailingDirectorySeparator(this.GetStringParameter(JET_param.SystemPath));
            }

            set
            {
                this.SetStringParameter(JET_param.SystemPath, Util.AddTrailingDirectorySeparator(value));
            }
        }

        /// <summary>
        /// Gets or sets the relative or absolute file system path of
        /// the folder that will contain the temporary database for the instance.
        /// </summary>
        public string TempDirectory
        {
            get
            {
                // Older versions of Esent (e.g. Windows XP) will return the
                // full path of the temporary database. Extract the directory name.
                string path = this.GetStringParameter(JET_param.TempPath);
                string dir = Path.GetDirectoryName(path);
                return Util.AddTrailingDirectorySeparator(dir);
            }

            set
            {
                this.SetStringParameter(JET_param.TempPath, Util.AddTrailingDirectorySeparator(value));
            }
        }

        /// <summary>
        /// Gets or sets the relative or absolute file system path of the
        /// folder that will contain the transaction logs for the instance.
        /// </summary>
        public string LogFileDirectory
        {
            get
            {
                return Util.AddTrailingDirectorySeparator(this.GetStringParameter(JET_param.LogFilePath));
            }

            set
            {
                this.SetStringParameter(JET_param.LogFilePath, Util.AddTrailingDirectorySeparator(value));
            }
        }

        /// <summary>
        /// Gets or sets the relative or absolute file system path of the
        /// a folder where crash recovery or a restore operation can find
        /// the databases referenced in the transaction log in the
        /// specified folder.
        /// </summary>
        /// <remarks>
        /// This parameter is ignored on Windows XP.
        /// </remarks>
        public string AlternateDatabaseRecoveryDirectory
        {
            get
            {
                if (EsentVersion.SupportsServer2003Features)
                {
                    return
                        Util.AddTrailingDirectorySeparator(
                            this.GetStringParameter(Server2003Param.AlternateDatabaseRecoveryPath));
                }

                return null;
            }

            set
            {
                if (EsentVersion.SupportsServer2003Features)
                {
                    this.SetStringParameter(
                        Server2003Param.AlternateDatabaseRecoveryPath, Util.AddTrailingDirectorySeparator(value));
                }
            }
        }

        /// <summary>
        /// Gets or sets the three letter prefix used for many of the files used by
        /// the database engine. For example, the checkpoint file is called EDB.CHK by
        /// default because EDB is the default base name.
        /// </summary>
        public string BaseName
        {
            get
            {
                return this.GetStringParameter(JET_param.BaseName);
            }

            set
            {
                this.SetStringParameter(JET_param.BaseName, value);
            }
        }

        /// <summary>
        /// Gets or sets an application specific string that will be added to
        /// any event log messages that are emitted by the database engine. This allows
        /// easy correlation of event log messages with the source application. By default
        /// the host application executable name will be used.
        /// </summary>
        public string EventSource
        {
            get
            {
                return this.GetStringParameter(JET_param.EventSource);
            }

            set
            {
                this.SetStringParameter(JET_param.EventSource, value);
            }
        }

        /// <summary>
        /// Gets or sets the number of sessions resources reserved for this instance.
        /// A session resource directly corresponds to a JET_SESID.
        /// </summary>
        public int MaxSessions
        {
            get
            {
                return this.GetIntegerParameter(JET_param.MaxSessions);
            }

            set
            {
                this.SetIntegerParameter(JET_param.MaxSessions, value);
            }
        }

        /// <summary>
        /// Gets or sets the number of B+ Tree resources reserved for this instance.
        /// </summary>
        public int MaxOpenTables
        {
            get
            {
                return this.GetIntegerParameter(JET_param.MaxOpenTables);
            }

            set
            {
                this.SetIntegerParameter(JET_param.MaxOpenTables, value);
            }
        }

        /// <summary>
        /// Gets or sets the number of cursor resources reserved for this instance.
        /// A cursor resource directly corresponds to a JET_TABLEID.
        /// </summary>
        public int MaxCursors
        {
            get
            {
                return this.GetIntegerParameter(JET_param.MaxCursors);
            }

            set
            {
                this.SetIntegerParameter(JET_param.MaxCursors, value);
            }
        }

        /// <summary>
        /// Gets or sets the maximum number of version store pages reserved
        /// for this instance.
        /// </summary>
        public int MaxVerPages
        {
            get
            {
                return this.GetIntegerParameter(JET_param.MaxVerPages);
            }

            set
            {
                this.SetIntegerParameter(JET_param.MaxVerPages, value);
            }
        }

        /// <summary>
        /// Gets or sets the preferred number of version store pages reserved
        /// for this instance. If the size of the version store exceeds this
        /// threshold then any information that is only used for optional
        /// background tasks, such as reclaiming deleted space in the database,
        /// is instead sacrificed to preserve room for transactional information.
        /// </summary>
        public int PreferredVerPages
        {
            get
            {
                return this.GetIntegerParameter(JET_param.PreferredVerPages);
            }

            set
            {
                this.SetIntegerParameter(JET_param.PreferredVerPages, value);
            }
        }

        /// <summary>
        /// Gets or sets the the number of background cleanup work items that
        /// can be queued to the database engine thread pool at any one time.
        /// </summary>
        public int VersionStoreTaskQueueMax
        {
            get
            {
                return this.GetIntegerParameter(JET_param.VersionStoreTaskQueueMax);
            }

            set
            {
                this.SetIntegerParameter(JET_param.VersionStoreTaskQueueMax, value);
            }
        }

        /// <summary>
        /// Gets or sets the number of temporary table resources for use
        /// by an instance. This setting will affect how many temporary tables can be used at
        /// the same time. If this system parameter is set to zero then no temporary database
        /// will be created and any activity that requires use of the temporary database will
        /// fail. This setting can be useful to avoid the I/O required to create the temporary
        /// database if it is known that it will not be used.
        /// </summary>
        /// <remarks>
        /// The use of a temporary table also requires a cursor resource.
        /// </remarks>
        public int MaxTemporaryTables
        {
            get
            {
                return this.GetIntegerParameter(JET_param.MaxTemporaryTables);
            }

            set
            {
                this.SetIntegerParameter(JET_param.MaxTemporaryTables, value);
            }
        }

        /// <summary>
        /// Gets or sets the size of the transaction log files. This parameter
        /// should be set in units of 1024 bytes (e.g. a setting of 2048 will
        /// give 2MB logfiles).
        /// </summary>
        public int LogFileSize
        {
            get
            {
                return this.GetIntegerParameter(JET_param.LogFileSize);
            }

            set
            {
                this.SetIntegerParameter(JET_param.LogFileSize, value);
            }
        }

        /// <summary>
        /// Gets or sets the amount of memory used to cache log records
        /// before they are written to the transaction log file. The unit for this
        /// parameter is the sector size of the volume that holds the transaction log files.
        /// The sector size is almost always 512 bytes, so it is safe to assume that size
        /// for the unit. This parameter has an impact on performance. When the database
        /// engine is under heavy update load, this buffer can become full very rapidly.
        /// A larger cache size for the transaction log file is critical for good update
        /// performance under such a high load condition. The default is known to be too small
        /// for this case.
        /// Do not set this parameter to a number of buffers that is larger (in bytes) than
        /// half the size of a transaction log file.
        /// </summary>
        public int LogBuffers
        {
            get
            {
                return this.GetIntegerParameter(JET_param.LogBuffers);
            }

            set
            {
                this.SetIntegerParameter(JET_param.LogBuffers, value);
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether circular logging is on.
        /// When circular logging is off, all transaction log files that are generated
        /// are retained on disk until they are no longer needed because a full backup of the
        /// database has been performed. When circular logging is on, only transaction log files
        /// that are younger than the current checkpoint are retained on disk. The benefit of
        /// this mode is that backups are not required to retire old transaction log files. 
        /// </summary>
        public bool CircularLog
        {
            get
            {
                return this.GetBoolParameter(JET_param.CircularLog);
            }

            set
            {
                this.SetBoolParameter(JET_param.CircularLog, value);
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether JetInit fails when the database
        /// engine is configured to start using transaction log files on disk
        /// that are of a different size than what is configured. Normally,
        /// <see cref="Api.JetInit"/> will successfully recover the databases
        /// but will fail with <see cref="JET_err.LogFileSizeMismatchDatabasesConsistent"/>
        /// to indicate that the log file size is misconfigured. However, when
        /// this parameter is set to true then the database engine will silently
        /// delete all the old log files, start a new set of transaction log files
        /// using the configured log file size. This parameter is useful when the
        /// application wishes to transparently change its transaction log file
        /// size yet still work transparently in upgrade and restore scenarios.
        /// </summary>
        public bool CleanupMismatchedLogFiles
        {
            get
            {
                return this.GetBoolParameter(JET_param.CleanupMismatchedLogFiles);
            }

            set
            {
                this.SetBoolParameter(JET_param.CleanupMismatchedLogFiles, value);
            }
        }

        /// <summary>
        /// Gets or sets the initial size of the temporary database. The size is in
        /// database pages. A size of zero indicates that the default size of an ordinary
        /// database should be used. It is often desirable for small applications to configure
        /// the temporary database to be as small as possible. Setting this parameter to
        /// <see cref="SystemParameters.PageTempDBSmallest"/> will achieve the smallest
        /// temporary database possible.
        /// </summary>
        public int PageTempDBMin
        {
            get
            {
                return this.GetIntegerParameter(JET_param.PageTempDBMin);
            }

            set
            {
                this.SetIntegerParameter(JET_param.PageTempDBMin, value);
            }
        }

        /// <summary>
        /// Gets or sets the threshold in bytes for about how many transaction log
        /// files will need to be replayed after a crash. If circular logging is enabled using
        /// CircularLog then this parameter will also control the approximate amount
        /// of transaction log files that will be retained on disk.
        /// </summary>
        public int CheckpointDepthMax
        {
            get
            {
                return this.GetIntegerParameter(JET_param.CheckpointDepthMax);
            }

            set
            {
                this.SetIntegerParameter(JET_param.CheckpointDepthMax, value);
            }
        }

        /// <summary>
        /// Gets or sets the number of pages that are added to a database file each
        /// time it needs to grow to accommodate more data.
        /// </summary>
        public int DbExtensionSize
        {
            get
            {
                return this.GetIntegerParameter(JET_param.DbExtensionSize);
            }

            set
            {
                this.SetIntegerParameter(JET_param.DbExtensionSize, value);
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether crash recovery is on.
        /// </summary>
        public bool Recovery
        {
            get
            {
                return 0 == String.Compare(this.GetStringParameter(JET_param.Recovery), "on", StringComparison.OrdinalIgnoreCase);
            }

            set
            {
                if (value)
                {
                    this.SetStringParameter(JET_param.Recovery, "on");
                }
                else
                {
                    this.SetStringParameter(JET_param.Recovery, "off");
                }
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether JetAttachDatabase will check for
        /// indexes that were build using an older version of the NLS library in the
        /// operating system.
        /// </summary>
        public bool EnableIndexChecking
        {
            get
            {
                return this.GetBoolParameter(JET_param.EnableIndexChecking);
            }

            set
            {
                this.SetBoolParameter(JET_param.EnableIndexChecking, value);
            }
        }

        /// <summary>
        /// Gets or sets the name of the event log the database engine uses for its event log
        /// messages. By default, all event log messages will go to the Application event log. If the registry
        /// key name for another event log is configured then the event log messages will go there instead.
        /// </summary>  
        public string EventSourceKey
        {
            get
            {
                return this.GetStringParameter(JET_param.EventSourceKey);  
            }

            set
            {
                this.SetStringParameter(JET_param.EventSourceKey, value);
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether informational event 
        /// log messages that would ordinarily be generated by the
        /// database engine will be suppressed.
        /// </summary>
        public bool NoInformationEvent
        {
            get
            {
                return this.GetBoolParameter(JET_param.NoInformationEvent);
            }

            set
            {
                this.SetBoolParameter(JET_param.NoInformationEvent, value);
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether only one database is allowed to
        /// be opened using JetOpenDatabase by a given session at one time.
        /// The temporary database is excluded from this restriction. 
        /// </summary>
        public bool OneDatabasePerSession
        {
            get
            {
                return this.GetBoolParameter(JET_param.OneDatabasePerSession);
            }

            set
            {
                this.SetBoolParameter(JET_param.OneDatabasePerSession, value);
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether ESENT will silently create folders
        /// that are missing in its filesystem paths.
        /// </summary>
        public bool CreatePathIfNotExist
        {
            get
            {
                return this.GetBoolParameter(JET_param.CreatePathIfNotExist);
            }

            set
            {
                this.SetBoolParameter(JET_param.CreatePathIfNotExist, value);
            }
        }

        /// <summary>
        /// Gets or sets a value giving the number of B+ Tree resources cached by
        /// the instance after the tables they represent have been closed by
        /// the application. Large values for this parameter will cause the
        /// database engine to use more memory but will increase the speed
        /// with which a large number of tables can be opened randomly by
        /// the application. This is useful for applications that have a
        /// schema with a very large number of tables.
        /// <para>
        /// Supported on Windows Vista and up. Ignored on Windows XP and
        /// Windows Server 2003.
        /// </para>
        /// </summary>
        public int CachedClosedTables
        {
            get
            {
                if (EsentVersion.SupportsVistaFeatures)
                {
                    return this.GetIntegerParameter(VistaParam.CachedClosedTables);
                }

                return 0;
            }

            set
            {
                if (EsentVersion.SupportsVistaFeatures)
                {
                    this.SetIntegerParameter(VistaParam.CachedClosedTables, value);
                }
            }
        }

        /// <summary>
        /// Gets or sets a the number of logs that esent will defer database
        /// flushes for. This can be used to increase database recoverability if
        /// failures cause logfiles to be lost.
        /// <para>
        /// Supported on Windows 7 and up. Ignored on Windows XP,
        /// Windows Server 2003, Windows Vista and Windows Server 2008.
        /// </para>
        /// </summary>
        public int WaypointLatency
        {
            get
            {
                if (EsentVersion.SupportsWindows7Features)
                {
                    return this.GetIntegerParameter(Windows7Param.WaypointLatency);
                }

                // older versions have no waypoint
                return 0;
            }

            set
            {
                if (EsentVersion.SupportsWindows7Features)
                {
                    this.SetIntegerParameter(Windows7Param.WaypointLatency, value);
                }
            }
        }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="InstanceParameters"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="InstanceParameters"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "InstanceParameters (0x{0:x})", this.instance.Value);
        }

        /// <summary>
        /// Set a system parameter which is a string.
        /// </summary>
        /// <param name="param">The parameter to set.</param>
        /// <param name="value">The value to set.</param>
        private void SetStringParameter(JET_param param, string value)
        {
            Api.JetSetSystemParameter(this.instance, this.sesid, param, 0, value);
        }

        /// <summary>
        /// Get a system parameter which is a string.
        /// </summary>
        /// <param name="param">The parameter to get.</param>
        /// <returns>The value of the parameter.</returns>
        private string GetStringParameter(JET_param param)
        {
            int ignored = 0;
            string value;
            Api.JetGetSystemParameter(this.instance, this.sesid, param, ref ignored, out value, 1024);
            return value;
        }

        /// <summary>
        /// Set a system parameter which is an integer.
        /// </summary>
        /// <param name="param">The parameter to set.</param>
        /// <param name="value">The value to set.</param>
        private void SetIntegerParameter(JET_param param, int value)
        {
            Api.JetSetSystemParameter(this.instance, this.sesid, param, value, null);
        }

        /// <summary>
        /// Get a system parameter which is an integer.
        /// </summary>
        /// <param name="param">The parameter to get.</param>
        /// <returns>The value of the parameter.</returns>
        private int GetIntegerParameter(JET_param param)
        {
            int value = 0;
            string ignored;
            Api.JetGetSystemParameter(this.instance, this.sesid, param, ref value, out ignored, 0);
            return value;
        }

        /// <summary>
        /// Set a system parameter which is a boolean.
        /// </summary>
        /// <param name="param">The parameter to set.</param>
        /// <param name="value">The value to set.</param>
        private void SetBoolParameter(JET_param param, bool value)
        {
            if (value)
            {
                Api.JetSetSystemParameter(this.instance, this.sesid, param, 1, null);
            }
            else
            {
                Api.JetSetSystemParameter(this.instance, this.sesid, param, 0, null);
            }
        }

        /// <summary>
        /// Get a system parameter which is a boolean.
        /// </summary>
        /// <param name="param">The parameter to get.</param>
        /// <returns>The value of the parameter.</returns>
        private bool GetBoolParameter(JET_param param)
        {
            int value = 0;
            string ignored;
            Api.JetGetSystemParameter(this.instance, this.sesid, param, ref value, out ignored, 0);
            return value != 0;
        }
    }
}