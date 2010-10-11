//-----------------------------------------------------------------------
// <copyright file="JetApi.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop.Implementation
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;
    using System.Globalization;
    using System.Runtime.CompilerServices;
    using System.Runtime.InteropServices;
    using System.Text;
    using Microsoft.Isam.Esent.Interop.Server2003;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.Isam.Esent.Interop.Windows7;
    using Win32 = Microsoft.Isam.Esent.Interop.Win32;

    /// <summary>
    /// Calls to the ESENT interop layer. These calls take the managed types (e.g. JET_SESID) and
    /// return errors.
    /// </summary>
    internal sealed partial class JetApi : IJetApi
    {
        /// <summary>
        /// API call tracing.
        /// </summary>
        private readonly TraceSwitch traceSwitch = new TraceSwitch("ESENT P/Invoke", "P/Invoke calls to ESENT");

        /// <summary>
        /// The version of esent. If this is zero then it is looked up
        /// with <see cref="JetGetVersion"/>.
        /// </summary>
        private readonly uint versionOverride;

        /// <summary>
        /// Callback wrapper collection. This is used for long-running callbacks
        /// (callbacks which can be called after the API call returns). Create a
        /// wrapper here and occasionally clean them up.
        /// </summary>
        private readonly CallbackWrappers callbackWrappers = new CallbackWrappers();

        /// <summary>
        /// Initializes static members of the JetApi class.
        /// </summary>
        static JetApi()
        {
            // Prepare these methods for inclusion in a constrained execution region (CER).
            // This is needed by the Instance class. Instance accesses these methods virtually
            // so RemoveUnnecessaryCode won't be able to prepare them.
            RuntimeHelpers.PrepareMethod(typeof(JetApi).GetMethod("JetCreateInstance").MethodHandle);
            RuntimeHelpers.PrepareMethod(typeof(JetApi).GetMethod("JetCreateInstance2").MethodHandle);
            RuntimeHelpers.PrepareMethod(typeof(JetApi).GetMethod("JetInit").MethodHandle);
            RuntimeHelpers.PrepareMethod(typeof(JetApi).GetMethod("JetInit2").MethodHandle);
            RuntimeHelpers.PrepareMethod(typeof(JetApi).GetMethod("JetTerm").MethodHandle);
            RuntimeHelpers.PrepareMethod(typeof(JetApi).GetMethod("JetTerm2").MethodHandle);
        }

        /// <summary>
        /// Initializes a new instance of the JetApi class. This allows the version
        /// to be set.
        /// </summary>
        /// <param name="version">
        /// The version of Esent. This is used to override the results of
        /// <see cref="JetGetVersion"/>.
        /// </param>
        public JetApi(uint version)
        {
            this.versionOverride = version;
            this.DetermineCapabilities();
        }

        /// <summary>
        /// Initializes a new instance of the JetApi class.
        /// </summary>
        public JetApi()
        {
            this.DetermineCapabilities();
        }

        /// <summary>
        /// Gets the capabilities of this implementation of ESENT.
        /// </summary>
        public JetCapabilities Capabilities { get; private set; }

        #region Init/Term

        /// <summary>
        /// Allocates a new instance of the database engine.
        /// </summary>
        /// <param name="instance">Returns the new instance.</param>
        /// <param name="name">The name of the instance. Names must be unique.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetCreateInstance(out JET_INSTANCE instance, string name)
        {
            this.TraceFunctionCall("JetCreateInstance");
            instance.Value = IntPtr.Zero;
            return this.Err(NativeMethods.JetCreateInstance(out instance.Value, name));
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
        /// <returns>An error if the call fails.</returns>
        public int JetCreateInstance2(out JET_INSTANCE instance, string name, string displayName, CreateInstanceGrbit grbit)
        {
            this.TraceFunctionCall("JetCreateInstance2");
            instance.Value = IntPtr.Zero;
            return this.Err(NativeMethods.JetCreateInstance2(out instance.Value, name, displayName, (uint)grbit));
        }

        /// <summary>
        /// Initialize the ESENT database engine.
        /// </summary>
        /// <param name="instance">
        /// The instance to initialize. If an instance hasn't been
        /// allocated then a new one is created and the engine
        /// will operate in single-instance mode.
        /// </param>
        /// <returns>An error if the call fails.</returns>
        public int JetInit(ref JET_INSTANCE instance)
        {
            this.TraceFunctionCall("JetInit");
            return this.Err(NativeMethods.JetInit(ref instance.Value));
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
        /// <returns>An error or a warning.</returns>
        public int JetInit2(ref JET_INSTANCE instance, InitGrbit grbit)
        {
            this.TraceFunctionCall("JetInit2");
            return this.Err(NativeMethods.JetInit2(ref instance.Value, (uint)grbit));
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
        /// <returns>An error code if the call fails.</returns>
        public int JetGetInstanceInfo(out int numInstances, out JET_INSTANCE_INFO[] instances)
        {
            this.TraceFunctionCall("JetGetInstanceInfo");

            unsafe
            {
                uint nativeNumInstance;
                NATIVE_INSTANCE_INFO* nativeInstanceInfos;
                int err;
                if (this.Capabilities.SupportsUnicodePaths)
                {
                    err = NativeMethods.JetGetInstanceInfoW(out nativeNumInstance, out nativeInstanceInfos);
                    instances = this.ConvertInstanceInfosUnicode(nativeNumInstance, nativeInstanceInfos);
                }
                else
                {
                    err = NativeMethods.JetGetInstanceInfo(out nativeNumInstance, out nativeInstanceInfos);
                    instances = this.ConvertInstanceInfosAscii(nativeNumInstance, nativeInstanceInfos);                    
                }

                numInstances = instances.Length;
                return this.Err(err);                
            }
        }

        /// <summary>
        /// Prevents streaming backup-related activity from continuing on a
        /// specific running instance, thus ending the streaming backup in
        /// a predictable way.
        /// </summary>
        /// <param name="instance">The instance to use.</param>
        /// <returns>An error code.</returns>
        public int JetStopBackupInstance(JET_INSTANCE instance)
        {
            this.TraceFunctionCall("JetStopBackupInstance");
            return this.Err(NativeMethods.JetStopBackupInstance(instance.Value));
        }

        /// <summary>
        /// Prepares an instance for termination.
        /// </summary>
        /// <param name="instance">The (running) instance to use.</param>
        /// <returns>An error code.</returns>
        public int JetStopServiceInstance(JET_INSTANCE instance)
        {
            this.TraceFunctionCall("JetStopServiceInstance");
            return this.Err(NativeMethods.JetStopServiceInstance(instance.Value));            
        }

        /// <summary>
        /// Terminate an instance that was created with <see cref="IJetApi.JetInit"/> or
        /// <see cref="IJetApi.JetCreateInstance"/>.
        /// </summary>
        /// <param name="instance">The instance to terminate.</param>
        /// <returns>An error or warning.</returns>
        public int JetTerm(JET_INSTANCE instance)
        {
            this.TraceFunctionCall("JetTerm");
            this.callbackWrappers.Collect();
            if (JET_INSTANCE.Nil != instance)
            {
                return this.Err(NativeMethods.JetTerm(instance.Value));
            }

            return (int)JET_err.Success;
        }

        /// <summary>
        /// Terminate an instance that was created with <see cref="IJetApi.JetInit"/> or
        /// <see cref="IJetApi.JetCreateInstance"/>.
        /// </summary>
        /// <param name="instance">The instance to terminate.</param>
        /// <param name="grbit">Termination options.</param>
        /// <returns>An error or warning.</returns>
        public int JetTerm2(JET_INSTANCE instance, TermGrbit grbit)
        {
            this.TraceFunctionCall("JetTerm2");
            this.callbackWrappers.Collect();
            if (JET_INSTANCE.Nil != instance)
            {
                return this.Err(NativeMethods.JetTerm2(instance.Value, (uint)grbit));
            }

            return (int)JET_err.Success;
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
        /// <returns>An error or warning.</returns>
        public int JetSetSystemParameter(JET_INSTANCE instance, JET_SESID sesid, JET_param paramid, int paramValue, string paramString)
        {
            this.TraceFunctionCall("JetSetSystemParameter");
            unsafe
            {
                IntPtr* pinstance = (IntPtr.Zero == instance.Value) ? null : &instance.Value;
                if (this.Capabilities.SupportsUnicodePaths)
                {
                    return this.Err(NativeMethods.JetSetSystemParameterW(pinstance, sesid.Value, (uint)paramid, new IntPtr(paramValue), paramString));
                }

                return this.Err(NativeMethods.JetSetSystemParameter(pinstance, sesid.Value, (uint)paramid, new IntPtr(paramValue), paramString));                
            }
        }

        /// <summary>
        /// Sets database configuration options. This overload is used when the
        /// parameter being set is of type JET_CALLBACK.
        /// </summary>
        /// <param name="instance">
        /// The instance to set the option on or <see cref="JET_INSTANCE.Nil"/>
        /// to set the option on all instances.
        /// </param>
        /// <param name="sesid">The session to use.</param>
        /// <param name="paramid">The parameter to set.</param>
        /// <param name="paramValue">The value of the parameter to set.</param>
        /// <param name="paramString">The value of the string parameter to set.</param>
        /// <returns>An error or warning.</returns>
        public int JetSetSystemParameter(JET_INSTANCE instance, JET_SESID sesid, JET_param paramid, JET_CALLBACK paramValue, string paramString)
        {
            this.TraceFunctionCall("JetSetSystemParameter");

            unsafe
            {
                // We are interested in the callback, not the string so we always use the ASCII API.
                IntPtr* pinstance = (IntPtr.Zero == instance.Value) ? null : &instance.Value;

                if (null == paramValue)
                {
                    return
                        this.Err(
                            NativeMethods.JetSetSystemParameter(
                                pinstance,
                                sesid.Value,
                                (uint) paramid,
                                IntPtr.Zero,
                                paramString));
                }

                JetCallbackWrapper wrapper = this.callbackWrappers.Add(paramValue);
                this.callbackWrappers.Collect();
                IntPtr functionPointer = Marshal.GetFunctionPointerForDelegate(wrapper.NativeCallback);
#if DEBUG
                GC.Collect();
#endif
                return this.Err(
                        NativeMethods.JetSetSystemParameter(
                            pinstance,
                            sesid.Value,
                            (uint) paramid,
                            functionPointer,
                            paramString));
            }
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
        /// <returns>An error or warning.</returns>
        public int JetGetSystemParameter(JET_INSTANCE instance, JET_SESID sesid, JET_param paramid, ref int paramValue, out string paramString, int maxParam)
        {
            this.TraceFunctionCall("JetGetSystemParameter");
            this.CheckNotNegative(maxParam, "maxParam");

            uint cbMax = checked((uint)(this.Capabilities.SupportsUnicodePaths ? maxParam * sizeof(char) : maxParam));

            var intValue = new IntPtr(paramValue);
            var sb = new StringBuilder(maxParam);
            int err;
            if (this.Capabilities.SupportsUnicodePaths)
            {
                err = this.Err(NativeMethods.JetGetSystemParameterW(instance.Value, sesid.Value, (uint)paramid, ref intValue, sb, cbMax));
            }
            else
            {
                err = this.Err(NativeMethods.JetGetSystemParameter(instance.Value, sesid.Value, (uint)paramid, ref intValue, sb, cbMax));      
            }

            paramString = sb.ToString();
            paramValue = intValue.ToInt32();
            return err;
        }

        /// <summary>
        /// Retrieves the version of the database engine.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="version">Returns the version number of the database engine.</param>
        /// <returns>An error code if the call fails.</returns>
        public int JetGetVersion(JET_SESID sesid, out uint version)
        {
            this.TraceFunctionCall("JetGetVersion");
            uint nativeVersion;
            int err;

            if (0 != this.versionOverride)
            {
                // We have an explicitly set version
                Trace.WriteLineIf(
                    this.traceSwitch.TraceVerbose, String.Format(CultureInfo.InvariantCulture, "JetGetVersion overridden with 0x{0:X}", this.versionOverride));
                nativeVersion = this.versionOverride;
                err = 0;
            }
            else
            {
                // Get the version from Esent
                err = this.Err(NativeMethods.JetGetVersion(sesid.Value, out nativeVersion));                
            }

            version = nativeVersion;
            return err;
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
        /// <returns>An error or warning.</returns>
        public int JetCreateDatabase(JET_SESID sesid, string database, string connect, out JET_DBID dbid, CreateDatabaseGrbit grbit)
        {
            this.TraceFunctionCall("JetCreateDatabase");
            this.CheckNotNull(database, "database");

            dbid = JET_DBID.Nil;
            if (this.Capabilities.SupportsUnicodePaths)
            {
                return this.Err(NativeMethods.JetCreateDatabaseW(sesid.Value, database, connect, out dbid.Value, (uint)grbit));
            }

            return this.Err(NativeMethods.JetCreateDatabase(sesid.Value, database, connect, out dbid.Value, (uint)grbit));
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
        /// <returns>An error or warning.</returns>
        public int JetCreateDatabase2(JET_SESID sesid, string database, int maxPages, out JET_DBID dbid, CreateDatabaseGrbit grbit)
        {
            this.TraceFunctionCall("JetCreateDatabase2");
            this.CheckNotNull(database, "database");
            this.CheckNotNegative(maxPages, "maxPages");

            dbid = JET_DBID.Nil;
            uint cpgDatabaseSizeMax = checked((uint)maxPages);
            if (this.Capabilities.SupportsUnicodePaths)
            {
                return this.Err(NativeMethods.JetCreateDatabase2W(sesid.Value, database, cpgDatabaseSizeMax, out dbid.Value, (uint)grbit));
            }

            return this.Err(NativeMethods.JetCreateDatabase2(sesid.Value, database, cpgDatabaseSizeMax, out dbid.Value, (uint)grbit));
        }

        /// <summary>
        /// Attaches a database file for use with a database instance. In order to use the
        /// database, it will need to be subsequently opened with <see cref="IJetApi.JetOpenDatabase"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="database">The database to attach.</param>
        /// <param name="grbit">Attach options.</param>
        /// <returns>An error or warning.</returns>
        public int JetAttachDatabase(JET_SESID sesid, string database, AttachDatabaseGrbit grbit)
        {
            this.TraceFunctionCall("JetAttachDatabase");
            this.CheckNotNull(database, "database");

            if (this.Capabilities.SupportsUnicodePaths)
            {
                return this.Err(NativeMethods.JetAttachDatabaseW(sesid.Value, database, (uint)grbit));                
            }

            return this.Err(NativeMethods.JetAttachDatabase(sesid.Value, database, (uint)grbit));
        }

        /// <summary>
        /// Attaches a database file for use with a database instance. In order to use the
        /// database, it will need to be subsequently opened with <see cref="JetOpenDatabase"/>.
        /// <seealso cref="JetCreateDatabase2"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="database">The database to attach.</param>
        /// <param name="maxPages">
        /// The maximum size, in database pages, of the database. Passing 0 means there is
        /// no enforced maximum.
        /// </param>
        /// <param name="grbit">Attach options.</param>
        /// <returns>An error or warning.</returns>
        public int JetAttachDatabase2(JET_SESID sesid, string database, int maxPages, AttachDatabaseGrbit grbit)
        {
            this.TraceFunctionCall("JetAttachDatabase2");
            this.CheckNotNull(database, "database");
            this.CheckNotNegative(maxPages, "maxPages");

            if (this.Capabilities.SupportsUnicodePaths)
            {
                return this.Err(NativeMethods.JetAttachDatabase2W(sesid.Value, database, checked((uint)maxPages), (uint)grbit));
            }

            return this.Err(NativeMethods.JetAttachDatabase2(sesid.Value, database, checked((uint)maxPages), (uint)grbit));            
        }

        /// <summary>
        /// Opens a database previously attached with <see cref="IJetApi.JetAttachDatabase"/>,
        /// for use with a database session. This function can be called multiple times
        /// for the same database.
        /// </summary>
        /// <param name="sesid">The session that is opening the database.</param>
        /// <param name="database">The database to open.</param>
        /// <param name="connect">Reserved for future use.</param>
        /// <param name="dbid">Returns the dbid of the attached database.</param>
        /// <param name="grbit">Open database options.</param>
        /// <returns>An error or warning.</returns>
        public int JetOpenDatabase(JET_SESID sesid, string database, string connect, out JET_DBID dbid, OpenDatabaseGrbit grbit)
        {
            this.TraceFunctionCall("JetOpenDatabase");
            this.CheckNotNull(database, "database");
            dbid = JET_DBID.Nil;

            if (this.Capabilities.SupportsUnicodePaths)
            {
                return this.Err(NativeMethods.JetOpenDatabaseW(sesid.Value, database, connect, out dbid.Value, (uint)grbit));                
            }

            return this.Err(NativeMethods.JetOpenDatabase(sesid.Value, database, connect, out dbid.Value, (uint)grbit));
        }

        /// <summary>
        /// Closes a database file that was previously opened with <see cref="IJetApi.JetOpenDatabase"/> or
        /// created with <see cref="IJetApi.JetCreateDatabase"/>.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to close.</param>
        /// <param name="grbit">Close options.</param>
        /// <returns>An error or warning.</returns>
        public int JetCloseDatabase(JET_SESID sesid, JET_DBID dbid, CloseDatabaseGrbit grbit)
        {
            this.TraceFunctionCall("JetCloseDatabase");
            return this.Err(NativeMethods.JetCloseDatabase(sesid.Value, dbid.Value, (uint)grbit));
        }

        /// <summary>
        /// Releases a database file that was previously attached to a database session.
        /// </summary>
        /// <param name="sesid">The database session to use.</param>
        /// <param name="database">The database to detach.</param>
        /// <returns>An error or warning.</returns>
        public int JetDetachDatabase(JET_SESID sesid, string database)
        {
            this.TraceFunctionCall("JetDetachDatabase");

            if (this.Capabilities.SupportsUnicodePaths)
            {
                return this.Err(NativeMethods.JetDetachDatabaseW(sesid.Value, database));                
            }

            return this.Err(NativeMethods.JetDetachDatabase(sesid.Value, database));
        }

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
        /// <returns>An error code.</returns>
        public int JetCompact(
            JET_SESID sesid,
            string sourceDatabase,
            string destinationDatabase,
            JET_PFNSTATUS statusCallback,
            object ignored,
            CompactGrbit grbit)
        {
            this.TraceFunctionCall("JetCompact");
            this.CheckNotNull(sourceDatabase, "sourceDatabase");
            this.CheckNotNull(destinationDatabase, "destinationDatabase");
            if (null != ignored)
            {
                throw new ArgumentException("must be null", "ignored");
            }

            var callbackWrapper = new StatusCallbackWrapper(statusCallback);
            IntPtr functionPointer = (null == statusCallback) ? IntPtr.Zero : Marshal.GetFunctionPointerForDelegate(callbackWrapper.NativeCallback);
#if DEBUG
            GC.Collect();
#endif

            int err;
            if (this.Capabilities.SupportsUnicodePaths)
            {
                err = this.Err(NativeMethods.JetCompactW(
                            sesid.Value, sourceDatabase, destinationDatabase, functionPointer, IntPtr.Zero, (uint)grbit));
            }
            else
            {
                err = this.Err(NativeMethods.JetCompact(
                            sesid.Value, sourceDatabase, destinationDatabase, functionPointer, IntPtr.Zero, (uint)grbit));
            }

            callbackWrapper.ThrowSavedException();
            return err;
        }

        /// <summary>
        /// Extends the size of a database that is currently open.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to grow.</param>
        /// <param name="desiredPages">The desired size of the database, in pages.</param>
        /// <param name="actualPages">
        /// The size of the database, in pages, after the call.
        /// </param>
        /// <returns>An error if the call fails.</returns>
        public int JetGrowDatabase(JET_SESID sesid, JET_DBID dbid, int desiredPages, out int actualPages)
        {
            this.TraceFunctionCall("JetGrowDatabase");
            this.CheckNotNegative(desiredPages, "desiredPages");
            uint actualPagesNative;
            int err = this.Err(NativeMethods.JetGrowDatabase(
                        sesid.Value, dbid.Value, checked((uint)desiredPages), out actualPagesNative));
            actualPages = checked((int)actualPagesNative);
            return err;
        }

        /// <summary>
        /// Extends the size of a database that is currently open.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="database">The name of the database to grow.</param>
        /// <param name="desiredPages">The desired size of the database, in pages.</param>
        /// <param name="actualPages">
        /// The size of the database, in pages, after the call.
        /// </param>
        /// <returns>An error if the call fails.</returns>
        public int JetSetDatabaseSize(JET_SESID sesid, string database, int desiredPages, out int actualPages)
        {
            this.TraceFunctionCall("JetSetDatabaseSize");
            this.CheckNotNegative(desiredPages, "desiredPages");
            this.CheckNotNull(database, "database");
            uint actualPagesNative;
            int err;
            if (this.Capabilities.SupportsUnicodePaths)
            {
                err = this.Err(NativeMethods.JetSetDatabaseSizeW(
                            sesid.Value, database, checked((uint)desiredPages), out actualPagesNative));
            }
            else
            {
                err = this.Err(NativeMethods.JetSetDatabaseSize(
                            sesid.Value, database, checked((uint)desiredPages), out actualPagesNative));                
            }

            actualPages = checked((int)actualPagesNative);
            return err;            
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
        /// <returns>An error code.</returns>
        public int JetBackupInstance(
            JET_INSTANCE instance, string destination, BackupGrbit grbit, JET_PFNSTATUS statusCallback)
        {
            this.TraceFunctionCall("JetBackupInstance");

            var callbackWrapper = new StatusCallbackWrapper(statusCallback);
            IntPtr functionPointer = (null == statusCallback) ? IntPtr.Zero : Marshal.GetFunctionPointerForDelegate(callbackWrapper.NativeCallback);
#if DEBUG
            GC.Collect();
#endif
            int err;
            if (this.Capabilities.SupportsUnicodePaths)
            {
                err = this.Err(NativeMethods.JetBackupInstanceW(instance.Value, destination, (uint)grbit, functionPointer));
            }
            else
            {
                err = this.Err(NativeMethods.JetBackupInstance(instance.Value, destination, (uint)grbit, functionPointer));                
            }

            callbackWrapper.ThrowSavedException();
            return err;
        }

        /// <summary>
        /// Restores and recovers a streaming backup of an instance including all
        /// the attached databases. It is designed to work with a backup created
        /// with the <see cref="Api.JetBackupInstance"/> function. This is the
        /// simplest and most encapsulated restore function. 
        /// </summary>
        /// <param name="instance">The instance to use.</param>
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
        /// <returns>An error code.</returns>
        public int JetRestoreInstance(JET_INSTANCE instance, string source, string destination, JET_PFNSTATUS statusCallback)
        {
            this.TraceFunctionCall("JetRestoreInstance");
            this.CheckNotNull(source, "source");

            var callbackWrapper = new StatusCallbackWrapper(statusCallback);
            IntPtr functionPointer = (null == statusCallback) ? IntPtr.Zero : Marshal.GetFunctionPointerForDelegate(callbackWrapper.NativeCallback);
#if DEBUG
            GC.Collect();
#endif

            int err;
            if (this.Capabilities.SupportsUnicodePaths)
            {
                err = this.Err(NativeMethods.JetRestoreInstanceW(instance.Value, source, destination, functionPointer));                
            }
            else
            {
                err = this.Err(NativeMethods.JetRestoreInstance(instance.Value, source, destination, functionPointer));                
            }

            callbackWrapper.ThrowSavedException();
            return err;
        }

        #endregion

        #region Snapshot Backup

        /// <summary>
        /// Notifies the engine that it can resume normal IO operations after a
        /// freeze period ended with a failed snapshot.
        /// </summary>
        /// <param name="snapid">Identifier of the snapshot session.</param>
        /// <param name="grbit">Options for this call.</param>
        /// <returns>An error code.</returns>
        public int JetOSSnapshotAbort(JET_OSSNAPID snapid, SnapshotAbortGrbit grbit)
        {
            this.TraceFunctionCall("JetOSSnapshotAbort");
            this.CheckSupportsServer2003Features("JetOSSnapshotAbort");
            return this.Err(NativeMethods.JetOSSnapshotAbort(snapid.Value, (uint)grbit));
        }

        /// <summary>
        /// Notifies the engine that the snapshot session finished.
        /// </summary>
        /// <param name="snapid">The identifier of the snapshot session.</param>
        /// <param name="grbit">Snapshot end options.</param>
        /// <returns>An error code.</returns>
        public int JetOSSnapshotEnd(JET_OSSNAPID snapid, SnapshotEndGrbit grbit)
        {
            this.TraceFunctionCall("JetOSSnapshotEnd");
            this.CheckSupportsVistaFeatures("JetOSSnapshotEnd");
            return this.Err(NativeMethods.JetOSSnapshotEnd(snapid.Value, (uint)grbit));            
        }

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
        /// <returns>An error code if the call fails.</returns>
        public int JetOSSnapshotFreeze(JET_OSSNAPID snapshot, out int numInstances, out JET_INSTANCE_INFO[] instances, SnapshotFreezeGrbit grbit)
        {
            this.TraceFunctionCall("JetOSSnapshotFreeze");

            unsafe
            {
                uint nativeNumInstance;
                NATIVE_INSTANCE_INFO* nativeInstanceInfos;
                int err;
                if (this.Capabilities.SupportsUnicodePaths)
                {
                    err = NativeMethods.JetOSSnapshotFreezeW(snapshot.Value, out nativeNumInstance, out nativeInstanceInfos, (uint)grbit);
                    instances = this.ConvertInstanceInfosUnicode(nativeNumInstance, nativeInstanceInfos);
                }
                else
                {
                    err = NativeMethods.JetOSSnapshotFreeze(snapshot.Value, out nativeNumInstance, out nativeInstanceInfos, (uint)grbit);
                    instances = this.ConvertInstanceInfosAscii(nativeNumInstance, nativeInstanceInfos);
                }

                numInstances = instances.Length;
                return this.Err(err);
            }            
        }

        /// <summary>
        /// Begins the preparations for a snapshot session. A snapshot session
        /// is a short time interval in which the engine does not issue any
        /// write IOs to disk, so that the engine can participate in a volume
        /// snapshot session (when driven by a snapshot writer).
        /// </summary>
        /// <param name="snapid">Returns the ID of the snapshot session.</param>
        /// <param name="grbit">Snapshot options.</param>
        /// <returns>An error code if the call fails.</returns>
        public int JetOSSnapshotPrepare(out JET_OSSNAPID snapid, SnapshotPrepareGrbit grbit)
        {
            this.TraceFunctionCall("JetOSSnapshotPrepare");
            snapid = JET_OSSNAPID.Nil;
            return this.Err(NativeMethods.JetOSSnapshotPrepare(out snapid.Value, (uint)grbit));
        }

        /// <summary>
        /// Notifies the engine that it can resume normal IO operations after a
        /// freeze period and a successful snapshot.
        /// </summary>
        /// <param name="snapid">The ID of the snapshot.</param>
        /// <param name="grbit">Thaw options.</param>
        /// <returns>An error code if the call fails.</returns>
        public int JetOSSnapshotThaw(JET_OSSNAPID snapid, SnapshotThawGrbit grbit)
        {
            this.TraceFunctionCall("JetOSSnapshotThaw");
            return this.Err(NativeMethods.JetOSSnapshotThaw(snapid.Value, (uint)grbit));
        }

        #endregion

        #region Streaming Backup/Restore

        /// <summary>
        /// Initiates an external backup while the engine and database are online and active. 
        /// </summary>
        /// <param name="instance">The instance prepare for backup.</param>
        /// <param name="grbit">Backup options.</param>
        /// <returns>An error code if the call fails.</returns>
        public int JetBeginExternalBackupInstance(JET_INSTANCE instance, BeginExternalBackupGrbit grbit)
        {
            this.TraceFunctionCall("JetBeginExternalBackupInstance");
            return this.Err(NativeMethods.JetBeginExternalBackupInstance(instance.Value, (uint)grbit));
        }

        /// <summary>
        /// Closes a file that was opened with JetOpenFileInstance after the
        /// data from that file has been extracted using JetReadFileInstance.
        /// </summary>
        /// <param name="instance">The instance to use.</param>
        /// <param name="handle">The handle to close.</param>
        /// <returns>An error code if the call fails.</returns>
        public int JetCloseFileInstance(JET_INSTANCE instance, JET_HANDLE handle)
        {
            this.TraceFunctionCall("JetCloseFileInstance");
            return this.Err(NativeMethods.JetCloseFileInstance(instance.Value, handle.Value));
        }

        /// <summary>
        /// Ends an external backup session. This API is the last API in a series
        /// of APIs that must be called to execute a successful online
        /// (non-VSS based) backup.
        /// </summary>
        /// <param name="instance">The instance to end the backup for.</param>
        /// <returns>An error code if the call fails.</returns>
        public int JetEndExternalBackupInstance(JET_INSTANCE instance)
        {
            this.TraceFunctionCall("JetEndExternalBackupInstance");
            return this.Err(NativeMethods.JetEndExternalBackupInstance(instance.Value));  
        }

        /// <summary>
        /// Ends an external backup session. This API is the last API in a series
        /// of APIs that must be called to execute a successful online
        /// (non-VSS based) backup.
        /// </summary>
        /// <param name="instance">The instance to end the backup for.</param>
        /// <param name="grbit">Options that specify how the backup ended.</param>
        /// <returns>An error code if the call fails.</returns>
        public int JetEndExternalBackupInstance2(JET_INSTANCE instance, EndExternalBackupGrbit grbit)
        {
            this.TraceFunctionCall("JetEndExternalBackupInstance2");
            return this.Err(NativeMethods.JetEndExternalBackupInstance2(instance.Value, (uint)grbit));
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
        /// <returns>An error code if the call fails.</returns>
        public int JetOpenFileInstance(JET_INSTANCE instance, string file, out JET_HANDLE handle, out long fileSizeLow, out long fileSizeHigh)
        {
            this.TraceFunctionCall("JetOpenFileInstance");
            this.CheckNotNull(file, "file");
            handle = JET_HANDLE.Nil;
            int err;
            uint nativeFileSizeLow;
            uint nativeFileSizeHigh;
            if (this.Capabilities.SupportsUnicodePaths)
            {
                err = this.Err(NativeMethods.JetOpenFileInstanceW(
                            instance.Value, file, out handle.Value, out nativeFileSizeLow, out nativeFileSizeHigh));
            }
            else
            {
                err = this.Err(NativeMethods.JetOpenFileInstance(
                            instance.Value, file, out handle.Value, out nativeFileSizeLow, out nativeFileSizeHigh));                
            }

            fileSizeLow = nativeFileSizeLow;
            fileSizeHigh = nativeFileSizeHigh;
            return err;                 
        }

        /// <summary>
        /// Retrieves the contents of a file opened with <see cref="Api.JetOpenFileInstance"/>.
        /// </summary>
        /// <param name="instance">The instance to use.</param>
        /// <param name="file">The file to read from.</param>
        /// <param name="buffer">The buffer to read into.</param>
        /// <param name="bufferSize">The size of the buffer.</param>
        /// <param name="bytesRead">Returns the amount of data read into the buffer.</param>
        /// <returns>An error code if the call fails.</returns>
        public int JetReadFileInstance(JET_INSTANCE instance, JET_HANDLE file, byte[] buffer, int bufferSize, out int bytesRead)
        {
            this.TraceFunctionCall("JetReadFileInstance");
            this.CheckNotNull(buffer, "buffer");
            this.CheckDataSize(buffer, bufferSize, "bufferSize");

            // ESENT requires that the buffer be aligned on a page allocation boundary.
            // VirtualAlloc is the API used to do that, so we use P/Invoke to call it.
            IntPtr alignedBuffer = Win32.NativeMethods.VirtualAlloc(
                IntPtr.Zero,
                (UIntPtr)bufferSize,
                (uint)(Win32.AllocationType.MEM_COMMIT | Win32.AllocationType.MEM_RESERVE),
                (uint)Win32.MemoryProtection.PAGE_READWRITE);
            if (IntPtr.Zero == alignedBuffer)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error(), "VirtualAlloc");
            }

            try
            {
                uint nativeBytesRead;
                int err =
                    this.Err(
                        NativeMethods.JetReadFileInstance(
                            instance.Value, file.Value, alignedBuffer, checked((uint)bufferSize), out nativeBytesRead));
                bytesRead = checked((int)nativeBytesRead);

                // Copy the memory out of the aligned buffer into the user buffer.
                Marshal.Copy(alignedBuffer, buffer, 0, bytesRead);
                return err;
            }
            finally
            {
                if (!Win32.NativeMethods.VirtualFree(alignedBuffer, UIntPtr.Zero, (uint)Win32.FreeType.MEM_RELEASE))
                {
                    throw new Win32Exception(Marshal.GetLastWin32Error(), "VirtualFree");
                }
            }
        }

        /// <summary>
        /// Used during a backup initiated by JetBeginExternalBackup to delete
        /// any transaction log files that will no longer be needed once the
        /// current backup completes successfully.
        /// </summary>
        /// <param name="instance">The instance to truncate.</param>
        /// <returns>An error code if the call fails.</returns>
        public int JetTruncateLogInstance(JET_INSTANCE instance)
        {
            this.TraceFunctionCall("JetTruncateLogInstance");
            return this.Err(NativeMethods.JetTruncateLogInstance(instance.Value));
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
        /// <returns>An error if the call fails.</returns>
        public int JetBeginSession(JET_INSTANCE instance, out JET_SESID sesid, string username, string password)
        {
            this.TraceFunctionCall("JetBeginSession");
            sesid = JET_SESID.Nil;
            return this.Err(NativeMethods.JetBeginSession(instance.Value, out sesid.Value, null, null));
        }

        /// <summary>
        /// Associates a session with the current thread using the given context
        /// handle. This association overrides the default engine requirement
        /// that a transaction for a given session must occur entirely on the
        /// same thread. 
        /// </summary>
        /// <param name="sesid">The session to set the context on.</param>
        /// <param name="context">The context to set.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetSetSessionContext(JET_SESID sesid, IntPtr context)
        {
            this.TraceFunctionCall("JetSetSessionContext");
            return this.Err(NativeMethods.JetSetSessionContext(sesid.Value, context));
        }

        /// <summary>
        /// Disassociates a session from the current thread. This should be
        /// used in conjunction with JetSetSessionContext.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetResetSessionContext(JET_SESID sesid)
        {
            this.TraceFunctionCall("JetResetSessionContext");
            return this.Err(NativeMethods.JetResetSessionContext(sesid.Value));
        }

        /// <summary>
        /// Ends a session.
        /// </summary>
        /// <param name="sesid">The session to end.</param>
        /// <param name="grbit">This parameter is not used.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetEndSession(JET_SESID sesid, EndSessionGrbit grbit)
        {
            this.TraceFunctionCall("JetEndSession");
            return this.Err(NativeMethods.JetEndSession(sesid.Value, (uint)grbit));
        }

        /// <summary>
        /// Initialize a new ESE session in the same instance as the given sesid.
        /// </summary>
        /// <param name="sesid">The session to duplicate.</param>
        /// <param name="newSesid">Returns the new session.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetDupSession(JET_SESID sesid, out JET_SESID newSesid)
        {
            this.TraceFunctionCall("JetDupSession");
            newSesid = JET_SESID.Nil;
            return this.Err(NativeMethods.JetDupSession(sesid.Value, out newSesid.Value));
        }

        /// <summary>
        /// Retrieves performance information from the database engine for the
        /// current thread. Multiple calls can be used to collect statistics
        /// that reflect the activity of the database engine on this thread
        /// between those calls. 
        /// </summary>
        /// <param name="threadstats">
        /// Returns the thread statistics..
        /// </param>
        /// <returns>An error code if the operation fails.</returns>
        public int JetGetThreadStats(out JET_THREADSTATS threadstats)
        {
            this.CheckSupportsVistaFeatures("JetGetThreadStats");

            NATIVE_THREADSTATS native;
            int err = this.Err(NativeMethods.JetGetThreadStats(out native, checked((uint)NATIVE_THREADSTATS.Size)));

            threadstats = new JET_THREADSTATS();
            threadstats.SetFromNativeThreadstats(native);
            return err;
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
        /// <param name="parametersLength">The parameter is not used.</param>
        /// <param name="grbit">Table open options.</param>
        /// <param name="tableid">Returns the opened table.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetOpenTable(JET_SESID sesid, JET_DBID dbid, string tablename, byte[] parameters, int parametersLength, OpenTableGrbit grbit, out JET_TABLEID tableid)
        {
            this.TraceFunctionCall("JetOpenTable");
            tableid = JET_TABLEID.Nil;
            this.CheckNotNull(tablename, "tablename");

            return this.Err(NativeMethods.JetOpenTable(sesid.Value, dbid.Value, tablename, IntPtr.Zero, 0, (uint)grbit, out tableid.Value));
        }

        /// <summary>
        /// Close an open table.
        /// </summary>
        /// <param name="sesid">The session which opened the table.</param>
        /// <param name="tableid">The table to close.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetCloseTable(JET_SESID sesid, JET_TABLEID tableid)
        {
            this.TraceFunctionCall("JetCloseTable");
            return this.Err(NativeMethods.JetCloseTable(sesid.Value, tableid.Value));
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
        /// <returns>An error if the call fails.</returns>
        public int JetDupCursor(JET_SESID sesid, JET_TABLEID tableid, out JET_TABLEID newTableid, DupCursorGrbit grbit)
        {
            this.TraceFunctionCall("JetDupCursor");
            newTableid = JET_TABLEID.Nil;
            return this.Err(NativeMethods.JetDupCursor(sesid.Value, tableid.Value, out newTableid.Value, (uint)grbit));
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
        /// <returns>An error if the call fails.</returns>
        public int JetComputeStats(JET_SESID sesid, JET_TABLEID tableid)
        {
            this.TraceFunctionCall("JetComputeStats");
            return this.Err(NativeMethods.JetComputeStats(sesid.Value, tableid.Value));           
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
        /// <returns>An error if the call fails.</returns>
        public int JetSetLS(JET_SESID sesid, JET_TABLEID tableid, JET_LS ls, LsGrbit grbit)
        {
            this.TraceFunctionCall("JetSetLS");
            return this.Err(NativeMethods.JetSetLS(sesid.Value, tableid.Value, ls.Value, (uint)grbit));
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
        /// <returns>An error if the call fails.</returns>
        public int JetGetLS(JET_SESID sesid, JET_TABLEID tableid, out JET_LS ls, LsGrbit grbit)
        {
            this.TraceFunctionCall("JetGetLS");
            IntPtr native;
            int err = NativeMethods.JetGetLS(sesid.Value, tableid.Value, out native, (uint)grbit);
            ls = new JET_LS { Value = native };
            return this.Err(err);
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
        /// <returns>An error if the call fails.</returns>
        public int JetGetCursorInfo(JET_SESID sesid, JET_TABLEID tableid)
        {
            this.TraceFunctionCall("JetGetCursorInfo");
            return this.Err(NativeMethods.JetGetCursorInfo(sesid.Value, tableid.Value, IntPtr.Zero, 0, 0));
        }

        #endregion

        #region Transactions

        /// <summary>
        /// Causes a session to enter a transaction or create a new save point in an existing
        /// transaction.
        /// </summary>
        /// <param name="sesid">The session to begin the transaction for.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetBeginTransaction(JET_SESID sesid)
        {
            this.TraceFunctionCall("JetBeginTransaction");
            return this.Err(NativeMethods.JetBeginTransaction(sesid.Value));
        }

        /// <summary>
        /// Causes a session to enter a transaction or create a new save point in an existing
        /// transaction.
        /// </summary>
        /// <param name="sesid">The session to begin the transaction for.</param>
        /// <param name="grbit">Transaction options.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetBeginTransaction2(JET_SESID sesid, BeginTransactionGrbit grbit)
        {
            this.TraceFunctionCall("JetBeginTransaction2");
            return this.Err(NativeMethods.JetBeginTransaction2(sesid.Value, unchecked((uint)grbit)));
        }

        /// <summary>
        /// Commits the changes made to the state of the database during the current save point
        /// and migrates them to the previous save point. If the outermost save point is committed
        /// then the changes made during that save point will be committed to the state of the
        /// database and the session will exit the transaction.
        /// </summary>
        /// <param name="sesid">The session to commit the transaction for.</param>
        /// <param name="grbit">Commit options.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetCommitTransaction(JET_SESID sesid, CommitTransactionGrbit grbit)
        {
            this.TraceFunctionCall("JetCommitTransaction");
            return this.Err(NativeMethods.JetCommitTransaction(sesid.Value, unchecked((uint)grbit)));
        }

        /// <summary>
        /// Undoes the changes made to the state of the database
        /// and returns to the last save point. JetRollback will also close any cursors
        /// opened during the save point. If the outermost save point is undone, the
        /// session will exit the transaction.
        /// </summary>
        /// <param name="sesid">The session to rollback the transaction for.</param>
        /// <param name="grbit">Rollback options.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetRollback(JET_SESID sesid, RollbackTransactionGrbit grbit)
        {
            this.TraceFunctionCall("JetRollback");
            return this.Err(NativeMethods.JetRollback(sesid.Value, unchecked((uint)grbit)));
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
        /// <returns>An error if the call fails.</returns>
        public int JetCreateTable(JET_SESID sesid, JET_DBID dbid, string table, int pages, int density, out JET_TABLEID tableid)
        {
            this.TraceFunctionCall("JetCreateTable");
            tableid = JET_TABLEID.Nil;
            this.CheckNotNull(table, "table");

            return this.Err(NativeMethods.JetCreateTable(sesid.Value, dbid.Value, table, pages, density, out tableid.Value));
        }

        /// <summary>
        /// Deletes a table from a database.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to delete the table from.</param>
        /// <param name="table">The name of the table to delete.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetDeleteTable(JET_SESID sesid, JET_DBID dbid, string table)
        {
            this.TraceFunctionCall("JetDeleteTable");
            this.CheckNotNull(table, "table");

            return this.Err(NativeMethods.JetDeleteTable(sesid.Value, dbid.Value, table));
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
        /// <returns>An error if the call fails.</returns>
        public int JetAddColumn(JET_SESID sesid, JET_TABLEID tableid, string column, JET_COLUMNDEF columndef, byte[] defaultValue, int defaultValueSize, out JET_COLUMNID columnid)
        {
            this.TraceFunctionCall("JetAddColumn");
            columnid = JET_COLUMNID.Nil;
            this.CheckNotNull(column, "column");
            this.CheckNotNull(columndef, "columndef");
            this.CheckDataSize(defaultValue, defaultValueSize, "defaultValueSize");

            NATIVE_COLUMNDEF nativeColumndef = columndef.GetNativeColumndef();
            int err = this.Err(NativeMethods.JetAddColumn(
                                   sesid.Value, 
                                   tableid.Value, 
                                   column, 
                                   ref nativeColumndef,
                                   defaultValue, 
                                   checked((uint)defaultValueSize),
                                   out columnid.Value));

            // esent doesn't actually set the columnid member of the passed in JET_COLUMNDEF, but we will do that here for
            // completeness.
            columndef.columnid = new JET_COLUMNID { Value = columnid.Value };
            return err;
        }

        /// <summary>
        /// Deletes a column from a database table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">A cursor on the table to delete the column from.</param>
        /// <param name="column">The name of the column to be deleted.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetDeleteColumn(JET_SESID sesid, JET_TABLEID tableid, string column)
        {
            this.TraceFunctionCall("JetDeleteColumn");
            this.CheckNotNull(column, "column");
            return this.Err(NativeMethods.JetDeleteColumn(sesid.Value, tableid.Value, column));
        }

        /// <summary>
        /// Deletes a column from a database table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">A cursor on the table to delete the column from.</param>
        /// <param name="column">The name of the column to be deleted.</param>
        /// <param name="grbit">Column deletion options.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetDeleteColumn2(JET_SESID sesid, JET_TABLEID tableid, string column, DeleteColumnGrbit grbit)
        {
            this.TraceFunctionCall("JetDeleteColumn2");
            this.CheckNotNull(column, "column");
            return this.Err(NativeMethods.JetDeleteColumn2(sesid.Value, tableid.Value, column, (uint)grbit));
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
        /// <returns>An error if the call fails.</returns>
        public int JetCreateIndex(
            JET_SESID sesid,
            JET_TABLEID tableid,
            string indexName,
            CreateIndexGrbit grbit, 
            string keyDescription,
            int keyDescriptionLength,
            int density)
        {
            this.TraceFunctionCall("JetCreateIndex");
            this.CheckNotNull(indexName, "indexName");
            this.CheckNotNegative(keyDescriptionLength, "keyDescriptionLength");
            this.CheckNotNegative(density, "density");
            if (keyDescriptionLength > checked(keyDescription.Length + 1))
            {
                throw new ArgumentOutOfRangeException(
                    "keyDescriptionLength", keyDescriptionLength, "cannot be greater than keyDescription.Length");
            }

            return this.Err(NativeMethods.JetCreateIndex(
                sesid.Value,
                tableid.Value,
                indexName,
                (uint)grbit,
                keyDescription,
                checked((uint)keyDescriptionLength),
                checked((uint)density)));
        }

        /// <summary>
        /// Creates indexes over data in an ESE database.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to create the index on.</param>
        /// <param name="indexcreates">Array of objects describing the indexes to be created.</param>
        /// <param name="numIndexCreates">Number of index description objects.</param>
        /// <returns>An error code.</returns>
        public int JetCreateIndex2(
            JET_SESID sesid,
            JET_TABLEID tableid,
            JET_INDEXCREATE[] indexcreates,
            int numIndexCreates)
        {
            this.TraceFunctionCall("JetCreateIndex2");
            this.CheckNotNull(indexcreates, "indexcreates");
            this.CheckNotNegative(numIndexCreates, "numIndexCreates");
            if (numIndexCreates > indexcreates.Length)
            {
                throw new ArgumentOutOfRangeException(
                    "numIndexCreates", numIndexCreates, "numIndexCreates is larger than the number of indexes passed in");
            }

            if (this.Capabilities.SupportsVistaFeatures)
            {
                return this.CreateIndexes2(sesid, tableid, indexcreates, numIndexCreates);                
            }

            return this.CreateIndexes(sesid, tableid, indexcreates, numIndexCreates);
        }

        /// <summary>
        /// Deletes an index from a database table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">A cursor on the table to delete the index from.</param>
        /// <param name="index">The name of the index to be deleted.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetDeleteIndex(JET_SESID sesid, JET_TABLEID tableid, string index)
        {
            this.TraceFunctionCall("JetDeleteIndex");
            this.CheckNotNull(index, "index");

            return this.Err(NativeMethods.JetDeleteIndex(sesid.Value, tableid.Value, index));
        }

        /// <summary>
        /// Creates a temporary table with a single index. A temporary table
        /// stores and retrieves records just like an ordinary table created
        /// using JetCreateTableColumnIndex. However, temporary tables are
        /// much faster than ordinary tables due to their volatile nature.
        /// They can also be used to very quickly sort and perform duplicate
        /// removal on record sets when accessed in a purely sequential manner.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="columns">
        /// Column definitions for the columns created in the temporary table.
        /// </param>
        /// <param name="numColumns">Number of column definitions.</param>
        /// <param name="grbit">Table creation options.</param>
        /// <param name="tableid">
        /// Returns the tableid of the temporary table. Closing this tableid
        /// frees the resources associated with the temporary table.
        /// </param>
        /// <param name="columnids">
        /// The output buffer that receives the array of column IDs generated
        /// during the creation of the temporary table. The column IDs in this
        /// array will exactly correspond to the input array of column definitions.
        /// As a result, the size of this buffer must correspond to the size of the input array.
        /// </param>
        /// <returns>An error code.</returns>
        public int JetOpenTempTable(
            JET_SESID sesid,
            JET_COLUMNDEF[] columns,
            int numColumns,
            TempTableGrbit grbit,
            out JET_TABLEID tableid,
            JET_COLUMNID[] columnids)
        {
            this.TraceFunctionCall("JetOpenTempTable");
            this.CheckNotNull(columns, "columnns");
            this.CheckDataSize(columns, numColumns, "numColumns");
            this.CheckNotNull(columnids, "columnids");
            this.CheckDataSize(columnids, numColumns, "numColumns");

            tableid = JET_TABLEID.Nil;

            NATIVE_COLUMNDEF[] nativecolumndefs = GetNativecolumndefs(columns, numColumns);
            var nativecolumnids = new uint[numColumns];

            int err = this.Err(NativeMethods.JetOpenTempTable(
                sesid.Value, nativecolumndefs, checked((uint)numColumns), (uint)grbit, out tableid.Value, nativecolumnids));

            SetColumnids(columns, columnids, nativecolumnids, numColumns);
            
            return err;
        }

        /// <summary>
        /// Creates a temporary table with a single index. A temporary table
        /// stores and retrieves records just like an ordinary table created
        /// using JetCreateTableColumnIndex. However, temporary tables are
        /// much faster than ordinary tables due to their volatile nature.
        /// They can also be used to very quickly sort and perform duplicate
        /// removal on record sets when accessed in a purely sequential manner.
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
        /// frees the resources associated with the temporary table.
        /// </param>
        /// <param name="columnids">
        /// The output buffer that receives the array of column IDs generated
        /// during the creation of the temporary table. The column IDs in this
        /// array will exactly correspond to the input array of column definitions.
        /// As a result, the size of this buffer must correspond to the size of the input array.
        /// </param>
        /// <returns>An error code.</returns>
        public int JetOpenTempTable2(
            JET_SESID sesid,
            JET_COLUMNDEF[] columns,
            int numColumns,
            int lcid,
            TempTableGrbit grbit,
            out JET_TABLEID tableid,
            JET_COLUMNID[] columnids)
        {
            this.TraceFunctionCall("JetOpenTempTable2");
            this.CheckNotNull(columns, "columnns");
            this.CheckDataSize(columns, numColumns, "numColumns");
            this.CheckNotNull(columnids, "columnids");
            this.CheckDataSize(columnids, numColumns, "numColumns");

            tableid = JET_TABLEID.Nil;

            NATIVE_COLUMNDEF[] nativecolumndefs = GetNativecolumndefs(columns, numColumns);
            var nativecolumnids = new uint[numColumns];

            int err = this.Err(NativeMethods.JetOpenTempTable2(
                sesid.Value, nativecolumndefs, checked((uint)numColumns), (uint)lcid, (uint)grbit, out tableid.Value, nativecolumnids));

            SetColumnids(columns, columnids, nativecolumnids, numColumns);

            return err;            
        }

        /// <summary>
        /// Creates a temporary table with a single index. A temporary table
        /// stores and retrieves records just like an ordinary table created
        /// using JetCreateTableColumnIndex. However, temporary tables are
        /// much faster than ordinary tables due to their volatile nature.
        /// They can also be used to very quickly sort and perform duplicate
        /// removal on record sets when accessed in a purely sequential manner.
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
        /// frees the resources associated with the temporary table.
        /// </param>
        /// <param name="columnids">
        /// The output buffer that receives the array of column IDs generated
        /// during the creation of the temporary table. The column IDs in this
        /// array will exactly correspond to the input array of column definitions.
        /// As a result, the size of this buffer must correspond to the size of the input array.
        /// </param>
        /// <returns>An error code.</returns>
        public int JetOpenTempTable3(
            JET_SESID sesid,
            JET_COLUMNDEF[] columns,
            int numColumns,
            JET_UNICODEINDEX unicodeindex,
            TempTableGrbit grbit,
            out JET_TABLEID tableid,
            JET_COLUMNID[] columnids)
        {
            this.TraceFunctionCall("JetOpenTempTable3");
            this.CheckNotNull(columns, "columnns");
            this.CheckDataSize(columns, numColumns, "numColumns");
            this.CheckNotNull(columnids, "columnids");
            this.CheckDataSize(columnids, numColumns, "numColumns");

            tableid = JET_TABLEID.Nil;

            NATIVE_COLUMNDEF[] nativecolumndefs = GetNativecolumndefs(columns, numColumns);
            var nativecolumnids = new uint[numColumns];

            int err;
            if (null != unicodeindex)
            {
                NATIVE_UNICODEINDEX nativeunicodeindex = unicodeindex.GetNativeUnicodeIndex();
                err = this.Err(NativeMethods.JetOpenTempTable3(
                    sesid.Value, nativecolumndefs, checked((uint)numColumns), ref nativeunicodeindex, (uint)grbit, out tableid.Value, nativecolumnids));
            }
            else
            {
                err = this.Err(NativeMethods.JetOpenTempTable3(
                    sesid.Value, nativecolumndefs, checked((uint)numColumns), IntPtr.Zero, (uint)grbit, out tableid.Value, nativecolumnids));                
            }

            SetColumnids(columns, columnids, nativecolumnids, numColumns);

            return err;            
        }

        /// <summary>
        /// Creates a temporary table with a single index. A temporary table
        /// stores and retrieves records just like an ordinary table created
        /// using JetCreateTableColumnIndex. However, temporary tables are
        /// much faster than ordinary tables due to their volatile nature.
        /// They can also be used to very quickly sort and perform duplicate
        /// removal on record sets when accessed in a purely sequential manner.
        /// </summary>
        /// <remarks>
        /// Introduced in Windows Vista;
        /// </remarks>
        /// <param name="sesid">The session to use.</param>
        /// <param name="temporarytable">
        /// Description of the temporary table to create on input. After a
        /// successful call, the structure contains the handle to the temporary
        /// table and column identifications.
        /// </param>
        /// <returns>An error code.</returns>
        public int JetOpenTemporaryTable(JET_SESID sesid, JET_OPENTEMPORARYTABLE temporarytable)
        {
            this.TraceFunctionCall("JetOpenTemporaryTable");
            this.CheckSupportsVistaFeatures("JetOpenTemporaryTables");
            this.CheckNotNull(temporarytable, "temporarytable");

            NATIVE_OPENTEMPORARYTABLE nativetemporarytable = temporarytable.GetNativeOpenTemporaryTable();
            var nativecolumnids = new uint[nativetemporarytable.ccolumn];
            NATIVE_COLUMNDEF[] nativecolumndefs = GetNativecolumndefs(temporarytable.prgcolumndef, temporarytable.ccolumn);
            unsafe
            {
                using (var gchandlecollection = new GCHandleCollection())
                {
                    // Pin memory
                    nativetemporarytable.prgcolumndef = (NATIVE_COLUMNDEF*) gchandlecollection.Add(nativecolumndefs);
                    nativetemporarytable.rgcolumnid = (uint*) gchandlecollection.Add(nativecolumnids);
                    if (null != temporarytable.pidxunicode)
                    {
                        nativetemporarytable.pidxunicode = (NATIVE_UNICODEINDEX*)
                            gchandlecollection.Add(temporarytable.pidxunicode.GetNativeUnicodeIndex());
                    }

                    // Call the interop method
                    int err = this.Err(NativeMethods.JetOpenTemporaryTable(sesid.Value, ref nativetemporarytable));

                    // Convert the return values
                    SetColumnids(temporarytable.prgcolumndef, temporarytable.prgcolumnid, nativecolumnids, temporarytable.ccolumn);
                    temporarytable.tableid = new JET_TABLEID { Value = nativetemporarytable.tableid };

                    return err;
                }
            }
        }

        /// <summary>
        /// Retrieves information about a table column.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table containing the column.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="columndef">Filled in with information about the column.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetGetTableColumnInfo(
                JET_SESID sesid,
                JET_TABLEID tableid,
                string columnName,
                out JET_COLUMNDEF columndef)
        {
            this.TraceFunctionCall("JetGetTableColumnInfo");
            columndef = new JET_COLUMNDEF();
            this.CheckNotNull(columnName, "columnName");

            var nativeColumndef = new NATIVE_COLUMNDEF();
            nativeColumndef.cbStruct = checked((uint)Marshal.SizeOf(nativeColumndef));
            int err = this.Err(NativeMethods.JetGetTableColumnInfo(
                sesid.Value,
                tableid.Value,
                columnName,
                ref nativeColumndef,
                nativeColumndef.cbStruct,
                (uint)JET_ColInfo.Default));
            columndef.SetFromNativeColumndef(nativeColumndef);

            return err;
        }

        /// <summary>
        /// Retrieves information about a table column.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table containing the column.</param>
        /// <param name="columnid">The columnid of the column.</param>
        /// <param name="columndef">Filled in with information about the column.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetGetTableColumnInfo(
                JET_SESID sesid,
                JET_TABLEID tableid,
                JET_COLUMNID columnid,
                out JET_COLUMNDEF columndef)
        {
            this.TraceFunctionCall("JetGetTableColumnInfo");
            columndef = new JET_COLUMNDEF();

            var nativeColumndef = new NATIVE_COLUMNDEF();
            nativeColumndef.cbStruct = checked((uint)Marshal.SizeOf(nativeColumndef));
            int err = this.Err(NativeMethods.JetGetTableColumnInfo(
                sesid.Value,
                tableid.Value,
                ref columnid.Value,
                ref nativeColumndef,
                nativeColumndef.cbStruct,
                (uint)JET_ColInfo.ByColid));
            columndef.SetFromNativeColumndef(nativeColumndef);

            return err;
        }

        /// <summary>
        /// Retrieves information about all columns in the table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table containing the column.</param>
        /// <param name="ignored">The parameter is ignored.</param>
        /// <param name="columnlist">Filled in with information about the columns in the table.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetGetTableColumnInfo(
                JET_SESID sesid,
                JET_TABLEID tableid,
                string ignored,
                out JET_COLUMNLIST columnlist)
        {
            this.TraceFunctionCall("JetGetTableColumnInfo");
            columnlist = new JET_COLUMNLIST();

            var nativeColumnlist = new NATIVE_COLUMNLIST();
            nativeColumnlist.cbStruct = checked((uint)Marshal.SizeOf(nativeColumnlist));
            int err = this.Err(NativeMethods.JetGetTableColumnInfo(
                sesid.Value,
                tableid.Value,
                ignored,
                ref nativeColumnlist,
                nativeColumnlist.cbStruct,
                (uint)JET_ColInfo.List));
            columnlist.SetFromNativeColumnlist(nativeColumnlist);

            return err;
        }

        /// <summary>
        /// Retrieves information about a table column.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database that contains the table.</param>
        /// <param name="tablename">The name of the table containing the column.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="columndef">Filled in with information about the column.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetGetColumnInfo(
                JET_SESID sesid,
                JET_DBID dbid,
                string tablename,
                string columnName,
                out JET_COLUMNDEF columndef)
        {
            this.TraceFunctionCall("JetGetColumnInfo");
            columndef = new JET_COLUMNDEF();
            this.CheckNotNull(tablename, "tablename");
            this.CheckNotNull(columnName, "columnName");

            var nativeColumndef = new NATIVE_COLUMNDEF();
            nativeColumndef.cbStruct = checked((uint)Marshal.SizeOf(nativeColumndef));
            int err = this.Err(NativeMethods.JetGetColumnInfo(
               sesid.Value,
               dbid.Value,
               tablename,
               columnName,
               ref nativeColumndef,
               nativeColumndef.cbStruct,
               (uint)JET_ColInfo.Default));
            columndef.SetFromNativeColumndef(nativeColumndef);

            return err;
        }

        /// <summary>
        /// Retrieves information about all columns in a table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database that contains the table.</param>
        /// <param name="tablename">The name of the table containing the column.</param>
        /// <param name="ignored">This parameter is ignored.</param>
        /// <param name="columnlist">Filled in with information about the columns in the table.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetGetColumnInfo(
                JET_SESID sesid,
                JET_DBID dbid,
                string tablename,
                string ignored,
                out JET_COLUMNLIST columnlist)
        {
            this.TraceFunctionCall("JetGetColumnInfo");      
            columnlist = new JET_COLUMNLIST();
            this.CheckNotNull(tablename, "tablename");

            var nativeColumnlist = new NATIVE_COLUMNLIST();
            nativeColumnlist.cbStruct = checked((uint)Marshal.SizeOf(nativeColumnlist));
            int err = this.Err(NativeMethods.JetGetColumnInfo(
                sesid.Value,
                dbid.Value,
                tablename,
                ignored,
                ref nativeColumnlist,
                nativeColumnlist.cbStruct,
                (uint)JET_ColInfo.List));
            columnlist.SetFromNativeColumnlist(nativeColumnlist);

            return err;
        }

        /// <summary>
        /// Retrieves information about database objects.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to use.</param>
        /// <param name="objectlist">Filled in with information about the objects in the database.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetGetObjectInfo(JET_SESID sesid, JET_DBID dbid, out JET_OBJECTLIST objectlist)
        {
            this.TraceFunctionCall("JetGetObjectInfo");
            objectlist = new JET_OBJECTLIST();

            var nativeObjectlist = new NATIVE_OBJECTLIST();
            nativeObjectlist.cbStruct = checked((uint)Marshal.SizeOf(nativeObjectlist));
            int err = this.Err(NativeMethods.JetGetObjectInfo(
                sesid.Value,
                dbid.Value,
                (uint)JET_objtyp.Table,
                null,
                null,
                ref nativeObjectlist,
                nativeObjectlist.cbStruct,
                (uint)JET_ObjInfo.ListNoStats));
            objectlist.SetFromNativeObjectlist(nativeObjectlist);

            return err;
        }

        /// <summary>
        /// JetGetCurrentIndex function determines the name of the current
        /// index of a given cursor. This name is also used to later re-select
        /// that index as the current index using JetSetCurrentIndex. It can
        /// also be used to discover the properties of that index using
        /// JetGetTableIndexInfo.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to get the index name for.</param>
        /// <param name="indexName">Returns the name of the index.</param>
        /// <param name="maxNameLength">
        /// The maximum length of the index name. Index names are no more than 
        /// Api.MaxNameLength characters.
        /// </param>
        /// <returns>An error if the call fails.</returns>
        public int JetGetCurrentIndex(JET_SESID sesid, JET_TABLEID tableid, out string indexName, int maxNameLength)
        {
            this.TraceFunctionCall("JetGetCurrentIndex");
            this.CheckNotNegative(maxNameLength, "maxNameLength");

            var name = new StringBuilder(maxNameLength);
            int err = this.Err(NativeMethods.JetGetCurrentIndex(sesid.Value, tableid.Value, name, checked((uint)maxNameLength)));
            indexName = name.ToString();
            return err;
        }

        /// <summary>
        /// Retrieves information about indexes on a table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to use.</param>
        /// <param name="tablename">The name of the table to retrieve index information about.</param>
        /// <param name="ignored">This parameter is ignored.</param>
        /// <param name="indexlist">Filled in with information about indexes on the table.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetGetIndexInfo(
                JET_SESID sesid,
                JET_DBID dbid,
                string tablename,
                string ignored,
                out JET_INDEXLIST indexlist)
        {
            this.TraceFunctionCall("JetGetIndexInfo");
            indexlist = new JET_INDEXLIST();
            this.CheckNotNull(tablename, "tablename");

            var nativeIndexlist = new NATIVE_INDEXLIST();
            nativeIndexlist.cbStruct = checked((uint)Marshal.SizeOf(nativeIndexlist));
            int err = this.Err(NativeMethods.JetGetIndexInfo(
                sesid.Value,
                dbid.Value,
                tablename,
                ignored,
                ref nativeIndexlist,
                nativeIndexlist.cbStruct,
                (uint)JET_IdxInfo.InfoList));
            indexlist.SetFromNativeIndexlist(nativeIndexlist);

            return err;
        }

        /// <summary>
        /// Retrieves information about indexes on a table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to retrieve index information about.</param>
        /// <param name="ignored">This parameter is ignored.</param>
        /// <param name="indexlist">Filled in with information about indexes on the table.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetGetTableIndexInfo(
                JET_SESID sesid,
                JET_TABLEID tableid,
                string ignored,
                out JET_INDEXLIST indexlist)
        {
            this.TraceFunctionCall("JetGetTableIndexInfo");
            indexlist = new JET_INDEXLIST();

            var nativeIndexlist = new NATIVE_INDEXLIST();
            nativeIndexlist.cbStruct = checked((uint)Marshal.SizeOf(nativeIndexlist));
            int err = this.Err(NativeMethods.JetGetTableIndexInfo(
                sesid.Value,
                tableid.Value,
                ignored,
                ref nativeIndexlist,
                nativeIndexlist.cbStruct,
                (uint)JET_IdxInfo.InfoList));
            indexlist.SetFromNativeIndexlist(nativeIndexlist);

            return err;
        }

        /// <summary>
        /// Changes the name of an existing table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database containing the table.</param>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="newTableName">The new name of the table.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetRenameTable(JET_SESID sesid, JET_DBID dbid, string tableName, string newTableName)
        {
            this.TraceFunctionCall("JetRenameTable");
            this.CheckNotNull(tableName, "tableName");
            this.CheckNotNull(newTableName, "newTableName");
            return this.Err(NativeMethods.JetRenameTable(sesid.Value, dbid.Value, tableName, newTableName));
        }

        /// <summary>
        /// Changes the name of an existing column.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table containing the column.</param>
        /// <param name="name">The name of the column.</param>
        /// <param name="newName">The new name of the column.</param>
        /// <param name="grbit">Column rename options.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetRenameColumn(JET_SESID sesid, JET_TABLEID tableid, string name, string newName, RenameColumnGrbit grbit)
        {
            this.TraceFunctionCall("JetRenameColumn");    
            this.CheckNotNull(name, "name");
            this.CheckNotNull(newName, "newName");
            return this.Err(
                NativeMethods.JetRenameColumn(sesid.Value, tableid.Value, name, newName, (uint)grbit));
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
        /// <returns>An error if the call fails.</returns>
        public int JetSetColumnDefaultValue(
            JET_SESID sesid, JET_DBID dbid, string tableName, string columnName, byte[] data, int dataSize, SetColumnDefaultValueGrbit grbit)
        {
            this.TraceFunctionCall("JetSetColumnDefaultValue");  
            this.CheckNotNull(tableName, "tableName");
            this.CheckNotNull(columnName, "columnName");
            this.CheckDataSize(data, dataSize, "dataSize");
            return this.Err(
                NativeMethods.JetSetColumnDefaultValue(
                    sesid.Value, dbid.Value, tableName, columnName, data, checked((uint)dataSize), (uint)grbit));
        }

        #endregion

        #region Navigation

        /// <summary>
        /// Positions a cursor to an index entry for the record that is associated with
        /// the specified bookmark. The bookmark can be used with any index defined over
        /// a table. The bookmark for a record can be retrieved using <see cref="IJetApi.JetGetBookmark"/>. 
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to position.</param>
        /// <param name="bookmark">The bookmark used to position the cursor.</param>
        /// <param name="bookmarkSize">The size of the bookmark.</param>        /// <returns>An error if the call fails.</returns>
        public int JetGotoBookmark(JET_SESID sesid, JET_TABLEID tableid, byte[] bookmark, int bookmarkSize)
        {
            this.TraceFunctionCall("JetGotoBookmark");
            this.CheckNotNull(bookmark, "bookmark");
            this.CheckDataSize(bookmark, bookmarkSize, "bookmarkSize");

            return
                this.Err(
                    NativeMethods.JetGotoBookmark(
                        sesid.Value, tableid.Value, bookmark, checked((uint)bookmarkSize)));
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
        /// <returns>An error if the call fails.</returns>
        public int JetGotoSecondaryIndexBookmark(
            JET_SESID sesid,
            JET_TABLEID tableid,
            byte[] secondaryKey,
            int secondaryKeySize,
            byte[] primaryKey,
            int primaryKeySize,
            GotoSecondaryIndexBookmarkGrbit grbit)
        {
            this.TraceFunctionCall("JetGotoSecondaryIndexBookmark");
            this.CheckNotNull(secondaryKey, "secondaryKey");
            this.CheckDataSize(secondaryKey, secondaryKeySize, "secondaryKeySize");
            this.CheckDataSize(primaryKey, primaryKeySize, "primaryKeySize");

            return
                this.Err(
                    NativeMethods.JetGotoSecondaryIndexBookmark(
                        sesid.Value,
                        tableid.Value,
                        secondaryKey,
                        checked((uint)secondaryKeySize),
                        primaryKey,
                        checked((uint)primaryKeySize),
                        (uint)grbit));
        }

        /// <summary>
        /// Constructs search keys that may then be used by <see cref="IJetApi.JetSeek"/> and <see cref="IJetApi.JetSetIndexRange"/>.
        /// </summary>
        /// <remarks>
        /// The MakeKey functions provide datatype-specific make key functionality.
        /// </remarks>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="dataSize">Size of the data.</param>
        /// <param name="grbit">Key options.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetMakeKey(JET_SESID sesid, JET_TABLEID tableid, IntPtr data, int dataSize, MakeKeyGrbit grbit)
        {
            this.TraceFunctionCall("JetMakeKey");
            this.CheckNotNegative(dataSize, "dataSize");
            return this.Err(NativeMethods.JetMakeKey(sesid.Value, tableid.Value, data, checked((uint)dataSize), unchecked((uint)grbit)));
        }

        /// <summary>
        /// Efficiently positions a cursor to an index entry that matches the search
        /// criteria specified by the search key in that cursor and the specified
        /// inequality. A search key must have been previously constructed using 
        /// JetMakeKey.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to position.</param>
        /// <param name="grbit">Seek options.</param>
        /// <returns>An error or warning..</returns>
        public int JetSeek(JET_SESID sesid, JET_TABLEID tableid, SeekGrbit grbit)
        {
            this.TraceFunctionCall("JetSeek");
            return this.Err(NativeMethods.JetSeek(sesid.Value, tableid.Value, unchecked((uint)grbit)));
        }

        /// <summary>
        /// Navigate through an index. The cursor can be positioned at the start or
        /// end of the index and moved backwards and forwards by a specified number
        /// of index entries.
        /// </summary>
        /// <param name="sesid">The session to use for the call.</param>
        /// <param name="tableid">The cursor to position.</param>
        /// <param name="numRows">An offset which indicates how far to move the cursor.</param>
        /// <param name="grbit">Move options.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetMove(JET_SESID sesid, JET_TABLEID tableid, int numRows, MoveGrbit grbit)
        {
            this.TraceFunctionCall("JetMove");
            return this.Err(NativeMethods.JetMove(sesid.Value, tableid.Value, numRows, unchecked((uint)grbit)));
        }

        /// <summary>
        /// Temporarily limits the set of index entries that the cursor can walk using
        /// <see cref="IJetApi.JetMove"/> to those starting
        /// from the current index entry and ending at the index entry that matches the
        /// search criteria specified by the search key in that cursor and the specified
        /// bound criteria. A search key must have been previously constructed using
        /// JetMakeKey.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to set the index range on.</param>
        /// <param name="grbit">Index range options.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetSetIndexRange(JET_SESID sesid, JET_TABLEID tableid, SetIndexRangeGrbit grbit)
        {
            this.TraceFunctionCall("JetSetIndexRange");
            return this.Err(NativeMethods.JetSetIndexRange(sesid.Value, tableid.Value, unchecked((uint)grbit)));
        }

        /// <summary>
        /// Computes the intersection between multiple sets of index entries from different secondary
        /// indices over the same table. This operation is useful for finding the set of records in a
        /// table that match two or more criteria that can be expressed using index ranges. 
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="ranges">
        /// An the index ranges to intersect. The tableids in the ranges
        ///  must have index ranges set on them.
        /// </param>
        /// <param name="numRanges">
        /// The number of index ranges.
        /// </param>
        /// <param name="recordlist">
        /// Returns information about the temporary table containing the intersection results.
        /// </param>
        /// <param name="grbit">Intersection options.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetIntersectIndexes(
            JET_SESID sesid,
            JET_INDEXRANGE[] ranges,
            int numRanges,
            out JET_RECORDLIST recordlist,
            IntersectIndexesGrbit grbit)
        {
            this.TraceFunctionCall("JetIntersectIndexes");
            this.CheckNotNull(ranges, "ranges");
            this.CheckDataSize(ranges, numRanges, "numRanges");
            if (numRanges < 2)
            {
                throw new ArgumentOutOfRangeException(
                    "numRanges", numRanges, "JetIntersectIndexes requires at least two index ranges.");
            }

            var indexRanges = new NATIVE_INDEXRANGE[numRanges];
            for (int i = 0; i < numRanges; ++i)
            {
                indexRanges[i] = ranges[i].GetNativeIndexRange();
            }

            var nativeRecordlist = new NATIVE_RECORDLIST();
            nativeRecordlist.cbStruct = checked((uint)Marshal.SizeOf(nativeRecordlist));

            int err = this.Err(
                        NativeMethods.JetIntersectIndexes(
                            sesid.Value,
                            indexRanges,
                            checked((uint)indexRanges.Length),
                            ref nativeRecordlist,
                            (uint)grbit));
            recordlist = new JET_RECORDLIST();
            recordlist.SetFromNativeRecordlist(nativeRecordlist);
            return err;
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
        /// <returns>An error if the call fails.</returns>
        public int JetSetCurrentIndex(JET_SESID sesid, JET_TABLEID tableid, string index)
        {
            this.TraceFunctionCall("JetSetCurrentIndex");

            // A null index name is valid here -- it will set the table to the primary index
            return this.Err(NativeMethods.JetSetCurrentIndex(sesid.Value, tableid.Value, index));
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
        /// <returns>An error if the call fails.</returns>
        public int JetSetCurrentIndex2(JET_SESID sesid, JET_TABLEID tableid, string index, SetCurrentIndexGrbit grbit)
        {
            this.TraceFunctionCall("JetSetCurrentIndex2");

            // A null index name is valid here -- it will set the table to the primary index
            return this.Err(NativeMethods.JetSetCurrentIndex2(sesid.Value, tableid.Value, index, (uint)grbit));
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
        /// <returns>An error if the call fails.</returns>
        public int JetSetCurrentIndex3(JET_SESID sesid, JET_TABLEID tableid, string index, SetCurrentIndexGrbit grbit, int itagSequence)
        {
            this.TraceFunctionCall("JetSetCurrentIndex3");

            // A null index name is valid here -- it will set the table to the primary index
            return this.Err(NativeMethods.JetSetCurrentIndex3(sesid.Value, tableid.Value, index, (uint)grbit, checked((uint)itagSequence)));
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
        /// The maximum number of records to count.
        /// </param>
        /// <returns>An error if the call fails.</returns>
        public int JetIndexRecordCount(JET_SESID sesid, JET_TABLEID tableid, out int numRecords, int maxRecordsToCount)
        {
            this.TraceFunctionCall("JetIndexRecordCount");
            this.CheckNotNegative(maxRecordsToCount, "maxRecordsToCount");
            uint crec;
            int err = this.Err(NativeMethods.JetIndexRecordCount(sesid.Value, tableid.Value, out crec, unchecked((uint)maxRecordsToCount))); // -1 is allowed
            numRecords = checked((int)crec);
            return err;
        }

        /// <summary>
        /// Notifies the database engine that the application is scanning the entire
        /// index that the cursor is positioned on. Consequently, the methods that
        /// are used to access the index data will be tuned to make this scenario as
        /// fast as possible. 
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor that will be accessing the data.</param>
        /// <param name="grbit">Reserved for future use.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetSetTableSequential(JET_SESID sesid, JET_TABLEID tableid, SetTableSequentialGrbit grbit)
        {
            this.TraceFunctionCall("JetSetTableSequential");
            return this.Err(NativeMethods.JetSetTableSequential(sesid.Value, tableid.Value, (uint)grbit));
        }

        /// <summary>
        /// Notifies the database engine that the application is no longer scanning the
        /// entire index the cursor is positioned on. This call reverses a notification
        /// sent by JetSetTableSequential.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor that was accessing the data.</param>
        /// <param name="grbit">Reserved for future use.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetResetTableSequential(JET_SESID sesid, JET_TABLEID tableid, ResetTableSequentialGrbit grbit)
        {
            this.TraceFunctionCall("JetResetTableSequential");
            return this.Err(NativeMethods.JetResetTableSequential(sesid.Value, tableid.Value, (uint)grbit));
        }

        /// <summary>
        /// Returns the fractional position of the current record in the current index
        /// in the form of a JET_RECPOS structure.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor positioned on the record.</param>
        /// <param name="recpos">Returns the approximate fractional position of the record.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetGetRecordPosition(JET_SESID sesid, JET_TABLEID tableid, out JET_RECPOS recpos)
        {
            this.TraceFunctionCall("JetGetRecordPosition");
            recpos = new JET_RECPOS();
            NATIVE_RECPOS native = recpos.GetNativeRecpos();
            int err = this.Err(NativeMethods.JetGetRecordPosition(sesid.Value, tableid.Value, out native, native.cbStruct));
            recpos.SetFromNativeRecpos(native);
            return err;
        }

        /// <summary>
        /// Moves a cursor to a new location that is a fraction of the way through
        /// the current index. 
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to position.</param>
        /// <param name="recpos">The approximate position to move to.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetGotoPosition(JET_SESID sesid, JET_TABLEID tableid, JET_RECPOS recpos)
        {
            this.TraceFunctionCall("JetGotoRecordPosition");
            NATIVE_RECPOS native = recpos.GetNativeRecpos();
            return this.Err(NativeMethods.JetGotoPosition(sesid.Value, tableid.Value, ref native));
        }

        #endregion

        #region Data Retrieval

        /// <summary>
        /// Retrieves the bookmark for the record that is associated with the index entry
        /// at the current position of a cursor. This bookmark can then be used to
        /// reposition that cursor back to the same record using <see cref="IJetApi.JetGotoBookmark"/>. 
        /// The bookmark will be no longer than <see cref="SystemParameters.BookmarkMost"/>
        /// bytes.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the bookmark from.</param>
        /// <param name="bookmark">Buffer to contain the bookmark.</param>
        /// <param name="bookmarkSize">Size of the bookmark buffer.</param>
        /// <param name="actualBookmarkSize">Returns the actual size of the bookmark.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetGetBookmark(JET_SESID sesid, JET_TABLEID tableid, byte[] bookmark, int bookmarkSize, out int actualBookmarkSize)
        {
            this.TraceFunctionCall("JetGetBookmark");
            this.CheckDataSize(bookmark, bookmarkSize, "bookmarkSize");

            uint cbActual;
            int err = this.Err(
                NativeMethods.JetGetBookmark(
                    sesid.Value,
                    tableid.Value,
                    bookmark,
                    checked((uint)bookmarkSize),
                    out cbActual));

            actualBookmarkSize = checked((int)cbActual);
            return err;
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
        /// <returns>An error if the call fails.</returns>
        public int JetGetSecondaryIndexBookmark(
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
            this.TraceFunctionCall("JetGetSecondaryIndexBookmark");
            this.CheckDataSize(secondaryKey, secondaryKeySize, "secondaryKeySize");
            this.CheckDataSize(primaryKey, primaryKeySize, "primaryKeySize");

            uint cbSecondaryKey;
            uint cbPrimaryKey;
            int err = this.Err(
                NativeMethods.JetGetSecondaryIndexBookmark(
                    sesid.Value,
                    tableid.Value,
                    secondaryKey,
                    checked((uint)secondaryKeySize),
                    out cbSecondaryKey,
                    primaryKey,
                    checked((uint)primaryKeySize),
                    out cbPrimaryKey,
                    (uint)grbit));

            actualSecondaryKeySize = checked((int)cbSecondaryKey);
            actualPrimaryKeySize = checked((int)cbPrimaryKey);

            return err;
        }

        /// <summary>
        /// Retrieves the key for the index entry at the current position of a cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the key from.</param>
        /// <param name="data">The buffer to retrieve the key into.</param>
        /// <param name="dataSize">The size of the buffer.</param>
        /// <param name="actualDataSize">Returns the actual size of the data.</param>
        /// <param name="grbit">Retrieve key options.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetRetrieveKey(JET_SESID sesid, JET_TABLEID tableid, byte[] data, int dataSize, out int actualDataSize, RetrieveKeyGrbit grbit)
        {
            this.TraceFunctionCall("JetRetrieveKey");
            this.CheckDataSize(data, dataSize, "dataSize");

            uint cbActual;
            int err = this.Err(NativeMethods.JetRetrieveKey(sesid.Value, tableid.Value, data, checked((uint)dataSize), out cbActual, unchecked((uint)grbit)));

            actualDataSize = checked((int)cbActual);
            return err;
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
        /// <returns>An error or warning.</returns>
        public int JetRetrieveColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, IntPtr data, int dataSize, out int actualDataSize, RetrieveColumnGrbit grbit, JET_RETINFO retinfo)
        {
            this.TraceFunctionCall("JetRetrieveColumn");
            this.CheckNotNegative(dataSize, "dataSize");

            int err;
            uint cbActual;
            if (null != retinfo)
            {
                NATIVE_RETINFO nativeRetinfo = retinfo.GetNativeRetinfo();
                err = this.Err(NativeMethods.JetRetrieveColumn(
                        sesid.Value,
                        tableid.Value,
                        columnid.Value,
                        data,
                        checked((uint)dataSize),
                        out cbActual,
                        unchecked((uint)grbit),
                        ref nativeRetinfo));
                retinfo.SetFromNativeRetinfo(nativeRetinfo);
            }
            else
            {
                err = this.Err(NativeMethods.JetRetrieveColumn(
                        sesid.Value,
                        tableid.Value,
                        columnid.Value,
                        data,
                        checked((uint)dataSize),
                        out cbActual,
                        unchecked((uint)grbit),
                        IntPtr.Zero));
            }

            actualDataSize = checked((int)cbActual);
            return err;
        }

        /// <summary>
        /// The JetRetrieveColumns function retrieves multiple column values
        /// from the current record in a single operation. An array of
        /// <see cref="NATIVE_RETRIEVECOLUMN"/> structures is used to
        /// describe the set of column values to be retrieved, and to describe
        /// output buffers for each column value to be retrieved.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve columns from.</param>
        /// <param name="retrievecolumns">
        /// An array of one or more JET_RETRIEVECOLUMN structures. Each
        /// structure includes descriptions of which column value to retrieve
        /// and where to store returned data.
        /// </param>
        /// <param name="numColumns">
        /// Number of structures in the array given by retrievecolumns.
        /// </param>
        /// <returns>
        /// An error or warning.
        /// </returns>
        public unsafe int JetRetrieveColumns(JET_SESID sesid, JET_TABLEID tableid, NATIVE_RETRIEVECOLUMN* retrievecolumns, int numColumns)
        {
            this.TraceFunctionCall("JetRetrieveColumns");
            return this.Err(NativeMethods.JetRetrieveColumns(sesid.Value, tableid.Value, retrievecolumns, checked((uint)numColumns)));
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
        /// <returns>A warning, error or success.</returns>
        public int JetEnumerateColumns(
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
            this.TraceFunctionCall("JetEnumerateColumns");
            this.CheckNotNull(allocator, "allocator");
            this.CheckNotNegative(maxDataSize, "maxDataSize");
            this.CheckDataSize(columnids, numColumnids, "numColumnids");

            unsafe
            {
                // Converting to the native structs is a bit complex because we
                // do not want to allocate heap memory for this operations. We
                // allocate the NATIVE_ENUMCOLUMNID array on the stack and 
                // convert the managed objects. During the conversion pass we
                // calculate the total size of the tags. An array for the tags
                // is then allocated and a second pass converts the tags.
                //
                // Because we are using stackalloc all the work has to be done
                // in the same method.
                NATIVE_ENUMCOLUMNID* nativecolumnids = stackalloc NATIVE_ENUMCOLUMNID[numColumnids];
                int totalNumTags = ConvertEnumColumnids(columnids, numColumnids, nativecolumnids);

                uint* tags = stackalloc uint[totalNumTags];
                ConvertEnumColumnidTags(columnids, numColumnids, nativecolumnids, tags);

                uint cEnumColumn;
                NATIVE_ENUMCOLUMN* nativeenumcolumns;
                int err = NativeMethods.JetEnumerateColumns(
                    sesid.Value,
                    tableid.Value,
                    checked((uint)numColumnids),
                    numColumnids > 0 ? nativecolumnids : null,
                    out cEnumColumn,
                    out nativeenumcolumns,
                    allocator,
                    allocatorContext,
                    checked((uint)maxDataSize),
                    (uint)grbit);

                ConvertEnumerateColumnsResult(allocator, allocatorContext, cEnumColumn, nativeenumcolumns, out numColumnValues, out columnValues);

                return this.Err(err);
            }
        }

        /// <summary>
        /// Retrieves record size information from the desired location.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">
        /// The cursor that will be used for the API call. The cursor must be
        /// positioned on a record, or have an update prepared.
        /// </param>
        /// <param name="recsize">Returns the size of the record.</param>
        /// <param name="grbit">Call options.</param>
        /// <returns>A warning, error or success.</returns>
        public int JetGetRecordSize(JET_SESID sesid, JET_TABLEID tableid, ref JET_RECSIZE recsize, GetRecordSizeGrbit grbit)
        {
            this.TraceFunctionCall("JetGetRecordSize");
            int err;

            // Use JetGetRecordSize2 if available, otherwise JetGetRecordSize.
            if (this.Capabilities.SupportsWindows7Features)
            {
                var native = recsize.GetNativeRecsize2();
                err = NativeMethods.JetGetRecordSize2(sesid.Value, tableid.Value, ref native, (uint)grbit);
                recsize.SetFromNativeRecsize(native);
            }
            else
            {
                var native = recsize.GetNativeRecsize();
                err = NativeMethods.JetGetRecordSize(sesid.Value, tableid.Value, ref native, (uint)grbit);
                recsize.SetFromNativeRecsize(native);
            }

            return this.Err(err);
        }

        #endregion

        #region DML

        /// <summary>
        /// Deletes the current record in a database table.
        /// </summary>
        /// <param name="sesid">The session that opened the cursor.</param>
        /// <param name="tableid">The cursor on a database table. The current row will be deleted.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetDelete(JET_SESID sesid, JET_TABLEID tableid)
        {
            this.TraceFunctionCall("JetDelete");
            return this.Err(NativeMethods.JetDelete(sesid.Value, tableid.Value));
        }

        /// <summary>
        /// Prepare a cursor for update.
        /// </summary>
        /// <param name="sesid">The session which is starting the update.</param>
        /// <param name="tableid">The cursor to start the update for.</param>
        /// <param name="prep">The type of update to prepare.</param>
        /// <returns>An error if the call fails.</returns>
        public int JetPrepareUpdate(JET_SESID sesid, JET_TABLEID tableid, JET_prep prep)
        {
            this.TraceFunctionCall("JetPrepareUpdate");
            return this.Err(NativeMethods.JetPrepareUpdate(sesid.Value, tableid.Value, unchecked((uint)prep)));
        }

        /// <summary>
        /// The JetUpdate function performs an update operation including inserting a new row into
        /// a table or updating an existing row. Deleting a table row is performed by calling
        /// <see cref="IJetApi.JetDelete"/>.
        /// </summary>
        /// <param name="sesid">The session which started the update.</param>
        /// <param name="tableid">The cursor to update. An update should be prepared.</param>
        /// <param name="bookmark">Returns the bookmark of the updated record. This can be null.</param>
        /// <param name="bookmarkSize">The size of the bookmark buffer.</param>
        /// <param name="actualBookmarkSize">Returns the actual size of the bookmark.</param>
        /// <remarks>
        /// JetUpdate is the final step in performing an insert or an update. The update is begun by
        /// calling <see cref="IJetApi.JetPrepareUpdate"/> and then by calling
        /// JetSetColumn
        /// one or more times to set the record state. Finally, JetUpdate
        /// is called to complete the update operation. Indexes are updated only by JetUpdate or and not during JetSetColumn.
        /// </remarks>
        /// <returns>An error if the call fails.</returns>
        public int JetUpdate(JET_SESID sesid, JET_TABLEID tableid, byte[] bookmark, int bookmarkSize, out int actualBookmarkSize)
        {
            this.TraceFunctionCall("JetUpdate");
            this.CheckDataSize(bookmark, bookmarkSize, "bookmarkSize");

            // BUG: debug builds of ESENT can fill cbActual with this value if no bookmark is given
            const uint CbActualDebugFill = 0xDDDDDDDD;
            uint cbActual;
            int err = this.Err(NativeMethods.JetUpdate(sesid.Value, tableid.Value, bookmark, checked((uint)bookmarkSize), out cbActual));

            if (CbActualDebugFill == cbActual)
            {
                actualBookmarkSize = 0;
            }
            else
            {
                actualBookmarkSize = checked((int)cbActual);
            }

            return err;
        }

        /// <summary>
        /// The JetUpdate2 function performs an update operation including inserting a new row into
        /// a table or updating an existing row. Deleting a table row is performed by calling
        /// <see cref="JetDelete"/>.
        /// </summary>
        /// <param name="sesid">The session which started the update.</param>
        /// <param name="tableid">The cursor to update. An update should be prepared.</param>
        /// <param name="bookmark">Returns the bookmark of the updated record. This can be null.</param>
        /// <param name="bookmarkSize">The size of the bookmark buffer.</param>
        /// <param name="actualBookmarkSize">Returns the actual size of the bookmark.</param>
        /// <param name="grbit">Update options.</param>
        /// <remarks>
        /// JetUpdate is the final step in performing an insert or an update. The update is begun by
        /// calling <see cref="JetPrepareUpdate"/> and then by calling
        /// JetSetColumn one or more times to set the record state. Finally, JetUpdate
        /// is called to complete the update operation. Indexes are updated only by JetUpdate or and not during JetSetColumn.
        /// </remarks>
        /// <returns>An error if the call fails.</returns>
        public int JetUpdate2(JET_SESID sesid, JET_TABLEID tableid, byte[] bookmark, int bookmarkSize, out int actualBookmarkSize, UpdateGrbit grbit)
        {
            this.TraceFunctionCall("JetUpdate2");
            this.CheckDataSize(bookmark, bookmarkSize, "bookmarkSize");
            this.CheckSupportsServer2003Features("JetUpdate2");

            // BUG: debug builds of ESENT can fill cbActual with this value if no bookmark is given
            const uint CbActualDebugFill = 0xDDDDDDDD;
            uint cbActual;
            int err = this.Err(NativeMethods.JetUpdate2(sesid.Value, tableid.Value, bookmark, checked((uint)bookmarkSize), out cbActual, (uint)grbit));

            if (CbActualDebugFill == cbActual)
            {
                actualBookmarkSize = 0;
            }
            else
            {
                actualBookmarkSize = checked((int)cbActual);
            }

            return err;            
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
        /// <returns>An error if the call fails.</returns>
        public int JetSetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, IntPtr data, int dataSize, SetColumnGrbit grbit, JET_SETINFO setinfo)
        {
            this.TraceFunctionCall("JetSetColumn");
            this.CheckNotNegative(dataSize, "dataSize");
            if (IntPtr.Zero == data)
            {
                if (dataSize > 0 && (SetColumnGrbit.SizeLV != (grbit & SetColumnGrbit.SizeLV)))
                {
                    throw new ArgumentOutOfRangeException(
                        "dataSize",
                        dataSize,
                        "cannot be greater than the length of the data (unless the SizeLV option is used)");
                }
            }

            if (null != setinfo)
            {
                NATIVE_SETINFO nativeSetinfo = setinfo.GetNativeSetinfo();
                return this.Err(NativeMethods.JetSetColumn(sesid.Value, tableid.Value, columnid.Value, data, checked((uint)dataSize), unchecked((uint)grbit), ref nativeSetinfo));
            }

            return this.Err(NativeMethods.JetSetColumn(sesid.Value, tableid.Value, columnid.Value, data, checked((uint)dataSize), unchecked((uint)grbit), IntPtr.Zero));
        }

        /// <summary>
        /// Allows an application to set multiple column values in a single
        /// operation. An array of <see cref="NATIVE_SETCOLUMN"/> structures is
        /// used to describe the set of column values to be set, and to describe
        /// input buffers for each column value to be set.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to set the columns on.</param>
        /// <param name="setcolumns">
        /// An array of <see cref="NATIVE_SETCOLUMN"/> structures describing the
        /// data to set.
        /// </param>
        /// <param name="numColumns">
        /// Number of entries in the setcolumns parameter.
        /// </param>
        /// <returns>An error code or warning.</returns>
        public unsafe int JetSetColumns(JET_SESID sesid, JET_TABLEID tableid, NATIVE_SETCOLUMN* setcolumns, int numColumns)
        {
            this.TraceFunctionCall("JetSetColumns");
            return this.Err(NativeMethods.JetSetColumns(sesid.Value, tableid.Value, setcolumns, checked((uint)numColumns)));
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
        /// <returns>An error if the call fails.</returns>
        public int JetGetLock(JET_SESID sesid, JET_TABLEID tableid, GetLockGrbit grbit)
        {
            this.TraceFunctionCall("JetGetLock");
            return this.Err(NativeMethods.JetGetLock(sesid.Value, tableid.Value, unchecked((uint)grbit)));
        }

        /// <summary>
        /// Performs an atomic addition operation on one column. This function allows
        /// multiple sessions to update the same record concurrently without conflicts.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
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
        /// <returns>An error code if the operation fails.</returns>
        public int JetEscrowUpdate(
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
            this.TraceFunctionCall("JetEscrowUpdate");
            this.CheckNotNull(delta, "delta");
            this.CheckDataSize(delta, deltaSize, "deltaSize");
            this.CheckDataSize(previousValue, previousValueLength, "previousValueLength");

            uint cbOldActual;
            int err = this.Err(NativeMethods.JetEscrowUpdate(
                                  sesid.Value,
                                  tableid.Value,
                                  columnid.Value,
                                  delta,
                                  checked((uint)deltaSize),
                                  previousValue,
                                  checked((uint)previousValueLength),
                                  out cbOldActual,
                                  unchecked((uint)grbit)));
            actualPreviousValueLength = checked((int)cbOldActual);
            return err;
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
        /// <returns>An error if the call fails.</returns>
        public int JetRegisterCallback(
            JET_SESID sesid,
            JET_TABLEID tableid,
            JET_cbtyp cbtyp,
            JET_CALLBACK callback,
            IntPtr context,
            out JET_HANDLE callbackId)
        {
            this.TraceFunctionCall("JetRegisterCallback");
            this.CheckNotNull(callback, "callback");

            callbackId = JET_HANDLE.Nil;
            return this.Err(NativeMethods.JetRegisterCallback(
                sesid.Value,
                tableid.Value,
                unchecked((uint)cbtyp), 
                this.callbackWrappers.Add(callback).NativeCallback,
                context,
                out callbackId.Value));
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
        /// <returns>An error if the call fails.</returns>
        public int JetUnregisterCallback(JET_SESID sesid, JET_TABLEID tableid, JET_cbtyp cbtyp, JET_HANDLE callbackId)
        {
            this.TraceFunctionCall("JetUnregisterCallback");
            this.callbackWrappers.Collect();
            return this.Err(NativeMethods.JetUnregisterCallback(
                sesid.Value,
                tableid.Value,
                unchecked((uint)cbtyp),
                callbackId.Value));
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
        /// <returns>An error code.</returns>
        public int JetDefragment(JET_SESID sesid, JET_DBID dbid, string tableName, ref int passes, ref int seconds, DefragGrbit grbit)
        {
            this.TraceFunctionCall("JetDefragment");
            uint nativePasses = unchecked((uint)passes);
            uint nativeSeconds = unchecked((uint)seconds);
            int err = this.Err(NativeMethods.JetDefragment(
                sesid.Value, dbid.Value, tableName, ref nativePasses, ref nativeSeconds, (uint)grbit));
            passes = unchecked((int)nativePasses);
            seconds = unchecked((int)nativeSeconds);
            return err;
        }

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
        /// <param name="callback">Callback function that defrag uses to report progress.</param>
        /// <param name="grbit">Defragmentation options.</param>
        /// <returns>An error code or warning.</returns>
        public int JetDefragment2(
            JET_SESID sesid,
            JET_DBID dbid,
            string tableName,
            ref int passes,
            ref int seconds,
            JET_CALLBACK callback,
            DefragGrbit grbit)
        {
            this.TraceFunctionCall("JetDefragment2");
            uint nativePasses = unchecked((uint)passes);
            uint nativeSeconds = unchecked((uint)seconds);

            IntPtr functionPointer;
            if (null == callback)
            {
                functionPointer = IntPtr.Zero;
            }
            else
            {
                JetCallbackWrapper callbackWrapper = this.callbackWrappers.Add(callback);
                functionPointer = Marshal.GetFunctionPointerForDelegate(callbackWrapper.NativeCallback);
#if DEBUG
                GC.Collect();
#endif
            }

            int err = this.Err(NativeMethods.JetDefragment2(
                sesid.Value, dbid.Value, tableName, ref nativePasses, ref nativeSeconds, functionPointer, (uint)grbit));
            passes = unchecked((int)nativePasses);
            seconds = unchecked((int)nativeSeconds);
            this.callbackWrappers.Collect();
            return err;
        }

        /// <summary>
        /// Performs idle cleanup tasks or checks the version store status in ESE.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="grbit">A combination of JetIdleGrbit flags.</param>
        /// <returns>An error code if the operation fails.</returns>
        public int JetIdle(JET_SESID sesid, IdleGrbit grbit)
        {
            this.TraceFunctionCall("JetIdle");
            return this.Err(NativeMethods.JetIdle(sesid.Value, (uint)grbit));
        }

        #endregion

        #region Misc

        /// <summary>
        /// Crash dump options for Watson.
        /// </summary>
        /// <param name="grbit">Crash dump options.</param>
        /// <returns>An error code.</returns>
        public int JetConfigureProcessForCrashDump(CrashDumpGrbit grbit)
        {
            this.TraceFunctionCall("JetConfigureProcessForCrashDump");
            this.CheckSupportsWindows7Features("JetConfigureProcessForCrashDump");
            return this.Err(NativeMethods.JetConfigureProcessForCrashDump((uint)grbit));
        }

        /// <summary>
        /// Frees memory that was allocated by a database engine call.
        /// </summary>
        /// <param name="buffer">
        /// The buffer allocated by a call to the database engine.
        /// <see cref="IntPtr.Zero"/> is acceptable, and will be ignored.
        /// </param>
        /// <returns>An error code.</returns>
        public int JetFreeBuffer(IntPtr buffer)
        {
            this.TraceFunctionCall("JetFreeBuffer");
            return this.Err(NativeMethods.JetFreeBuffer(buffer));
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Convert managed JET_ENUMCOLUMNID objects to NATIVE_ENUMCOLUMNID
        /// structures.
        /// </summary>
        /// <param name="columnids">The columnids to convert.</param>
        /// <param name="numColumnids">The number of columnids to convert.</param>
        /// <param name="nativecolumnids">The array to store the converted columnids.</param>
        /// <returns>The total number of tag entries in the converted structures.</returns>
        private static unsafe int ConvertEnumColumnids(JET_ENUMCOLUMNID[] columnids, int numColumnids, NATIVE_ENUMCOLUMNID* nativecolumnids)
        {
            int totalNumTags = 0;
            for (int i = 0; i < numColumnids; ++i)
            {
                nativecolumnids[i] = columnids[i].GetNativeEnumColumnid();
                checked
                {
                    totalNumTags += columnids[i].ctagSequence;                    
                }
            }

            return totalNumTags;
        }

        /// <summary>
        /// Convert managed rgtagSequence to unmanaged rgtagSequence.
        /// </summary>
        /// <param name="columnids">The columnids to convert.</param>
        /// <param name="numColumnids">The number of columnids to covert.</param>
        /// <param name="nativecolumnids">The unmanaged columnids to add the tags to.</param>
        /// <param name="tags">
        /// Memory to use for converted rgtagSequence. This should be large enough to
        /// hold all columnids.
        /// </param>
        private static unsafe void ConvertEnumColumnidTags(JET_ENUMCOLUMNID[] columnids, int numColumnids, NATIVE_ENUMCOLUMNID* nativecolumnids, uint* tags)
        {
            for (int i = 0; i < numColumnids; ++i)
            {
                nativecolumnids[i].rgtagSequence = tags;
                for (int j = 0; j < columnids[i].ctagSequence; ++j)
                {
                    nativecolumnids[i].rgtagSequence[j] = checked((uint)columnids[i].rgtagSequence[j]);
                }

                tags += columnids[i].ctagSequence;
            }
        }

        /// <summary>
        /// Convert the native (unmanaged) results of JetEnumerateColumns to
        /// managed objects. This uses the allocator callback to free some
        /// memory as the data is converted.
        /// </summary>
        /// <param name="allocator">The allocator callback used.</param>
        /// <param name="allocatorContext">The allocator callback context.</param>
        /// <param name="cEnumColumn">Number of NATIVE_ENUMCOLUMN structures returned.</param>
        /// <param name="nativeenumcolumns">NATIVE_ENUMCOLUMN structures.</param>
        /// <param name="numColumnValues">Returns the number of converted JET_ENUMCOLUMN objects.</param>
        /// <param name="columnValues">Returns the convertd column values.</param>
        private static unsafe void ConvertEnumerateColumnsResult(JET_PFNREALLOC allocator, IntPtr allocatorContext, uint cEnumColumn, NATIVE_ENUMCOLUMN* nativeenumcolumns, out int numColumnValues, out JET_ENUMCOLUMN[] columnValues)
        {
            numColumnValues = checked((int)cEnumColumn);
            columnValues = new JET_ENUMCOLUMN[numColumnValues];
            for (int i = 0; i < numColumnValues; ++i)
            {
                columnValues[i] = new JET_ENUMCOLUMN();
                columnValues[i].SetFromNativeEnumColumn(nativeenumcolumns[i]);
                if (JET_wrn.ColumnSingleValue != columnValues[i].err)
                {
                    columnValues[i].rgEnumColumnValue = new JET_ENUMCOLUMNVALUE[columnValues[i].cEnumColumnValue];
                    for (int j = 0; j < columnValues[i].cEnumColumnValue; ++j)
                    {
                        columnValues[i].rgEnumColumnValue[j] = new JET_ENUMCOLUMNVALUE();
                        columnValues[i].rgEnumColumnValue[j].SetFromNativeEnumColumnValue(nativeenumcolumns[i].rgEnumColumnValue[j]);
                    }

                    // the NATIVE_ENUMCOLUMNVALUES have been converted
                    // free their memory
                    allocator(allocatorContext, new IntPtr(nativeenumcolumns[i].rgEnumColumnValue), 0);
                    nativeenumcolumns[i].rgEnumColumnValue = null;
                }
            }

            // Now we have converted all the NATIVE_ENUMCOLUMNS we can
            // free the memory they use
            allocator(allocatorContext, new IntPtr(nativeenumcolumns), 0);
            nativeenumcolumns = null;
        }

        /// <summary>
        /// Make an array of native columndefs from JET_COLUMNDEFs.
        /// </summary>
        /// <param name="columns">Columndefs to convert.</param>
        /// <param name="numColumns">Number of columndefs to convert.</param>
        /// <returns>An array of native columndefs.</returns>
        private static NATIVE_COLUMNDEF[] GetNativecolumndefs(JET_COLUMNDEF[] columns, int numColumns)
        {
            var nativecolumndefs = new NATIVE_COLUMNDEF[numColumns];
            for (int i = 0; i < numColumns; ++i)
            {
                nativecolumndefs[i] = columns[i].GetNativeColumndef();
            }

            return nativecolumndefs;
        }

        /// <summary>
        /// Set managed columnids from unmanaged columnids. This also sets the columnids
        /// in the columndefs.
        /// </summary>
        /// <param name="columns">The column definitions.</param>
        /// <param name="columnids">The columnids to set.</param>
        /// <param name="nativecolumnids">The native columnids.</param>
        /// <param name="numColumns">The number of columnids to set.</param>
        private static void SetColumnids(JET_COLUMNDEF[] columns, JET_COLUMNID[] columnids, uint[] nativecolumnids, int numColumns)
        {
            for (int i = 0; i < numColumns; ++i)
            {
                columnids[i] = new JET_COLUMNID { Value = nativecolumnids[i] };
                columns[i].columnid = columnids[i];
            }
        }

        /// <summary>
        /// Convert native instance info structures to managed, treating the
        /// unmanaged strings as Unicode.
        /// </summary>
        /// <param name="nativeNumInstance">The number of native structures.</param>
        /// <param name="nativeInstanceInfos">
        /// A pointer to the native structures. This pointer will be freed with JetFreeBuffer.
        /// </param>
        /// <returns>
        /// An array of JET_INSTANCE_INFO structures created from the unmanaged.
        /// </returns>
        private unsafe JET_INSTANCE_INFO[] ConvertInstanceInfosUnicode(uint nativeNumInstance, NATIVE_INSTANCE_INFO* nativeInstanceInfos)
        {
            int numInstances = checked((int)nativeNumInstance);
            var instances = new JET_INSTANCE_INFO[numInstances];
            for (int i = 0; i < numInstances; ++i)
            {
                instances[i] = new JET_INSTANCE_INFO();
                instances[i].SetFromNativeUnicode(nativeInstanceInfos[i]);
            }

            this.JetFreeBuffer(new IntPtr(nativeInstanceInfos));
            return instances;
        }

        /// <summary>
        /// Convert native instance info structures to managed, treating the
        /// unmanaged string as Unicode.
        /// </summary>
        /// <param name="nativeNumInstance">The number of native structures.</param>
        /// <param name="nativeInstanceInfos">
        /// A pointer to the native structures. This pointer will be freed with JetFreeBuffer.
        /// </param>
        /// <returns>
        /// An array of JET_INSTANCE_INFO structures created from the unmanaged.
        /// </returns>
        private unsafe JET_INSTANCE_INFO[] ConvertInstanceInfosAscii(uint nativeNumInstance, NATIVE_INSTANCE_INFO* nativeInstanceInfos)
        {
            int numInstances = checked((int)nativeNumInstance);
            var instances = new JET_INSTANCE_INFO[numInstances];
            for (int i = 0; i < numInstances; ++i)
            {
                instances[i] = new JET_INSTANCE_INFO();
                instances[i].SetFromNativeAscii(nativeInstanceInfos[i]);
            }

            this.JetFreeBuffer(new IntPtr(nativeInstanceInfos));
            return instances;
        }

        /// <summary>
        /// Creates indexes over data in an ESE database.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to create the index on.</param>
        /// <param name="indexcreates">Array of objects describing the indexes to be created.</param>
        /// <param name="numIndexCreates">Number of index description objects.</param>
        /// <returns>An error code.</returns>
        private int CreateIndexes(JET_SESID sesid, JET_TABLEID tableid, JET_INDEXCREATE[] indexcreates, int numIndexCreates)
        {
            var nativeIndexcreates = new NATIVE_INDEXCREATE[indexcreates.Length];
            for (int i = 0; i < numIndexCreates; ++i)
            {
                nativeIndexcreates[i] = indexcreates[i].GetNativeIndexcreate();
            }

            // pin the memory
            unsafe
            {
                using (var handles = new GCHandleCollection())
                {
                    for (int i = 0; i < numIndexCreates; ++i)
                    {
                        if (null != indexcreates[i].pidxUnicode)
                        {
                            NATIVE_UNICODEINDEX unicode = indexcreates[i].pidxUnicode.GetNativeUnicodeIndex();
                            nativeIndexcreates[i].pidxUnicode = (NATIVE_UNICODEINDEX*) handles.Add(unicode);
                            nativeIndexcreates[i].grbit |= (uint)VistaGrbits.IndexUnicode;
                        }
                        ////nativeIndexcreates[i].rgconditionalcolumn = handles.Add(indexcreates[i].rgconditionalcolumn);
                    }

                    return
                        this.Err(
                            NativeMethods.JetCreateIndex2(
                                sesid.Value, tableid.Value, nativeIndexcreates, checked((uint)numIndexCreates)));
                }
            }
        }

        /// <summary>
        /// Creates indexes over data in an ESE database.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to create the index on.</param>
        /// <param name="indexcreates">Array of objects describing the indexes to be created.</param>
        /// <param name="numIndexCreates">Number of index description objects.</param>
        /// <returns>An error code.</returns>
        private int CreateIndexes2(JET_SESID sesid, JET_TABLEID tableid, JET_INDEXCREATE[] indexcreates, int numIndexCreates)
        {
            var nativeIndexcreates = new NATIVE_INDEXCREATE2[indexcreates.Length];
            for (int i = 0; i < numIndexCreates; ++i)
            {
                nativeIndexcreates[i] = indexcreates[i].GetNativeIndexcreate2();
            }

            // pin the memory
            unsafe
            {
                using (var handles = new GCHandleCollection())
                {
                    for (int i = 0; i < numIndexCreates; ++i)
                    {
                        if (null != indexcreates[i].pidxUnicode)
                        {
                            NATIVE_UNICODEINDEX unicode = indexcreates[i].pidxUnicode.GetNativeUnicodeIndex();
                            nativeIndexcreates[i].indexcreate.pidxUnicode = (NATIVE_UNICODEINDEX*) handles.Add(unicode);
                            nativeIndexcreates[i].indexcreate.grbit |= (uint)VistaGrbits.IndexUnicode;
                        }
                        ////nativeIndexcreates[i].rgconditionalcolumn = handles.Add(indexcreates[i].rgconditionalcolumn);
                    }

                    return
                        this.Err(
                            NativeMethods.JetCreateIndex2(sesid.Value, tableid.Value, nativeIndexcreates, checked((uint)numIndexCreates)));
                }                
            }
        }

        #endregion

        #region Capability Checking

        /// <summary>
        /// Check that ESENT supports Server 2003 features. Throws an exception if Server 2003 features
        /// aren't supported.
        /// </summary>
        /// <param name="api">The API that is being called.</param>
        private void CheckSupportsServer2003Features(string api)
        {
            if (!this.Capabilities.SupportsServer2003Features)
            {
                this.ThrowUnsupportedApiException(api);
            }
        }

        /// <summary>
        /// Check that ESENT supports Vista features. Throws an exception if Vista features
        /// aren't supported.
        /// </summary>
        /// <param name="api">The API that is being called.</param>
        private void CheckSupportsVistaFeatures(string api)
        {
            if (!this.Capabilities.SupportsVistaFeatures)
            {
                this.ThrowUnsupportedApiException(api);
            }
        }

        /// <summary>
        /// Check that ESENT supports Windows7 features. Throws an exception if Windows7 features
        /// aren't supported.
        /// </summary>
        /// <param name="api">The API that is being called.</param>
        private void CheckSupportsWindows7Features(string api)
        {
            if (!this.Capabilities.SupportsWindows7Features)
            {
                this.ThrowUnsupportedApiException(api);
            }
        }

        #endregion

        #region Parameter Checking and Tracing

        /// <summary>
        /// Make sure the data and dataSize arguments match.
        /// </summary>
        /// <param name="data">The data buffer.</param>
        /// <param name="dataSize">The size of the data.</param>
        /// <param name="argumentName">The name of the size argument.</param>
        /// <typeparam name="T">The type of the data.</typeparam>
        private void CheckDataSize<T>(ICollection<T> data, int dataSize, string argumentName)
        {
            this.CheckNotNegative(dataSize, argumentName);
            if ((null == data && 0 != dataSize) || (null != data && dataSize > data.Count))
            {
                Trace.WriteLineIf(this.traceSwitch.TraceError, "CheckDataSize failed");
                throw new ArgumentOutOfRangeException(
                    argumentName,
                    dataSize,
                    "cannot be greater than the length of the buffer");
            }
        }

        /// <summary>
        /// Make sure the given object isn't null. If it is
        /// then throw an ArgumentNullException.
        /// </summary>
        /// <param name="o">The object to check.</param>
        /// <param name="paramName">The name of the parameter.</param>
        private void CheckNotNull(object o, string paramName)
        {
            if (null == o)
            {
                Trace.WriteLineIf(this.traceSwitch.TraceError, "CheckNotNull failed");
                throw new ArgumentNullException(paramName);
            }
        }

        /// <summary>
        /// Make sure the given integer isn't negative. If it is
        /// then throw an ArgumentOutOfRangeException.
        /// </summary>
        /// <param name="i">The integer to check.</param>
        /// <param name="paramName">The name of the parameter.</param>
        private void CheckNotNegative(int i, string paramName)
        {
            if (i < 0)
            {
                Trace.WriteLineIf(this.traceSwitch.TraceError, "CheckNotNegative failed");
                throw new ArgumentOutOfRangeException(paramName, i, "cannot be negative");
            }
        }

        /// <summary>
        /// Used when an unsupported API method is called. This 
        /// logs an error and throws an InvalidOperationException.
        /// </summary>
        /// <param name="method">The name of the method.</param>
        private void ThrowUnsupportedApiException(string method)
        {
            string error = String.Format(CultureInfo.InvariantCulture, "Method {0} is not supported by this version of ESENT", method);
            Trace.WriteLineIf(this.traceSwitch.TraceError, error);
            throw new InvalidOperationException(error);
        }

        /// <summary>
        /// Trace a call to an ESENT function.
        /// </summary>
        /// <param name="function">The name of the function being called.</param>
        [Conditional("TRACE")]
        private void TraceFunctionCall(string function)
        {
            Trace.WriteLineIf(this.traceSwitch.TraceInfo, function);
        }

        /// <summary>
        /// Can be used to trap ESENT errors.
        /// </summary>
        /// <param name="err">The error being returned.</param>
        /// <returns>The error.</returns>
        private int Err(int err)
        {
            this.TraceErr(err);
            return err;
        }

        /// <summary>
        /// Trace an error generated by a call to ESENT.
        /// </summary>
        /// <param name="err">The error to trace.</param>
        [Conditional("TRACE")]
        private void TraceErr(int err)
        {
            if (0 == err)
            {
                Trace.WriteLineIf(this.traceSwitch.TraceVerbose, "JET_err.Success");
            }
            else if (err > 0)
            {
                Trace.WriteLineIf(this.traceSwitch.TraceWarning, unchecked((JET_wrn)err));
            }
            else
            {
                Trace.WriteLineIf(this.traceSwitch.TraceError, unchecked((JET_err)err));
            }
        }

        #endregion Parameter Checking and Tracing
    }
}
