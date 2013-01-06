//-----------------------------------------------------------------------
// <copyright file="ConnectionManager.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Microsoft.Isam.Esent.Interop;
using Microsoft.Isam.Esent.Interop.Windows7;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// For each database that we open/create we want just one instance.
    /// This class keeps track of instance => database mappings and terminates
    /// instances when finished.
    /// </summary>
    internal class ConnectionManager : IConnectionManager
    {
        /// <summary>
        /// This maps a database path to the instance which has the database attached.
        /// </summary>
        private readonly Dictionary<string, InstanceInfo> instances;

        /// <summary>
        /// Used to lock the obect for concurrency.
        /// </summary>
        private readonly object lockObject;

        /// <summary>
        /// Used to generate instance names.
        /// </summary>
        private int instanceCounter;

        /// <summary>
        /// Set to true once the global parameters have been set.
        /// </summary>
        private bool globalParametersHaveBeenSet;

        /// <summary>
        /// Initializes a new instance of the ConnectionManager class.
        /// </summary>
        public ConnectionManager()
        {
            this.Tracer = new Tracer("ConnectionManager", "Esent Connection Factory", "ConnectionManager");
            this.instances = new Dictionary<string, InstanceInfo>(StringComparer.InvariantCultureIgnoreCase);
            this.lockObject = new object();
            this.Tracer.TraceVerbose("created singleton");
        }

        /// <summary>
        /// Gets or sets the tracing object for this ConnectionManager.
        /// </summary>
        private Tracer Tracer { get; set; }

        /// <summary>
        /// Create a new database and return a connection to
        /// the database. The database will be overwritten if
        /// it already exists.
        /// </summary>
        /// <param name="database">The path to the database.</param>
        /// <param name="mode">Creation mode for the database.</param>
        /// <returns>A new connection to the database.</returns>
        public virtual Connection CreateDatabase(string database, DatabaseCreationMode mode)
        {
            database = Path.GetFullPath(database);

            lock (this.lockObject)
            {
                this.SetGlobalParameters();
                this.Tracer.TraceInfo("create database '{0}'", database);

                // Create the database then open it
                using (var instance = new Instance(this.GetNewInstanceName()))
                {
                    SetParametersAndInitializeInstance(database, instance);

                    using (var session = new Session(instance))
                    {
                        CreateDatabaseGrbit grbit = (DatabaseCreationMode.OverwriteExisting == mode) ?
                            CreateDatabaseGrbit.OverwriteExisting : CreateDatabaseGrbit.None;

                        JET_DBID dbid;
                        Api.JetCreateDatabase(session, database, String.Empty, out dbid, grbit);
                        Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
                        Api.JetDetachDatabase(session, database);
                    }
                }

                return this.AttachDatabase(database, DatabaseOpenMode.ReadWrite);
            }
        }

        /// <summary>
        /// Attach an existing database and return a connection to
        /// the database.
        /// </summary>
        /// <param name="database">The path to the database.</param>
        /// <param name="mode">The mode to open the database in.</param>
        /// <returns>A new connection to the database.</returns>
        public virtual Connection AttachDatabase(string database, DatabaseOpenMode mode)
        {
            database = Path.GetFullPath(database);

            lock (this.lockObject)
            {
                this.SetGlobalParameters();
                this.Tracer.TraceInfo("attach database '{0}'", database);

                if (!this.instances.ContainsKey(database))
                {
                    return this.CreateNewInstanceAndConnection(database, mode);
                }

                return this.CreateNewConnection(database, mode);
            }
        }

        /// <summary>
        /// Set parameters and initialize the instance.
        /// </summary>
        /// <param name="database">The database that will be used by the instance.</param>
        /// <param name="instance">The instance to initialize.</param>
        private static void SetParametersAndInitializeInstance(string database, Instance instance)
        {
            SetDefaultInstanceParameters(instance);
            SetPathParameters(instance, database);

            InitGrbit grbit = EsentVersion.SupportsWindows7Features
                                  ? Windows7Grbits.ReplayIgnoreLostLogs
                                  : InitGrbit.None;
            instance.Init(grbit);
        }

        /// <summary>
        /// Set the default parameters for the instance.
        /// </summary>
        /// <param name="instance">The instance to set the default parameters on.</param>
        private static void SetDefaultInstanceParameters(Instance instance)
        {
            var defaultParameters = Dependencies.Container.Resolve<IEnumerable<JetParameter>>("DefaultInstanceParameters");
            foreach (JetParameter parameter in defaultParameters)
            {
                parameter.SetParameter(instance);
            }
        }

        /// <summary>
        /// Set the instance path parameters for the given database.
        /// </summary>
        /// <param name="instance">The instance to set the parameters on.</param>
        /// <param name="database">The database the instance will be using or creating.</param>
        private static void SetPathParameters(Instance instance, string database)
        {
            var parameters = new InstanceParameters(instance);
            string directory = Path.GetDirectoryName(Path.GetFullPath(database));
            parameters.SystemDirectory = directory;
            parameters.TempDirectory = directory;
            parameters.LogFileDirectory = directory;
        }

        /// <summary>
        /// Create a new instance and a connection on that instance.
        /// A new InstanceInfo is added to the instance dictionary.
        /// </summary>
        /// <param name="database">The database to connect to.</param>
        /// <param name="mode">The mode to connect to the database in.</param>
        /// <returns>A new Connection to the database.</returns>
        private Connection CreateNewInstanceAndConnection(string database, DatabaseOpenMode mode)
        {
            var instanceName = this.GetNewInstanceName();
            var instance = new Instance(instanceName);
            this.Tracer.TraceInfo("created instance '{0}'", instanceName);
            try
            {
                SetParametersAndInitializeInstance(database, instance);
                this.instances[database] = new InstanceInfo(instance, database);
                return this.CreateNewConnection(database, mode);
            }
            catch (Exception)
            {
                // Creating the new instance failed. Terminate ESE and remove the 
                // instance information.
                instance.Term();
                this.instances.Remove(database);
                throw;
            }
        }

        /// <summary>
        /// Create a new connection to an existing instance.
        /// </summary>
        /// <param name="database">The database to connect to.</param>
        /// <param name="mode">The mode to connect to the database in.</param>
        /// <returns>A new connection to the database.</returns>
        private Connection CreateNewConnection(string database, DatabaseOpenMode mode)
        {
            Instance instance = this.instances[database].Instance;
            string connectionName = Path.GetFileName(database);
            ConnectionBase connection;
            if (DatabaseOpenMode.ReadOnly == mode)
            {
                connection = new ReadOnlyConnection(instance, connectionName, database);
            }
            else
            {
                Debug.Assert(DatabaseOpenMode.ReadWrite == mode, "Unknown DatabaseOpenMode");
                connection = new ReadWriteConnection(instance, connectionName, database);
            }

            this.instances[database].Connections.Add(connection);
            connection.Disposed += this.OnConnectionClose;

            this.Tracer.TraceVerbose("created new connection '{0}'", connectionName);
            this.Tracer.TraceVerbose("database '{0}' has {1} connections", database, this.instances[database].Connections.Count);
            return connection;
        }

        /// <summary>
        /// Called when a connection is closed. This removes the connection from the list of
        /// connections and terminates the instance if necessary.
        /// </summary>
        /// <param name="connection">The connection being closed.</param>
        private void OnConnectionClose(ConnectionBase connection)
        {
            lock (this.lockObject)
            {
                connection.Disposed -= this.OnConnectionClose;

                this.Tracer.TraceVerbose("closing connection '{0}' (database '{1}')", connection.Name, connection.Database);

                string database = connection.Database;
                this.instances[database].Connections.Remove(connection);

                this.Tracer.TraceVerbose("database '{0}' has {1} connections left", database, this.instances[database].Connections.Count);

                if (0 == this.instances[database].Connections.Count())
                {
                    this.Tracer.TraceInfo("no connections left to database '{0}'. Terminating ese", database);

                    // all connections are closed, terminate the instance
                    this.instances[database].Instance.Term();
                    this.instances.Remove(database);
                }
            }
        }

        /// <summary>
        /// Set the global parameters, if not already set. This should be called before creating
        /// the first instance.
        /// </summary>
        private void SetGlobalParameters()
        {
            if (!this.globalParametersHaveBeenSet)
            {
                var globalParameters = Dependencies.Container.Resolve<IEnumerable<JetParameter>>("GlobalParameters");
                foreach (JetParameter parameter in globalParameters)
                {
                    parameter.SetParameter();
                }

                this.globalParametersHaveBeenSet = true;
            }

            Debug.Assert(this.globalParametersHaveBeenSet, "Global parameters have not been set");
        }

        /// <summary>
        /// Gets a new, unique, instance name. The object should be locked when this is called.
        /// </summary>
        /// <returns>A new instance name.</returns>
        private string GetNewInstanceName()
        {
            this.instanceCounter++;
            return String.Format("Pixie_{0}", this.instanceCounter);
        }

        /// <summary>
        /// Information about one ESE database/instance.
        /// </summary>
        private class InstanceInfo
        {
            /// <summary>
            /// Initializes a new instance of the InstanceInfo class.
            /// </summary>
            /// <param name="instance">The ESE instance</param>
            /// <param name="database">The name of the database this instance is for.</param>
            public InstanceInfo(Instance instance, string database)
            {
                this.Connections = new List<Connection>();
                this.Instance = instance;
                this.Database = database;
            }

            /// <summary>
            /// Gets or sets the full path to the database.
            /// </summary>
            public string Database { get; private set; }

            /// <summary>
            /// Gets or sets the instance that has the database open.
            /// </summary>
            public Instance Instance { get; private set; }

            /// <summary>
            /// Gets or sets all the connections for the database.
            /// </summary>
            public List<Connection> Connections { get; private set; }
        }
    }
}
