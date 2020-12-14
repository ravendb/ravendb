using System;
using System.Collections.Generic;
using System.Threading;
using Lucene.Net.Search;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.DiskIO;
using Raven.Database.Plugins;
using Raven.Database.Raft;
using Raven.Database.Server.Connections;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi;
using Raven.Database.Storage.Esent;

namespace Raven.Database.Server
{
    public sealed class RavenDBOptions : IDisposable
    {
        private static readonly ILog log = LogManager.GetCurrentClassLogger();

        private readonly DatabasesLandlord databasesLandlord;
        private readonly MixedModeRequestAuthorizer mixedModeRequestAuthorizer;
        private readonly DocumentDatabase systemDatabase;
        private readonly RequestManager requestManager;
        private readonly FileSystemsLandlord fileSystemLandlord;
        private readonly CountersLandlord countersLandlord;
        private readonly TimeSeriesLandlord timeSeriesLandlord;

        private readonly IList<IDisposable> toDispose = new List<IDisposable>();
        private readonly IEnumerable<IServerStartupTask> serverStartupTasks;

        private bool preventDisposing;

        public RavenDBOptions(InMemoryRavenConfiguration configuration, DocumentDatabase db = null)
        {
            if (configuration == null)
                throw new ArgumentNullException("configuration");
            
            try
            {
                ThreadPool.SetMinThreads(configuration.MinThreadPoolWorkerThreads, configuration.MinThreadPoolCompletionThreads);
                HttpEndpointRegistration.RegisterHttpEndpointTarget();
                HttpEndpointRegistration.RegisterAdminLogsTarget();
                if (db == null)
                {
                    configuration.UpdateDataDirForLegacySystemDb();

                    BooleanQuery.MaxClauseCount = configuration.MaxClauseCount;

                    // initialize before starting the first esent instance
                    SetMaxInstances(configuration);

                    systemDatabase = new DocumentDatabase(configuration, null, null, (sender, exception) =>
                    {
                        if (log.IsInfoEnabled)
                        {
                            log.ErrorException(
                                @"Found errors in the system database while loading it for the first time.
                                    This is recoverable error, since we will simply ingore transactions after the faulted one.",exception);
                        }
                    });
                    systemDatabase.SpinBackgroundWorkers(false);
                }
                else
                {
                    systemDatabase = db;
                }

                WebSocketBufferPool.Initialize(configuration.WebSockets.InitialBufferPoolSize);
                fileSystemLandlord = new FileSystemsLandlord(systemDatabase);
                databasesLandlord = new DatabasesLandlord(systemDatabase);
                countersLandlord = new CountersLandlord(systemDatabase);
                timeSeriesLandlord = new TimeSeriesLandlord(systemDatabase);
                requestManager = new RequestManager(databasesLandlord);
                systemDatabase.RequestManager = requestManager;
                ClusterManager = new Reference<ClusterManager>();
                systemDatabase.ClusterManager = ClusterManager;
                mixedModeRequestAuthorizer = new MixedModeRequestAuthorizer();
                mixedModeRequestAuthorizer.Initialize(systemDatabase, new RavenServer(databasesLandlord.SystemDatabase, configuration));

                serverStartupTasks = configuration.Container.GetExportedValues<IServerStartupTask>();

                foreach (var task in serverStartupTasks)
                {
                    toDispose.Add(task);
                    try
                    {
                        task.Execute(this);
                    }
                    catch (Exception e)
                    {
                        systemDatabase.LogErrorAndAddAlertOnStartupTaskException(task.GetType().FullName, e);
                    }
                }
            }
            catch (Exception e)
            {
                if (systemDatabase != null)
                    systemDatabase.Dispose();
                throw;
            }
        }

        public IEnumerable<IServerStartupTask> ServerStartupTasks
        {
            get { return serverStartupTasks; }
        }

        public DocumentDatabase SystemDatabase
        {
            get { return systemDatabase; }
        }

        public MixedModeRequestAuthorizer MixedModeRequestAuthorizer
        {
            get { return mixedModeRequestAuthorizer; }
        }

        public DatabasesLandlord DatabaseLandlord
        {
            get { return databasesLandlord; }
        }
        public FileSystemsLandlord FileSystemLandlord
        {
            get { return fileSystemLandlord; }
        }

        public CountersLandlord CountersLandlord
        {
            get { return countersLandlord; }
        }

        public TimeSeriesLandlord TimeSeriesLandlord
        {
            get { return timeSeriesLandlord; }
        }

        public RequestManager RequestManager
        {
            get { return requestManager; }
        }

        public Reference<ClusterManager> ClusterManager { get; private set; }

        public bool Disposed { get; private set; }

        public void Dispose()
        {
            if (preventDisposing || Disposed)
                return;

            Disposed = true;

            toDispose.Add(mixedModeRequestAuthorizer);
            toDispose.Add(databasesLandlord);
            toDispose.Add(fileSystemLandlord);
            toDispose.Add(systemDatabase);
            toDispose.Add(LogManager.GetTarget<AdminLogsTarget>());
            toDispose.Add(requestManager);
            toDispose.Add(countersLandlord);
            toDispose.Add(ClusterManager.Value);

            var errors = new List<Exception>();

            foreach (var disposable in toDispose)
            {
                try
                {
                    if (disposable != null)
                        disposable.Dispose();
                }
                catch (Exception e)
                {
                    errors.Add(e);
                }
            }

            if (errors.Count != 0)
                throw new AggregateException(errors);
        }

        public IDisposable PreventDispose()
        {
            preventDisposing = true;

            return new DisposableAction(() => preventDisposing = false);
        }

        private class RavenServer : IRavenServer
        {
            private readonly InMemoryRavenConfiguration systemConfiguration;
            private readonly DocumentDatabase systemDatabase;

            public RavenServer(DocumentDatabase systemDatabase, InMemoryRavenConfiguration systemConfiguration)
            {
                this.systemDatabase = systemDatabase;
                this.systemConfiguration = systemConfiguration;
            }

            public DocumentDatabase SystemDatabase
            {
                get { return systemDatabase; }
            }

            public InMemoryRavenConfiguration SystemConfiguration
            {
                get { return systemConfiguration; }
            }
        }

        private static void SetMaxInstances(InMemoryRavenConfiguration configuration)
        {
            try
            {
                var maxInstances = configuration.Storage.Esent.MaxInstances;
                SystemParameters.MaxInstances = maxInstances;
            }
            catch (EsentErrorException e)
            {
                // this is expected if we had done something like recycling the app domain
                // because the engine state is actually at the process level (unmanaged)
                // so we ignore this error
                if (e.Error == JET_err.AlreadyInitialized)
                    return;

                throw;
            }
        }
    }
}
