using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;

#if DEBUG

using System.Text;

#endif

using System.Threading;
using System.Threading.Tasks;
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Pipeline;
using Lextm.SharpSnmpLib.Security;
using Raven.Client;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Monitoring.Snmp.Objects.Cluster;
using Raven.Server.Monitoring.Snmp.Objects.Database;
using Raven.Server.Monitoring.Snmp.Objects.Server;
using Raven.Server.Monitoring.Snmp.Providers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Monitoring.Snmp;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using ServerVersion = Raven.Server.Monitoring.Snmp.Objects.Server.ServerVersion;
using TimeoutException = System.TimeoutException;

namespace Raven.Server.Monitoring.Snmp
{
    public class SnmpWatcher
    {
        private readonly ConcurrentDictionary<string, SnmpDatabase> _loadedDatabases = new ConcurrentDictionary<string, SnmpDatabase>(StringComparer.OrdinalIgnoreCase);

        private readonly SemaphoreSlim _locker = new SemaphoreSlim(1, 1);

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<SnmpWatcher>("Server");

        private readonly RavenServer _server;

        private ObjectStore _objectStore;

        private SnmpEngine _snmpEngine;

        public SnmpWatcher(RavenServer server)
        {
            _server = server;
            _server.ServerStore.LicenseManager.LicenseChanged += OnLicenseChanged;
        }

        private void OnLicenseChanged()
        {
            if (_server.Configuration.Monitoring.Snmp.Enabled == false)
                return;

            _locker.Wait();

            try
            {
                var snmpEngine = _snmpEngine;
                if (snmpEngine == null) // precaution
                    return;

                var activate = _server.ServerStore.LicenseManager.CanUseSnmpMonitoring(withNotification: true);
                if (activate)
                {
                    if (snmpEngine.Active == false)
                        snmpEngine.Start();
                }
                else
                {
                    snmpEngine.Stop();
                }
            }
            catch (ObjectDisposedException)
            {
                // ignore
                // we are shutting down the server
            }
            finally
            {
                _locker.Release();
            }
        }

        public void Execute()
        {
            if (_server.Configuration.Monitoring.Snmp.Enabled == false)
                return;

            _locker.Wait();

            try
            {
                _objectStore = CreateStore(_server);

                _snmpEngine = CreateSnmpEngine(_server, _objectStore);

                var activate = _server.ServerStore.LicenseManager.CanUseSnmpMonitoring(withNotification: true);
                if (activate)
                {
                    if (_snmpEngine.Active)
                        throw new InvalidOperationException("Cannot start SNMP Engine because it is already activated. Should not happen!");

                    _snmpEngine.Start();
                }

                _server.ServerStore.DatabasesLandlord.OnDatabaseLoaded += AddDatabaseIfNecessary;
            }
            finally
            {
                _locker.Release();
            }

            AsyncHelpers.RunSync(AddDatabases);
        }

        public ISnmpData GetData(string oid)
        {
            if (oid == null)
                throw new ArgumentNullException(nameof(oid));

            var scalarObject = _objectStore.GetObject(new ObjectIdentifier(oid));

            return scalarObject?.Data;
        }

        private void AddDatabaseIfNecessary(string databaseName)
        {
            if (string.IsNullOrWhiteSpace(databaseName))
                return;

            if (_loadedDatabases.ContainsKey(databaseName))
                return;

            Task.Factory.StartNew(async () =>
            {
                await _locker.WaitAsync();

                try
                {
                    if (_loadedDatabases.ContainsKey(databaseName))
                        return;

                    using (_server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    {
                        context.OpenReadTransaction();

                        var mapping = GetMapping(_server.ServerStore, context);
                        if (mapping.ContainsKey(databaseName) == false)
                        {
                            context.CloseTransaction();

                            var result = await _server.ServerStore.SendToLeaderAsync(new UpdateSnmpDatabasesMappingCommand(new List<string> { databaseName }, RaftIdGenerator.NewId()));
                            await _server.ServerStore.Cluster.WaitForIndexNotification(result.Index);

                            context.OpenReadTransaction();

                            mapping = GetMapping(_server.ServerStore, context);
                        }

                        LoadDatabase(databaseName, mapping[databaseName]);
                    }
                }
                catch (Exception e)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($"Failed to update the SNMP mapping for database: {databaseName}", e);
                }
                finally
                {
                    _locker.Release();
                }
            });
        }

        private static SnmpEngine CreateSnmpEngine(RavenServer server, ObjectStore objectStore)
        {
            (HashSet<SnmpVersion> versions, string handlerVersion) = GetVersions(server);
            var membershipProvider = CreateMembershipProvider(server, versions);

            var handlers = new[]
            {
                new HandlerMapping(handlerVersion, "GET", new GetMessageHandler()),
                new HandlerMapping(handlerVersion, "GETNEXT", new GetNextMessageHandler()),
                new HandlerMapping(handlerVersion, "GETBULK", new GetBulkMessageHandler())
            };

            var messageHandlerFactory = new MessageHandlerFactory(handlers);

            var factory = new SnmpApplicationFactory(new SnmpLogger(Logger), objectStore, membershipProvider, messageHandlerFactory);

            var listener = new Listener();

            if (versions.Contains(SnmpVersion.V3))
            {
                var authenticationPassword = server.Configuration.Monitoring.Snmp.AuthenticationPassword ?? server.Configuration.Monitoring.Snmp.Community;

                IAuthenticationProvider authenticationProvider;
                switch (server.Configuration.Monitoring.Snmp.AuthenticationProtocol)
                {
                    case SnmpAuthenticationProtocol.SHA1:
                        authenticationProvider = new SHA1AuthenticationProvider(new OctetString(authenticationPassword));
                        break;

                    case SnmpAuthenticationProtocol.MD5:
                        authenticationProvider = new MD5AuthenticationProvider(new OctetString(authenticationPassword));
                        break;

                    default:
                        throw new InvalidOperationException($"Unknown authentication protocol '{server.Configuration.Monitoring.Snmp.AuthenticationProtocol}'.");
                }

                var privacyPassword = server.Configuration.Monitoring.Snmp.PrivacyPassword;

                IPrivacyProvider privacyProvider;
                switch (server.Configuration.Monitoring.Snmp.PrivacyProtocol)
                {
                    case SnmpPrivacyProtocol.None:
                        privacyProvider = new DefaultPrivacyProvider(authenticationProvider);
                        break;

                    case SnmpPrivacyProtocol.DES:
                        privacyProvider = new BouncyCastleDESPrivacyProvider(new OctetString(privacyPassword), authenticationProvider);
                        break;

                    case SnmpPrivacyProtocol.AES:
                        privacyProvider = new BouncyCastleAESPrivacyProvider(new OctetString(privacyPassword), authenticationProvider);
                        break;

                    default:
                        throw new InvalidOperationException($"Unknown privacy protocol '{server.Configuration.Monitoring.Snmp.AuthenticationProtocol}'.");
                }

                listener.Users.Add(new OctetString(server.Configuration.Monitoring.Snmp.AuthenticationUser), privacyProvider);
            }

            int engineBoots;
            using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var tree = tx.InnerTransaction.CreateTree(nameof(SnmpWatcher));
                engineBoots = (int)tree.Increment("EngineBoots", 1);

                tx.Commit();
            }

            var engineGroup = new EngineGroup(engineBoots, GetIsInTime(server.Configuration.Monitoring))
            {
                EngineId = new OctetString(server.ServerStore.GetServerId().ToString("N"))
            };

            var engine = new SnmpEngine(factory, listener, engineGroup);
            engine.Listener.AddBinding(new IPEndPoint(IPAddress.Any, server.Configuration.Monitoring.Snmp.Port));
            engine.Listener.ExceptionRaised += (sender, e) =>
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("SNMP error: " + e.Exception.Message, e.Exception);
            };

            return engine;
        }

        private static Func<int[], int, int, bool> GetIsInTime(MonitoringConfiguration monitoringConfiguration)
        {
            return monitoringConfiguration.Snmp.DisableTimeWindowChecks
                ? (currentTimeData, pastReboots, pastTime) => true
                : (Func<int[], int, int, bool>)null;
        }

        private static (HashSet<SnmpVersion> Versions, string HandlerVersion) GetVersions(RavenServer server)
        {
            var length = server.Configuration.Monitoring.Snmp.SupportedVersions.Length;
            if (length <= 0)
                throw new InvalidOperationException($"There are no SNMP versions configured. Please set at least one in via '{RavenConfiguration.GetKey(x => x.Monitoring.Snmp.SupportedVersions)}' configuration option.");

            var protocols = new HashSet<string>();
            var versions = new HashSet<SnmpVersion>();
            foreach (string version in server.Configuration.Monitoring.Snmp.SupportedVersions)
            {
                if (Enum.TryParse(version, ignoreCase: true, out SnmpVersion v) == false)
                    throw new InvalidOperationException($"Could not recognize '{version}' as a valid SNMP version.");

                versions.Add(v);

                switch (v)
                {
                    case SnmpVersion.V2C:
                        protocols.Add("V2");
                        break;

                    case SnmpVersion.V3:
                        protocols.Add("V3");
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            return (versions, string.Join(",", protocols));
        }

        private static ComposedMembershipProvider CreateMembershipProvider(RavenServer server, HashSet<SnmpVersion> versions)
        {
            var providers = new List<IMembershipProvider>();
            foreach (var version in versions)
            {
                switch (version)
                {
                    case SnmpVersion.V2C:
                        providers.Add(new Version2MembershipProvider(
                            new OctetString(server.Configuration.Monitoring.Snmp.Community),
                            new OctetString(server.Configuration.Monitoring.Snmp.Community)));
                        break;

                    case SnmpVersion.V3:
                        providers.Add(new Version3MembershipProvider());
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            return new ComposedMembershipProvider(providers.ToArray());
        }

        private static ObjectStore CreateStore(RavenServer server)
        {
            var store = new ObjectStore();

            store.Add(new ServerUrl(server.Configuration));
            store.Add(new ServerPublicUrl(server.Configuration));
            store.Add(new ServerTcpUrl(server.Configuration));
            store.Add(new ServerPublicTcpUrl(server.Configuration));

            store.Add(new ServerVersion());
            store.Add(new ServerFullVersion());

            store.Add(new ServerUpTime(server.Statistics));
            store.Add(new ServerUpTimeGlobal(server.Statistics));

            store.Add(new ServerPid());

            store.Add(new ServerConcurrentRequests(server.Metrics));
            store.Add(new ServerTotalRequests(server.Metrics));
            store.Add(new ServerRequestsPerSecond(server.Metrics));

            store.Add(new ProcessCpu(server.MetricCacher, server.CpuUsageCalculator));
            store.Add(new MachineCpu(server.MetricCacher, server.CpuUsageCalculator));
            store.Add(new IoWait(server.MetricCacher, server.CpuUsageCalculator));

            store.Add(new CpuCreditsBase(server.CpuCreditsBalance));
            store.Add(new CpuCreditsMax(server.CpuCreditsBalance));
            store.Add(new CpuCreditsRemaining(server.CpuCreditsBalance));
            store.Add(new CpuCreditsCurrentConsumption(server.CpuCreditsBalance));
            store.Add(new CpuCreditsBackgroundTasksAlertRaised(server.CpuCreditsBalance));
            store.Add(new CpuCreditsFailoverAlertRaised(server.CpuCreditsBalance));
            store.Add(new CpuCreditsAlertRaised(server.CpuCreditsBalance));

            store.Add(new ServerTotalMemory(server.MetricCacher));
            store.Add(new ServerLowMemoryFlag());
            store.Add(new ServerTotalSwapSize(server.MetricCacher));
            store.Add(new ServerTotalSwapUsage(server.MetricCacher));
            store.Add(new ServerWorkingSetSwapUsage(server.MetricCacher));
            store.Add(new ServerDirtyMemory());
            store.Add(new ServerManagedMemory());
            store.Add(new ServerUnmanagedMemory());
            store.Add(new ServerEncryptionBuffersMemoryInUse());
            store.Add(new ServerEncryptionBuffersMemoryInPool());

            store.Add(new ServerLastRequestTime(server.Statistics));
            store.Add(new ServerLastAuthorizedNonClusterAdminRequestTime(server.Statistics));

            store.Add(new DatabaseLoadedCount(server.ServerStore.DatabasesLandlord));
            store.Add(new DatabaseTotalCount(server.ServerStore));
            store.Add(new DatabaseOldestBackup(server.ServerStore));
            store.Add(new DatabaseDisabledCount(server.ServerStore));
            store.Add(new DatabaseEncryptedCount(server.ServerStore));
            store.Add(new DatabaseNodeCount(server.ServerStore));

            store.Add(new ClusterNodeState(server.ServerStore));
            store.Add(new ClusterNodeTag(server.ServerStore));

            store.Add(new ClusterId(server.ServerStore));
            store.Add(new ClusterIndex(server.ServerStore));
            store.Add(new ClusterTerm(server.ServerStore));

            store.Add(new ServerLicenseType(server.ServerStore));
            store.Add(new ServerLicenseExpiration(server.ServerStore));
            store.Add(new ServerLicenseExpirationLeft(server.ServerStore));
            store.Add(new ServerLicenseUtilizedCpuCores(server.ServerStore));
            store.Add(new ServerLicenseMaxCpuCores(server.ServerStore));

            store.Add(new ServerStorageUsedSize(server.ServerStore));
            store.Add(new ServerStorageTotalSize(server.ServerStore));
            store.Add(new ServerStorageDiskRemainingSpace(server.ServerStore));
            store.Add(new ServerStorageDiskRemainingSpacePercentage(server.ServerStore));

            store.Add(new ServerCertificateExpiration(server.ServerStore));
            store.Add(new ServerCertificateExpirationLeft(server.ServerStore));
            store.Add(new WellKnownAdminCertificates(server.ServerStore));

            store.Add(new MachineProcessorCount());
            store.Add(new MachineAssignedProcessorCount());

            store.Add(new ServerBackupsCurrent(server.ServerStore));
            store.Add(new ServerBackupsMax(server.ServerStore));

            store.Add(new ThreadPoolAvailableWorkerThreads());
            store.Add(new ThreadPoolAvailableCompletionPortThreads());

            store.Add(new TcpActiveConnections());

            return store;
        }

        private async Task AddDatabases()
        {
            await _locker.WaitAsync();

            List<string> databases = null;
            List<string> missingDatabases = null;

            try
            {
                using (_server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    context.OpenReadTransaction();

                    databases = _server
                        .ServerStore
                        .Cluster
                        .ItemKeysStartingWith(context, Constants.Documents.Prefix, 0, int.MaxValue)
                        .Select(x => x.Substring(Constants.Documents.Prefix.Length))
                        .ToList();

                    if (databases.Count == 0)
                        return;

                    var mapping = GetMapping(_server.ServerStore, context);

                    missingDatabases = new List<string>();
                    foreach (var database in databases)
                    {
                        if (mapping.ContainsKey(database) == false)
                            missingDatabases.Add(database);
                    }

                    if (missingDatabases.Count > 0)
                    {
                        context.CloseTransaction();

                        try
                        {
                            var result = await _server.ServerStore.SendToLeaderAsync(new UpdateSnmpDatabasesMappingCommand(missingDatabases, RaftIdGenerator.NewId()));
                            await _server.ServerStore.Cluster.WaitForIndexNotification(result.Index);
                        }
                        catch (Exception e) when (e is TimeoutException || e is OperationCanceledException)
                        {
                            // we will update it in the OnDatabaseLoaded event
                            return;
                        }

                        context.OpenReadTransaction();

                        mapping = GetMapping(_server.ServerStore, context);
                    }

                    foreach (var database in databases)
                        LoadDatabase(database, mapping[database]);
                }
            }
            catch (Exception e)
            {
                var msg = "Failed to update the SNMP mapping";
                if (databases?.Count > 0)
                    msg += $" databases to update: {string.Join(", ", databases)}";
                if (missingDatabases?.Count > 0)
                    msg += $" missing databases: {string.Join(", ", missingDatabases)}";

                if (Logger.IsOperationsEnabled)
                    Logger.Operations(msg, e);
            }
            finally
            {
                _locker.Release();
            }
        }

        private void LoadDatabase(string databaseName, long databaseIndex)
        {
            if (_loadedDatabases.ContainsKey(databaseName))
                return;

            _loadedDatabases[databaseName] = new SnmpDatabase(_server.ServerStore.DatabasesLandlord, _objectStore, databaseName, (int)databaseIndex);
        }

        internal static Dictionary<string, long> GetMapping(ServerStore serverStore, TransactionOperationContext context)
        {
            var json = serverStore.Cluster.Read(context, Constants.Monitoring.Snmp.DatabasesMappingKey);

            var result = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
            if (json == null)
                return result;

            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            for (int i = 0; i < json.Count; i++)
            {
                json.GetPropertyByIndex(i, ref propertyDetails);

                result[propertyDetails.Name] = (long)propertyDetails.Value;
            }

            return result;
        }

        private class SnmpLogger : ILogger
        {
            private readonly Logger _logger;

            public SnmpLogger(Logger logger)
            {
                _logger = logger;
            }

            public void Log(ISnmpContext context)
            {
#if DEBUG
                if (_logger.IsInfoEnabled)
                    return;

                var builder = new StringBuilder();
                builder.AppendLine("SNMP:");
                var requestedOids = context.Request.Scope.Pdu.Variables.Select(x => x.Id);
                foreach (var oid in requestedOids)
                {
                    if (context.Response == null)
                    {
                        builder.AppendLine(string.Format("OID: {0}. Response: null", oid));
                        continue;
                    }

                    var responseData = context.Response.Scope.Pdu.Variables
                        .Where(x => x.Id == oid)
                        .Select(x => x.Data)
                        .FirstOrDefault();

                    builder.AppendLine(string.Format("OID: {0}. Response: {1}", oid, responseData?.ToString()));
                }

                _logger.Info(builder.ToString());
#endif
            }
        }
    }
}
