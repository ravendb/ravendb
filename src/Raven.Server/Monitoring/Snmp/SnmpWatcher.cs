using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Messaging;
using Lextm.SharpSnmpLib.Pipeline;
using Raven.Client;
using Raven.Server.Monitoring.Snmp.Objects.Documents;
using Raven.Server.Monitoring.Snmp.Objects.Server;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections.LockFree;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Monitoring.Snmp
{
    public class SnmpWatcher
    {
        private readonly ConcurrentDictionary<string, SnmpDatabase> _loadedDatabases = new ConcurrentDictionary<string, SnmpDatabase>(StringComparer.OrdinalIgnoreCase);

        private readonly object _locker = new object();

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RavenServer>(nameof(SnmpWatcher));

        private readonly RavenServer _server;

        private ObjectStore _objectStore;

        private SnmpEngine _snmpEngine;

        public SnmpWatcher(RavenServer server)
        {
            _server = server;
        }

        public void Execute()
        {
            if (_server.Configuration.Monitoring.Snmp.Enabled == false)
                return;

            // validate license here

            _objectStore = CreateStore(_server);

            _snmpEngine = CreateSnmpEngine(_server, _objectStore);
            _snmpEngine.Start();

            _server.ServerStore.DatabasesLandlord.OnDatabaseLoaded += AddDatabaseIfNecessary;
            AddDatabases();
        }

        private void AddDatabaseIfNecessary(string databaseName)
        {
            if (string.IsNullOrWhiteSpace(databaseName))
                return;

            Task.Factory.StartNew(() =>
            {
                lock (_locker)
                {
                    using (_server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var mapping = LoadMapping(context);

                        var index = GetOrAddDatabaseIndex(mapping, databaseName);
                        PersistMappingIfNecessary(mapping);

                        LoadDatabase(databaseName, index);
                    }
                }
            });
        }

        private static SnmpEngine CreateSnmpEngine(RavenServer server, ObjectStore objectStore)
        {
            var v2MembershipProvider = new Version2MembershipProvider(new OctetString(server.Configuration.Monitoring.Snmp.Community), new OctetString(server.Configuration.Monitoring.Snmp.Community));
            var v3MembershipProvider = new Version3MembershipProvider();
            var membershipProvider = new ComposedMembershipProvider(new IMembershipProvider[] { v2MembershipProvider, v3MembershipProvider });

            var handlers = new[]
            {
                new HandlerMapping("V2,V3", "GET", new GetMessageHandler()),
                new HandlerMapping("V2,V3", "GETNEXT", new GetNextMessageHandler()),
                new HandlerMapping("V2,V3", "GETBULK", new GetBulkMessageHandler())
            };

            var messageHandlerFactory = new MessageHandlerFactory(handlers);

            var factory = new SnmpApplicationFactory(new SnmpLogger(Logger), objectStore, membershipProvider, messageHandlerFactory);

            var listener = new Listener();
            var engineGroup = new EngineGroup();

            var engine = new SnmpEngine(factory, listener, engineGroup);
            engine.Listener.AddBinding(new IPEndPoint(IPAddress.Any, server.Configuration.Monitoring.Snmp.Port));
            engine.Listener.ExceptionRaised += (sender, e) =>
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("SNMP error: " + e.Exception.Message, e.Exception);
            };

            return engine;
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

            store.Add(new ServerCpu());
            store.Add(new ServerTotalMemory());

            store.Add(new ServerLastRequestTime(server.Statistics));

            store.Add(new DatabaseLoadedCount(server.ServerStore.DatabasesLandlord));
            store.Add(new DatabaseTotalCount(server.ServerStore));

            return store;
        }

        private void AddDatabases()
        {
            lock (_locker)
            {
                using (_server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var databases = _server
                        .ServerStore
                        .Cluster
                        .ItemKeysStartingWith(context, Constants.Documents.Prefix, 0, int.MaxValue)
                        .Select(x => x.Substring(Constants.Documents.Prefix.Length))
                        .ToList();

                    if (databases.Count == 0)
                        return;

                    var mapping = LoadMapping(context);

                    var databaseIndexes = AssignIndexes(databases, mapping);
                    PersistMappingIfNecessary(mapping);

                    foreach (var kvp in databaseIndexes)
                        LoadDatabase(kvp.DatabaseName, kvp.DatabaseIndex);
                }
            }
        }

        private void LoadDatabase(string databaseName, int databaseIndex)
        {
            _loadedDatabases.GetOrAdd(databaseName, _ => new SnmpDatabase(_server.ServerStore.DatabasesLandlord, _objectStore, databaseName, databaseIndex));
        }

        private List<(string DatabaseName, int DatabaseIndex)> AssignIndexes(IEnumerable<string> databases, BlittableJsonReaderObject mapping)
        {
            var results = new List<(string DatabaseName, int DatabaseIndex)>();
            foreach (var database in databases)
                results.Add((database, GetOrAddDatabaseIndex(mapping, database)));

            return results;
        }

        private void PersistMappingIfNecessary(BlittableJsonReaderObject mapping)
        {
            if (mapping.Modifications == null || mapping.Modifications.Properties.Count <= 0)
                return;

            using (_server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext writeContext))
            using (var tx = writeContext.OpenWriteTransaction())
            {
                mapping = writeContext.ReadObject(mapping, Constants.Monitoring.Snmp.MappingKey);
                _server.ServerStore.Cluster.PutLocalState(writeContext, Constants.Monitoring.Snmp.MappingKey, mapping);

                tx.Commit();
            }
        }

        private BlittableJsonReaderObject LoadMapping(TransactionOperationContext context)
        {
            return _server.ServerStore.Cluster.GetLocalState(context, Constants.Monitoring.Snmp.MappingKey)
                   ?? context.ReadObject(new DynamicJsonValue(), Constants.Monitoring.Snmp.MappingKey);
        }

        private int GetOrAddDatabaseIndex(BlittableJsonReaderObject mappingJson, string databaseName)
        {
            if (databaseName == null)
                throw new ArgumentNullException(nameof(databaseName));

            if (mappingJson.TryGet(databaseName, out int index))
                return index;

            if (mappingJson.Modifications == null)
                mappingJson.Modifications = new DynamicJsonValue();

            index = mappingJson.Count + mappingJson.Modifications.Properties.Count + 1;
            mappingJson.Modifications[databaseName] = index;

            return index;
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

                    builder.AppendLine(string.Format("OID: {0}. Response: {1}", oid, responseData != null ? responseData.ToString() : null));
                }

                _logger.Info(builder.ToString());
#endif
            }
        }
    }
}
