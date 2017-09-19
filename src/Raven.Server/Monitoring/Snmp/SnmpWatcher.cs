using System.Linq;
using System.Net;
using System.Text;
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Messaging;
using Lextm.SharpSnmpLib.Pipeline;
using Raven.Server.Monitoring.Snmp.Objects.Server;
using Sparrow.Logging;

namespace Raven.Server.Monitoring.Snmp
{
    public class SnmpWatcher
    {
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

            // add databases here
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

            store.Add(new Objects.Server.ServerVersion());
            store.Add(new ServerFullVersion());

            store.Add(new ServerUpTime(server));
            store.Add(new ServerUpTimeGlobal(server));

            store.Add(new ServerPid());

            store.Add(new ServerConcurrentRequests(server));
            store.Add(new ServerTotalRequests(server));

            store.Add(new ServerCpu());
            store.Add(new ServerTotalMemory());

            store.Add(new ServerLastRequestTime(server));

            //store.Add(new DatabaseLoadedCount(serverOptions.DatabaseLandlord));
            //store.Add(new DatabaseTotalCount(serverOptions.SystemDatabase));

            return store;
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
                {
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
                }
#endif
            }
        }
    }
}
