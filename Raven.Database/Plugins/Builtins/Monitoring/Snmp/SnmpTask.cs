// -----------------------------------------------------------------------
//  <copyright file="SnmpTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Net;

using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Messaging;
using Lextm.SharpSnmpLib.Pipeline;
using Lextm.SharpSnmpLib.Security;

using Raven.Abstractions.Logging;
using Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server;
using Raven.Database.Server.Tenancy;
using Raven.Server;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp
{
	public class SnmpTask : IServerStartupTask
	{
		private readonly ILog log = LogManager.GetCurrentClassLogger();

		private DocumentDatabase systemDatabase;

		private DatabasesLandlord databaseLandlord;

		private SnmpEngine snmpEngine;

		public void Dispose()
		{
			if (snmpEngine != null)
				snmpEngine.Dispose();
		}

		public void Execute(RavenDbServer server)
		{
			// TODO [ppekrol] Check if SNMP is turned on
			// TODO [ppekrol] Validate license here

			systemDatabase = server.SystemDatabase;
			databaseLandlord = server.Options.DatabaseLandlord;

			snmpEngine = CreateSnmpEngine(server);
			snmpEngine.Start();
		}

		private SnmpEngine CreateSnmpEngine(RavenDbServer server)
		{
			var configuration = server.Configuration;

			var v2MembershipProvider = new Version2MembershipProvider(new OctetString(configuration.Monitoring.Snmp.Community), new OctetString(configuration.Monitoring.Snmp.Community));
			var v3MembershipProvider = new Version3MembershipProvider();
			var membershipProvider = new ComposedMembershipProvider(new IMembershipProvider[] { v2MembershipProvider, v3MembershipProvider });

			var handlers = new[]
			{
				new HandlerMapping("V2,V3", "GET", new GetMessageHandler()),
				new HandlerMapping("V2,V3", "GETNEXT", new GetNextMessageHandler()),
				new HandlerMapping("V2,V3", "GETBULK", new GetBulkMessageHandler()),
			};
			var messageHandlerFactory = new MessageHandlerFactory(handlers);

			var store = CreateStore(server);
			var factory = new SnmpApplicationFactory(new Logger(log), store, membershipProvider, messageHandlerFactory);

			var userRegistry = new UserRegistry();
			userRegistry.Add(new OctetString("neither"), DefaultPrivacyProvider.DefaultPair);
			//userRegistry.Add(new OctetString("authen"), new DefaultPrivacyProvider(new MD5AuthenticationProvider(new OctetString("authentication"))));
			//userRegistry.Add(new OctetString("privacy"), new DESPrivacyProvider(new OctetString("privacyphrase"), new MD5AuthenticationProvider(new OctetString("authentication"))));

			var listener = new Listener();

			var engineGroup = new EngineGroup();

			var engine = new SnmpEngine(factory, listener, engineGroup);
			engine.Listener.AddBinding(new IPEndPoint(IPAddress.Any, configuration.Monitoring.Snmp.Port));
			engine.Listener.ExceptionRaised += (sender, e) => log.ErrorException("SNMP error: " + e.Exception.Message, e.Exception);

			return engine;
		}

		private static ObjectStore CreateStore(RavenDbServer server)
		{
			var store = new ObjectStore();
			store.Add(new ServerUpTime(server.Options.RequestManager));
			store.Add(new ServerName(server.SystemDatabase.Configuration));
			store.Add(new ServerBuildVersion());
			store.Add(new ServerProductVersion());
			store.Add(new ServerPid());
			store.Add(new ServerTotalRequests(server.Options.RequestManager));
			store.Add(new ServerConcurrentRequests(server.Options.RequestManager));
			store.Add(new ServerCpu());
			store.Add(new ServerTotalMemory());

			return store;
		}

		private class Logger : ILogger
		{
			private readonly ILog log;

			public Logger(ILog log)
			{
				this.log = log;
			}

			public void Log(ISnmpContext context)
			{
				// TODO [ppekrol]
			}
		}
	}
}