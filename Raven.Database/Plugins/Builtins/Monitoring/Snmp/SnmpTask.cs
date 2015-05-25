// -----------------------------------------------------------------------
//  <copyright file="SnmpTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Messaging;
using Lextm.SharpSnmpLib.Pipeline;

using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Commercial;
using Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Database;
using Raven.Database.Plugins.Builtins.Monitoring.Snmp.Objects.Server;
using Raven.Database.Server.Tenancy;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Server;

namespace Raven.Database.Plugins.Builtins.Monitoring.Snmp
{
	public class SnmpTask : IServerStartupTask
	{
		private readonly ConcurrentDictionary<string, SnmpDatabase> loadedDatabases = new ConcurrentDictionary<string, SnmpDatabase>(StringComparer.OrdinalIgnoreCase);

		private readonly object locker = new object();

		private readonly ILog log = LogManager.GetCurrentClassLogger();

		private DocumentDatabase systemDatabase;

		private DatabasesLandlord databaseLandlord;

		private SnmpEngine snmpEngine;

		private ObjectStore objectStore;

		public void Dispose()
		{
			if (snmpEngine != null)
				snmpEngine.Dispose();
		}

		public void Execute(RavenDbServer server)
		{
			if (server.Configuration.Monitoring.Snmp.Enabled == false)
				return;

			if (IsLicenseValid() == false)
				throw new InvalidOperationException("Your license does not allow you to use SNMP monitoring.");

			systemDatabase = server.SystemDatabase;
			databaseLandlord = server.Options.DatabaseLandlord;

			objectStore = CreateStore(server);

			snmpEngine = CreateSnmpEngine(server, objectStore);
			snmpEngine.Start();

			databaseLandlord.OnDatabaseLoaded += AddDatabaseIfNecessary;
			AddDatabases();
		}

		private static bool IsLicenseValid()
		{
			DevelopmentHelper.TimeBomb();
			return true;

			string monitoring;
			if (ValidateLicense.CurrentLicense.Attributes.TryGetValue("monitoring", out monitoring))
			{
				bool active;
				if (bool.TryParse(monitoring, out active))
					return true;
			}

			return false;
		}

		private void AddDatabaseIfNecessary(string databaseName)
		{
			if (string.IsNullOrEmpty(databaseName))
				return;

			if (string.Equals(databaseName, Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase))
				return;

			Task.Factory.StartNew(() =>
			{
				lock (locker)
				{
					AddDatabase(objectStore, databaseName);
				}
			});
		}

		private SnmpEngine CreateSnmpEngine(RavenDbServer server, ObjectStore store)
		{
			var configuration = server.Configuration;

			var v2MembershipProvider = new Version2MembershipProvider(new OctetString(configuration.Monitoring.Snmp.Community), new OctetString(configuration.Monitoring.Snmp.Community));
			var v3MembershipProvider = new Version3MembershipProvider();
			var membershipProvider = new ComposedMembershipProvider(new IMembershipProvider[] { v2MembershipProvider, v3MembershipProvider });

			var handlers = new[]
			{
				new HandlerMapping("V2,V3", "GET", new GetMessageHandler()),
				new HandlerMapping("V2,V3", "GETNEXT", new GetNextMessageHandler()),
				new HandlerMapping("V2,V3", "GETBULK", new GetBulkMessageHandler())
			};

			var messageHandlerFactory = new MessageHandlerFactory(handlers);

			var factory = new SnmpApplicationFactory(new Logger(log), store, membershipProvider, messageHandlerFactory);

			var listener = new Listener();
			var engineGroup = new EngineGroup();

			var engine = new SnmpEngine(factory, listener, engineGroup);
			engine.Listener.AddBinding(new IPEndPoint(IPAddress.Any, configuration.Monitoring.Snmp.Port));
			engine.Listener.ExceptionRaised += (sender, e) => log.ErrorException("SNMP error: " + e.Exception.Message, e.Exception);

			return engine;
		}

		private ObjectStore CreateStore(RavenDbServer server)
		{
			var store = new ObjectStore();
			store.Add(new ServerUpTime(server.Options.RequestManager));
			store.Add(new ServerUpTimeGlobal(server.Options.RequestManager));
			store.Add(new ServerName(server.SystemDatabase.Configuration));
			store.Add(new ServerBuildVersion());
			store.Add(new ServerProductVersion());
			store.Add(new ServerPid());
			store.Add(new ServerTotalRequests(server.Options.RequestManager));
			store.Add(new ServerConcurrentRequests(server.Options.RequestManager));
			store.Add(new ServerCpu());
			store.Add(new ServerTotalMemory());
			store.Add(new ServerUrl(server.SystemDatabase.Configuration));
			store.Add(new ServerIndexingErrors(server.Options.DatabaseLandlord));
			store.Add(new ServerLastRequestTime(server.Options.RequestManager));

			store.Add(new DatabaseLoadedCount(server.Options.DatabaseLandlord));
			store.Add(new DatabaseTotalCount(server.SystemDatabase));

			return store;
		}

		private void AddDatabases()
		{
			var nextStart = 0;
			var databases = systemDatabase
				.Documents
				.GetDocumentsWithIdStartingWith(Constants.Database.Prefix, null, null, 0, int.MaxValue, systemDatabase.WorkContext.CancellationToken, ref nextStart);

			var databaseNames = new List<string> { Constants.SystemDatabase };

			foreach (RavenJObject database in databases)
			{
				var id = database[Constants.Metadata].Value<string>("@id");
				if (id.StartsWith(Constants.Database.Prefix))
					id = id.Substring(Constants.Database.Prefix.Length);

				databaseNames.Add(id);
			}

			databaseNames.ForEach(databaseName => AddDatabase(objectStore, databaseName));
		}

		private void AddDatabase(ObjectStore store, string databaseName)
		{
			var index = (int)GetOrAddDatabaseIndex(databaseName);

			loadedDatabases.GetOrAdd(databaseName, new SnmpDatabase(databaseLandlord, store, databaseName, index));
		}

		private long GetOrAddDatabaseIndex(string databaseName)
		{
			if (databaseName == null || string.Equals(databaseName, Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase))
				return 1;

			var mappingDocument = systemDatabase.Documents.Get(Constants.Monitoring.Snmp.DatabaseMappingDocumentKey, null) ?? new JsonDocument();

			RavenJToken value;
			if (mappingDocument.DataAsJson.TryGetValue(databaseName, out value))
				return value.Value<int>();

			var index = 0L;
			systemDatabase.TransactionalStorage.Batch(actions =>
			{
				mappingDocument.DataAsJson[databaseName] = index = actions.General.GetNextIdentityValue(Constants.Monitoring.Snmp.DatabaseMappingDocumentKey) + 1;
				systemDatabase.Documents.Put(Constants.Monitoring.Snmp.DatabaseMappingDocumentKey, null, mappingDocument.DataAsJson, mappingDocument.Metadata, null);
			});

			return index;
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
				if (log.IsDebugEnabled == false)
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

				log.Debug(builder.ToString);
			}
		}
	}
}