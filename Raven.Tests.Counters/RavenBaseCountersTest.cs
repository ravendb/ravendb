using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Counters;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Raven.Tests.Helpers;

namespace Raven.Tests.Counters
{
	public class RavenBaseCountersTest : RavenTestBase
	{
		protected readonly Lazy<IDocumentStore> ravenStore;
		private readonly ConcurrentDictionary<string, int> storeCount;
		protected readonly string DefaultCounterStorageName = "ThisIsRelativelyUniqueCounterName";

		protected RavenBaseCountersTest()
		{
			foreach (var folder in Directory.EnumerateDirectories(Directory.GetCurrentDirectory(), "ThisIsRelativelyUniqueCounterName*"))
				IOExtensions.DeleteDirectory(folder);

			ravenStore = new Lazy<IDocumentStore>(() => NewRemoteDocumentStore(fiddler: true));
			DefaultCounterStorageName += Guid.NewGuid();
			storeCount = new ConcurrentDictionary<string, int>();
		}			

		protected void ConfigureServerForAuth(InMemoryRavenConfiguration serverConfiguration)
		{
			serverConfiguration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
			Authentication.EnableOnce();
		}

		protected void ConfigureApiKey(Database.DocumentDatabase database, string Name, string Secret, string resourceName = null, bool isAdmin = false)
		{
			var allowedResources = new List<ResourceAccess>
			{					
				new ResourceAccess {TenantId = resourceName, Admin = isAdmin}
			};

			if (isAdmin)
			{
				allowedResources.Add(new ResourceAccess { TenantId = Constants.SystemDatabase, Admin = true});
			}

			var apiKeyDefinition = RavenJObject.FromObject(new ApiKeyDefinition
			{
				Name = Name,
				Secret = Secret,
				Enabled = true,
				Databases = allowedResources
			});
			database.Documents.Put("Raven/ApiKeys/" + Name, null, apiKeyDefinition, new RavenJObject(), null);
		}


		protected ICounterStore NewRemoteCountersStore(string counterStorageName, bool createDefaultCounter = true,OperationCredentials credentials = null, IDocumentStore ravenStore = null)
		{
			ravenStore = ravenStore ?? this.ravenStore.Value;
			storeCount.AddOrUpdate(ravenStore.Identifier, id => 1, (id, val) => val++);		
	
			var counterStore = new CounterStore
			{
				Url = ravenStore.Url,
				Credentials = credentials ?? new OperationCredentials(null,CredentialCache.DefaultNetworkCredentials),
				Name = counterStorageName + storeCount[ravenStore.Identifier]
			};
			counterStore.Initialize(createDefaultCounter);
			return counterStore;
		}

		public override void Dispose()
		{
			if (ravenStore.IsValueCreated && ravenStore.Value != null) ravenStore.Value.Dispose();

			try
			{
				foreach(var server in servers)
					IOExtensions.DeleteDirectory(server.Configuration.DataDirectory); //for failover tests that have runInMemory = false
				IOExtensions.DeleteDirectory("Counters");
				base.Dispose();
			}
			catch (AggregateException) //TODO: do not forget to investigate where counter storage is not being disposed
			{
			}
		}

		protected async Task<bool> WaitForReplicationBetween(ICounterStore source, ICounterStore destination, string groupName, string counterName, int timeoutInSec = 3)
		{
			var waitStartingTime = DateTime.Now;
			var hasReplicated = false;

			if (Debugger.IsAttached)
				timeoutInSec = 60 * 60; //1 hour timeout if debugging

			while (true)
			{
				if ((DateTime.Now - waitStartingTime).TotalSeconds > timeoutInSec)
					break;
				try
				{
					var sourceValue = await source.GetOverallTotalAsync(groupName, counterName);
					var targetValue = await destination.GetOverallTotalAsync(groupName, counterName);
					if (sourceValue == targetValue)
					{
						hasReplicated = true;
						break;
					}
				}
				catch (InvalidOperationException e)
				{
					var exception = e.InnerException as ErrorResponseException;
					if (exception != null && exception.StatusCode != HttpStatusCode.NotFound)
						throw;
				}
				Thread.Sleep(50);
			}

			return hasReplicated;
		}

		protected static async Task<object> SetupReplicationAsync(ICounterStore source, params ICounterStore[] destinations)
		{
			var replicationDocument = new CountersReplicationDocument();
			foreach (var destStore in destinations)
			{
				replicationDocument.Destinations.Add(new CounterReplicationDestination
				{
					CounterStorageName = destStore.Name,
					ServerUrl = destStore.Url
				});
			}

			await source.SaveReplicationsAsync(replicationDocument);
			return null;
		}
	}
}
