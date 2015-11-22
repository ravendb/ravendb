using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Counters;
using Raven.Database.Config;
using Raven.Database.Counters;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Helpers;

namespace Raven.Tests.Counters
{
    public class RavenBaseCountersTest : RavenTestBase
    {
        protected readonly Lazy<RavenDbServer> RavenDbServer;
        private readonly ConcurrentDictionary<string, int> serverCount;
        private readonly List<IDisposable> disposables = new List<IDisposable>();
        protected readonly string DefaultCounterStorageName = "ThisIsRelativelyUniqueCounterName";

        protected RavenBaseCountersTest()
        {
            foreach (var folder in Directory.EnumerateDirectories(Directory.GetCurrentDirectory(), "ThisIsRelativelyUniqueCounterName*"))
                IOExtensions.DeleteDirectory(folder);

            RavenDbServer = new Lazy<RavenDbServer>(() => GetNewServer(requestedStorage: "voron"));
            DefaultCounterStorageName += Guid.NewGuid();
            serverCount = new ConcurrentDictionary<string, int>();
        }

        protected CounterStorage NewCounterStorage()
        {
            var newCounterStorage = new CounterStorage(String.Empty, DefaultCounterStorageName, new InMemoryRavenConfiguration
            {
                RunInMemory = true
            });

            disposables.Add(newCounterStorage);
            return newCounterStorage;
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


        protected ICounterStore NewRemoteCountersStore(string counterStorageName, bool createDefaultCounter = true,OperationCredentials credentials = null, RavenDbServer ravenServer = null)
        {
            ravenServer = ravenServer ?? this.RavenDbServer.Value;
            var serverUrl = ravenServer.SystemDatabase.ServerUrl;
            serverCount.AddOrUpdate(serverUrl, id => 1, (id, val) => val++);		
    
            var counterStore = new CounterStore
            {
                Url = GetServerUrl(true, serverUrl),
                Credentials = credentials ?? new OperationCredentials(null,CredentialCache.DefaultNetworkCredentials),
                Name = counterStorageName + serverCount[serverUrl]
            };
            counterStore.Initialize(createDefaultCounter);
            return counterStore;
        }

        public override void Dispose()
        {
            if (RavenDbServer.IsValueCreated && RavenDbServer.Value != null)
                RavenDbServer.Value.Dispose();

            try
            {
                foreach(var server in servers)
                    IOExtensions.DeleteDirectory(server.Configuration.DataDirectory); //for failover tests that have runInMemory = false
                IOExtensions.DeleteDirectory("Counters");

                disposables.ForEach(d => d.Dispose());
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
                    if (sourceValue.IsExists && targetValue.IsExists && sourceValue.Total == targetValue.Total)
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
