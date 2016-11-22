using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using Raven.NewClient.Abstractions.Connection;
using Raven.NewClient.Abstractions.Counters;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Connection.Implementation;
using Raven.NewClient.Client.Counters.Changes;
using Raven.NewClient.Client.Counters.Replication;
using Raven.NewClient.Client.Extensions;
using Raven.NewClient.Client.Util;
using Newtonsoft.Json;

namespace Raven.NewClient.Client.Counters
{
    /// <summary>
    /// Implements client-side counters functionality
    /// </summary>
    public partial class CounterStore : ICounterStore
    {
        private readonly AtomicDictionary<ICountersChanges> counterStorageChanges = new AtomicDictionary<ICountersChanges>(StringComparer.OrdinalIgnoreCase);
        private CounterReplicationInformer replicationInformer;
        private bool isInitialized;

        public CounterStore()
        {
            JsonSerializer = JsonExtensions.CreateDefaultJsonSerializer();
            JsonRequestFactory = new HttpJsonRequestFactory(Constants.NumberOfCachedRequests);
            CountersConvention = new CountersConvention();
            Credentials = new OperationCredentials(null, CredentialCache.DefaultNetworkCredentials);
            Advanced = new CounterStoreAdvancedOperations(this);
            Admin = new CounterStoreAdminOperations(this);
            batch = new Lazy<BatchOperationsStore>(() => new BatchOperationsStore(this));
            isInitialized = false;
        }

        public void Initialize(bool ensureDefaultCounterExists = false)
        {
            if(isInitialized)
                throw new InvalidOperationException($"CounterStore already initialized. (name = {Name})");

            isInitialized = true;
            SecurityExtensions.InitializeSecurity(CountersConvention, JsonRequestFactory, Url, Credentials.Credentials);

            if (ensureDefaultCounterExists)
            {
                if (string.IsNullOrWhiteSpace(Name))
                    throw new InvalidOperationException("Name is null or empty and ensureDefaultCounterExists = true --> cannot create default counter storage with empty name");

                var existingCounterStorageNames = AsyncHelpers.RunSync(() => Admin.GetCounterStoragesNamesAsync());
                if (!existingCounterStorageNames.Contains(Name, StringComparer.OrdinalIgnoreCase)) 
                {
                    //this statement will essentially overwrite the counter storage, therefore it should not be called if the storage is already there
                    Admin.CreateCounterStorageAsync(new CounterStorageDocument
                    {
                        Id = Constants.Counter.Prefix + Name,
                        Settings = new Dictionary<string, string>
                        {
                            {"Raven/Counter/DataDir", @"~\Counters\" + Name}
                        },
                    }, Name).ConfigureAwait(false).GetAwaiter().GetResult();
                }
            }			

            replicationInformer = new CounterReplicationInformer(JsonRequestFactory, this, CountersConvention); // make sure it is initialized
        }

        public ICountersChanges Changes(string counterStorage = null)
        {
            AssertInitialized();

            if (string.IsNullOrWhiteSpace(counterStorage))
                counterStorage = Name;

            return counterStorageChanges.GetOrAdd(counterStorage, CreateCounterStorageChanges);
        }

        private ICountersChanges CreateCounterStorageChanges(string counterStorage)
        {
            if (string.IsNullOrEmpty(Url))
                throw new InvalidOperationException("Changes API requires usage of server/client");
            
            AssertInitialized();

            var tenantUrl = $"{Url}/cs/{counterStorage}";

            using (NoSynchronizationContext.Scope())
            {
                var client = new CountersChangesClient(tenantUrl,
                    Credentials.ApiKey,
                    Credentials.Credentials,
                    CountersConvention,
                    () => counterStorageChanges.Remove(counterStorage));

                return client;
            }
        }

        public event EventHandler AfterDispose = delegate { }; 

        public bool WasDisposed { get; private set; }

        internal void AssertInitialized()
        {
            if (!isInitialized)
                throw new InvalidOperationException("You cannot access the counters commands before initializing the counter store. " +
                                                    $"Did you forget calling Initialize()? (Counter store name = {Name})");
        }

        private readonly Lazy<BatchOperationsStore> batch;

        public BatchOperationsStore Batch => batch.Value;

        public OperationCredentials Credentials { get; set; }

        public HttpJsonRequestFactory JsonRequestFactory { get; set; }

        public string Url { get; set; }

        public string Name { get; set; }

        public CountersConvention CountersConvention { get; set; }

        internal JsonSerializer JsonSerializer { get; set; }

        public CounterStoreAdvancedOperations Advanced { get; private set; }

        public CounterStoreAdminOperations Admin { get; private set; }
        
        private HttpJsonRequest CreateHttpJsonRequest(string requestUriString, HttpMethod httpMethod, bool disableRequestCompression = false, bool disableAuthentication = false)
        {
            return JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, 
                requestUriString, 
                httpMethod, 
                Credentials, 
                CountersConvention)
            {
                DisableRequestCompression = disableRequestCompression,
                DisableAuthentication = disableAuthentication
            });
        }

        public CounterReplicationInformer ReplicationInformer => 
            replicationInformer ?? (replicationInformer = new CounterReplicationInformer(JsonRequestFactory, this, CountersConvention));

        public void Dispose()
        {
            if(batch.IsValueCreated)
                batch.Value.Dispose();
        }
    }
}
