using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.OAuth;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Implementation;
using Raven.Client.Connection.Profiling;
using Raven.Client.Counters.Actions;
using Raven.Client.Counters.Changes;
using Raven.Client.Counters.Replication;
using Raven.Client.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Counters
{
	/// <summary>
	/// implements administration level counters functionality
	/// </summary>
	public class CounterStore : ICounterStore
	{
		private readonly AtomicDictionary<ICountersChanges> counterStorageChanges = new AtomicDictionary<ICountersChanges>(StringComparer.OrdinalIgnoreCase);
		private ICountersReplicationInformer replicationInformer;
		private bool isInitialized;

		public CounterStore()
		{
			JsonSerializer = JsonExtensions.CreateDefaultJsonSerializer();
			JsonRequestFactory = new HttpJsonRequestFactory(Constants.NumberOfCachedRequests);
			Convention = new Convention();
			Credentials = new OperationCredentials(null, CredentialCache.DefaultNetworkCredentials);
			Advanced = new CounterStoreAdvancedOperations(this);
			batch = new Lazy<BatchOperationsStore>(() => new BatchOperationsStore(this));
			isInitialized = false;
		}

		public void Initialize(bool ensureDefaultCounterExists = false)
		{
			if(isInitialized)
				throw new InvalidOperationException("CounterStore already initialized.");

			isInitialized = true;
			InitializeSecurity();

			if (ensureDefaultCounterExists && !string.IsNullOrWhiteSpace(DefaultCounterStorageName))
			{
				if (String.IsNullOrWhiteSpace(DefaultCounterStorageName))
					throw new InvalidOperationException("DefaultCounterStorageName is null or empty and ensureDefaultCounterExists = true --> cannot create default counter storage with empty name");

				CreateCounterStorageAsync(new CounterStorageDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\" + DefaultCounterStorageName}
					},
				}, DefaultCounterStorageName).ConfigureAwait(false).GetAwaiter().GetResult();
			}			

			replicationInformer = new CounterReplicationInformer(Convention, JsonRequestFactory); // make sure it is initialized
		}

		public ICountersChanges Changes(string counterStorage = null)
		{
			AssertInitialized();

			if (string.IsNullOrWhiteSpace(counterStorage))
				counterStorage = DefaultCounterStorageName;

			return counterStorageChanges.GetOrAdd(counterStorage, CreateCounterStorageChanges);
		}

		private ICountersChanges CreateCounterStorageChanges(string counterStorage)
		{
			if (string.IsNullOrEmpty(Url))
				throw new InvalidOperationException("Changes API requires usage of server/client");

			var tenantUrl = Url + "/cs/" + counterStorage;

			using (NoSynchronizationContext.Scope())
			{
				var client = new CountersChangesClient(tenantUrl,
					Credentials.ApiKey,
					Credentials.Credentials,
					JsonRequestFactory,
					Convention,
					() =>
					{
						counterStorageChanges.Remove(counterStorage);
					});

				return client;
			}
		}

		public event EventHandler AfterDispose;

		public bool WasDisposed { get; private set; }

		private void AssertInitialized()
		{
			if (!isInitialized)
				throw new InvalidOperationException("You cannot open a session or access the counters commands before initializing the counter store. Did you forget calling Initialize()?");
		}

		private readonly Lazy<BatchOperationsStore> batch;

		public BatchOperationsStore Batch
		{
			get { return batch.Value; }
		}

		public OperationCredentials Credentials { get; set; }

		public HttpJsonRequestFactory JsonRequestFactory { get; set; }

		public string Url { get; set; }

		public string DefaultCounterStorageName { get; set; }

		public Convention Convention { get; set; }

		public JsonSerializer JsonSerializer { get; set; }

		public CounterStoreAdvancedOperations Advanced { get; private set; }

		/// <summary>
		/// Create new counter storage on the server.
		/// </summary>
		/// <param name="counterStorageDocument">Settings for the counter storage. If null, default settings will be used, and the name specified in the client ctor will be used</param>
		/// <param name="counterStorageName">Override counter storage name specified in client ctor. If null, the name already specified will be used</param>
		public async Task CreateCounterStorageAsync(CounterStorageDocument counterStorageDocument, string counterStorageName, bool shouldUpateIfExists = false, CancellationToken token = default(CancellationToken))
		{
			if (counterStorageDocument == null)
				throw new ArgumentNullException("counterStorageDocument");

			var urlTemplate = "{0}/admin/cs/{1}";
			if (shouldUpateIfExists)
				urlTemplate += "?update=true";

			var requestUriString = String.Format(urlTemplate, Url, counterStorageName);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Put))
			{
				try
				{
					await request.WriteAsync(RavenJObject.FromObject(counterStorageDocument)).WithCancellation(token).ConfigureAwait(false);
				}
				catch (ErrorResponseException e)
				{
					if (e.StatusCode == HttpStatusCode.Conflict)
						throw new InvalidOperationException("Cannot create counter storage with the name '" + counterStorageName + "' because it already exists. Use CreateOrUpdateCounterStorageAsync in case you want to update an existing counter storage", e);

					throw;
				}					
			}
		}

		public async Task DeleteCounterStorageAsync(string counterStorageName, bool hardDelete = false, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/admin/cs/{1}?hard-delete={2}", Url, counterStorageName, hardDelete);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Delete))
			{
				try
				{
					await request.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
				}
				catch (ErrorResponseException e)
				{
					if (e.StatusCode == HttpStatusCode.NotFound)
						throw new InvalidOperationException(string.Format("Counter storage with specified name ({0}) doesn't exist", counterStorageName));
					throw;
				}
			}
		}

		public CountersClient NewCounterClient(string counterStorageName = null)
		{
			if (counterStorageName == null && String.IsNullOrWhiteSpace(DefaultCounterStorageName))
				throw new ArgumentNullException("counterStorageName", 
					@"counterStorageName is null and default counter storage name is empty - 
						this means no default counter exists.");
			return new CountersClient(this,counterStorageName ?? DefaultCounterStorageName);
		}

		public async Task<string[]> GetCounterStoragesNamesAsync(CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/cs/counterStorageNames", Url);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
				return response.ToObject<string[]>(JsonSerializer);
			}
		}

		protected HttpJsonRequest CreateHttpJsonRequest(string requestUriString, HttpMethod httpMethod, bool disableRequestCompression = false, bool disableAuthentication = false)
		{
			return JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, httpMethod, Credentials, Convention)
			{
				DisableRequestCompression = disableRequestCompression,
				DisableAuthentication = disableAuthentication
			});
		}

		public ProfilingInformation ProfilingInformation { get; private set; }
		
		public ICountersReplicationInformer ReplicationInformer
		{
			get { return replicationInformer ?? (replicationInformer = new CounterReplicationInformer(Convention, JsonRequestFactory)); }
		}


		private void InitializeSecurity()
		{
			if (Convention.HandleUnauthorizedResponseAsync != null)
				return; // already setup by the user

			if (string.IsNullOrEmpty(Credentials.ApiKey) == false)
				Credentials = null;

			var basicAuthenticator = new BasicAuthenticator(JsonRequestFactory.EnableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers);
			var securedAuthenticator = new SecuredAuthenticator();

			JsonRequestFactory.ConfigureRequest += basicAuthenticator.ConfigureRequest;
			JsonRequestFactory.ConfigureRequest += securedAuthenticator.ConfigureRequest;

			Convention.HandleForbiddenResponseAsync = (forbiddenResponse, credentials) =>
			{
				if (credentials.ApiKey == null)
				{
					AssertForbiddenCredentialSupportWindowsAuth(forbiddenResponse);
					return null;
				}

				return null;
			};

			Convention.HandleUnauthorizedResponseAsync = (unauthorizedResponse, credentials) =>
			{
				var oauthSource = unauthorizedResponse.Headers.GetFirstValue("OAuth-Source");

#if DEBUG && FIDDLER
                // Make sure to avoid a cross DNS security issue, when running with Fiddler
				if (string.IsNullOrEmpty(oauthSource) == false)
					oauthSource = oauthSource.Replace("localhost:", "localhost.fiddler:");
#endif

				// Legacy support
				if (string.IsNullOrEmpty(oauthSource) == false &&
					oauthSource.EndsWith("/OAuth/API-Key", StringComparison.CurrentCultureIgnoreCase) == false)
				{
					return basicAuthenticator.HandleOAuthResponseAsync(oauthSource, credentials.ApiKey);
				}

				if (credentials.ApiKey == null)
				{
					AssertUnauthorizedCredentialSupportWindowsAuth(unauthorizedResponse, credentials.Credentials);
					return null;
				}

				if (string.IsNullOrEmpty(oauthSource))
					oauthSource = Url + "/OAuth/API-Key";

				return securedAuthenticator.DoOAuthRequestAsync(Url, oauthSource, credentials.ApiKey);
			};

		}

		private void AssertForbiddenCredentialSupportWindowsAuth(HttpResponseMessage response)
		{
			if (Credentials == null)
				return;

			var requiredAuth = response.Headers.GetFirstValue("Raven-Required-Auth");
			if (requiredAuth == "Windows")
			{
				// we are trying to do windows auth, but we didn't get the windows auth headers
				throw new SecurityException(
					"Attempted to connect to a RavenDB Server that requires authentication using Windows credentials, but the specified server does not support Windows authentication." +
					Environment.NewLine +
					"If you are running inside IIS, make sure to enable Windows authentication.");
			}
		}

		private static void AssertUnauthorizedCredentialSupportWindowsAuth(HttpResponseMessage response, ICredentials credentials)
		{
			if (credentials == null)
				return;

			var authHeaders = response.Headers.WwwAuthenticate.FirstOrDefault();
			if (authHeaders == null ||
				(authHeaders.ToString().Contains("NTLM") == false && authHeaders.ToString().Contains("Negotiate") == false)
				)
			{
				// we are trying to do windows auth, but we didn't get the windows auth headers
				throw new SecurityException(
					"Attempted to connect to a RavenDB Server that requires authentication using Windows credentials," + Environment.NewLine
					+ " but either wrong credentials where entered or the specified server does not support Windows authentication." +
					Environment.NewLine +
					"If you are running inside IIS, make sure to enable Windows authentication.");
			}
		}

		public void Dispose()
		{
			if(batch.IsValueCreated)
				batch.Value.Dispose();

			
		}

		public class BatchOperationsStore : ICountersBatchOperation
		{
			private readonly ICounterStore parent;
			private readonly Lazy<CountersBatchOperation> defaultBatchOperation;
			private readonly ConcurrentDictionary<string, CountersBatchOperation> batchOperations;

			public BatchOperationsStore(ICounterStore parent)
			{				
				batchOperations = new ConcurrentDictionary<string, CountersBatchOperation>();
				this.parent = parent;
				if(string.IsNullOrWhiteSpace(parent.DefaultCounterStorageName) == false)
					defaultBatchOperation = new Lazy<CountersBatchOperation>(() => new CountersBatchOperation(parent, parent.DefaultCounterStorageName));

				OperationId = Guid.NewGuid();
			}

			public ICountersBatchOperation this[string storageName]
			{
				get { return GetOrCreateBatchOperation(storageName); }
			}

			private ICountersBatchOperation GetOrCreateBatchOperation(string storageName)
			{
				return batchOperations.GetOrAdd(storageName, arg => new CountersBatchOperation(parent, storageName));
			}

			public void Dispose()
			{
				batchOperations.Values
					.ForEach(operation => operation.Dispose());
				if (defaultBatchOperation != null && defaultBatchOperation.IsValueCreated)
					defaultBatchOperation.Value.Dispose();
			}

			public void ScheduleChange(string groupName, string counterName, long delta)
			{
				if (string.IsNullOrWhiteSpace(parent.DefaultCounterStorageName))
					throw new InvalidOperationException("Default counter storage name cannot be empty!");

				defaultBatchOperation.Value.ScheduleChange(groupName, counterName, delta);
			}

			public void ScheduleIncrement(string groupName, string counterName)
			{
				if (string.IsNullOrWhiteSpace(parent.DefaultCounterStorageName))
					throw new InvalidOperationException("Default counter storage name cannot be empty!");

				defaultBatchOperation.Value.ScheduleIncrement(groupName, counterName);
			}

			public void ScheduleDecrement(string groupName, string counterName)
			{
				if (string.IsNullOrWhiteSpace(parent.DefaultCounterStorageName))
					throw new InvalidOperationException("Default counter storage name cannot be empty!");

				defaultBatchOperation.Value.ScheduleDecrement(groupName, counterName);
			}

			public async Task FlushAsync()
			{
				if (string.IsNullOrWhiteSpace(parent.DefaultCounterStorageName))
					throw new InvalidOperationException("Default counter storage name cannot be empty!");

				await defaultBatchOperation.Value.FlushAsync();
			}

			public Guid OperationId { get; private set; }

			public CountersBatchOptions Options
			{
				get
				{
					if (string.IsNullOrWhiteSpace(parent.DefaultCounterStorageName))
						throw new InvalidOperationException("Default counter storage name cannot be empty!");
					return defaultBatchOperation.Value.Options;
				}
			}
		}

		public class CounterStoreAdvancedOperations
		{
			private readonly ICounterStore parent;

			internal CounterStoreAdvancedOperations(ICounterStore parent)
			{
				this.parent = parent;
			}

			public ICountersBatchOperation NewBatch(CountersBatchOptions options = null)
			{
				if (parent.DefaultCounterStorageName == null)
					throw new ArgumentException("Default Counter Storage isn't set!");

				return new CountersBatchOperation(parent, parent.DefaultCounterStorageName, options);
			}

			public ICountersBatchOperation NewBatch(string counterStorageName, CountersBatchOptions options = null)
			{
				if (string.IsNullOrWhiteSpace(counterStorageName))
					throw new ArgumentException("Counter Storage name isn't set!");

				return new CountersBatchOperation(parent, counterStorageName, options);
			}
		}
	}
}