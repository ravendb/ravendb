using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.OAuth;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Implementation;
using Raven.Client.Counters.Changes;
using Raven.Client.Counters.Replication;
using Raven.Client.Util;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Client.Counters
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
			Convention = new Convention();
			Credentials = new OperationCredentials(null, CredentialCache.DefaultNetworkCredentials);
			Advanced = new CounterStoreAdvancedOperations(this);
			Admin = new CounterStoreAdminOperations(this);
			batch = new Lazy<BatchOperationsStore>(() => new BatchOperationsStore(this));
			isInitialized = false;
		}

		public void Initialize(bool ensureDefaultCounterExists = false)
		{
			if(isInitialized)
				throw new InvalidOperationException("CounterStore already initialized.");

			isInitialized = true;
			InitializeSecurity();

			if (ensureDefaultCounterExists && !string.IsNullOrWhiteSpace(Name))
			{
				if (String.IsNullOrWhiteSpace(Name))
					throw new InvalidOperationException("DefaultCounterStorageName is null or empty and ensureDefaultCounterExists = true --> cannot create default counter storage with empty name");

				Admin.CreateCounterStorageAsync(new CounterStorageDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\" + Name}
					},
				}, Name).ConfigureAwait(false).GetAwaiter().GetResult();
			}			

			replicationInformer = new CounterReplicationInformer(JsonRequestFactory, this); // make sure it is initialized
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

			var tenantUrl = Url + "/cs/" + counterStorage;

			using (NoSynchronizationContext.Scope())
			{
				var client = new CountersChangesClient(tenantUrl,
					Credentials.ApiKey,
					Credentials.Credentials,
					JsonRequestFactory,
					Convention,
					() => counterStorageChanges.Remove(counterStorage));

				return client;
			}
		}

		public event EventHandler AfterDispose;

		public bool WasDisposed { get; private set; }

		internal void AssertInitialized()
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

		public string Name { get; set; }

		public Convention Convention { get; set; }

		internal JsonSerializer JsonSerializer { get; set; }

		public CounterStoreAdvancedOperations Advanced { get; private set; }

		public CounterStoreAdminOperations Admin { get; private set; }
		
		protected HttpJsonRequest CreateHttpJsonRequest(string requestUriString, HttpMethod httpMethod, bool disableRequestCompression = false, bool disableAuthentication = false)
		{
			return JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, requestUriString, httpMethod, Credentials, Convention.ShouldCacheRequest)
			{
				DisableRequestCompression = disableRequestCompression,
				DisableAuthentication = disableAuthentication
			});
		}

		public CounterReplicationInformer ReplicationInformer
		{
			get
			{
				return replicationInformer ?? (replicationInformer = new CounterReplicationInformer(JsonRequestFactory, this));
			}
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
	}
}