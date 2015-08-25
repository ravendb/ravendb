using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security;
using Raven.Abstractions.Connection;
using Raven.Abstractions.TimeSeries;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.OAuth;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Implementation;
using Raven.Client.Extensions;
using Raven.Client.TimeSeries.Changes;
using Raven.Client.TimeSeries.Replication;
using Raven.Client.Util;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Client.TimeSeries
{
	/// <summary>
	/// Implements client-side time series functionality
	/// </summary>
	public partial class TimeSeriesStore : ITimeSeriesStore
	{
		private readonly AtomicDictionary<ITimeSeriesChanges> timeSeriesChanges = new AtomicDictionary<ITimeSeriesChanges>(StringComparer.OrdinalIgnoreCase);
		private TimeSeriesReplicationInformer replicationInformer;
		private bool isInitialized;

		public TimeSeriesStore()
		{
			JsonSerializer = JsonExtensions.CreateDefaultJsonSerializer();
			JsonRequestFactory = new HttpJsonRequestFactory(Constants.NumberOfCachedRequests);
			TimeSeriesConvention = new TimeSeriesConvention();
			Credentials = new OperationCredentials(null, CredentialCache.DefaultNetworkCredentials);
			Advanced = new TimeSeriesStoreAdvancedOperations(this);
			Admin = new TimeSeriesStoreAdminOperations(this);
			batch = new Lazy<BatchOperationsStore>(() => new BatchOperationsStore(this));
			isInitialized = false;
		}

		public void Initialize(bool ensureDefaultTimeSeriesExists = false)
		{
			if(isInitialized)
				throw new InvalidOperationException(string.Format("TimeSeriesStore already initialized. (name = {0})", Name));

			isInitialized = true;
			InitializeSecurity();

			if (ensureDefaultTimeSeriesExists && !string.IsNullOrWhiteSpace(Name))
			{
				if (String.IsNullOrWhiteSpace(Name))
					throw new InvalidOperationException("Name is null or empty and ensureDefaultTimeSeriesExists = true --> cannot create default time series with empty name");

				Admin.CreateTimeSeriesAsync(MultiDatabase.CreateTimeSeriesDocument(Name)).ConfigureAwait(false).GetAwaiter().GetResult();
			}			

			replicationInformer = new TimeSeriesReplicationInformer(JsonRequestFactory, this, TimeSeriesConvention); // make sure it is initialized
		}

		public ITimeSeriesChanges Changes(string timeSeries = null)
		{
			AssertInitialized();

			if (string.IsNullOrWhiteSpace(timeSeries))
				timeSeries = Name;

			return timeSeriesChanges.GetOrAdd(timeSeries, CreateTimeSeriesChanges);
		}

		private ITimeSeriesChanges CreateTimeSeriesChanges(string timeSeries)
		{
			if (string.IsNullOrEmpty(Url))
				throw new InvalidOperationException("Changes API requires usage of server/client");
			
			AssertInitialized();

			var tenantUrl = Url + "/ts/" + timeSeries;

			using (NoSynchronizationContext.Scope())
			{
				var client = new TimeSeriesChangesClient(tenantUrl,
					Credentials.ApiKey,
					Credentials.Credentials,
					JsonRequestFactory,
					TimeSeriesConvention,
					() => timeSeriesChanges.Remove(timeSeries));

				return client;
			}
		}

		public event EventHandler AfterDispose;

		public bool WasDisposed { get; private set; }

		internal void AssertInitialized()
		{
			if (!isInitialized)
				throw new InvalidOperationException(string.Format("You cannot open a session or access the time series commands before initializing the time series store. Did you forget calling Initialize()? (TimeSeries store name = {0})", Name));
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

		public TimeSeriesConvention TimeSeriesConvention { get; set; }

		internal JsonSerializer JsonSerializer { get; set; }

		public TimeSeriesStoreAdvancedOperations Advanced { get; private set; }

		public TimeSeriesStoreAdminOperations Admin { get; private set; }
		
		protected HttpJsonRequest CreateHttpJsonRequest(string requestUriString, HttpMethod httpMethod, bool disableRequestCompression = false, bool disableAuthentication = false)
		{
			return JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, requestUriString, httpMethod, Credentials, TimeSeriesConvention)
			{
				DisableRequestCompression = disableRequestCompression,
				DisableAuthentication = disableAuthentication
			});
		}

		public TimeSeriesReplicationInformer ReplicationInformer
		{
			get
			{
				return replicationInformer ?? (replicationInformer = new TimeSeriesReplicationInformer(JsonRequestFactory, this, TimeSeriesConvention));
			}
		}

		private void InitializeSecurity()
		{
			if (TimeSeriesConvention.HandleUnauthorizedResponseAsync != null)
				return; // already setup by the user

			if (string.IsNullOrEmpty(Credentials.ApiKey) == false)
				Credentials = null;

			var basicAuthenticator = new BasicAuthenticator(JsonRequestFactory.EnableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers);
			var securedAuthenticator = new SecuredAuthenticator();

			JsonRequestFactory.ConfigureRequest += basicAuthenticator.ConfigureRequest;
			JsonRequestFactory.ConfigureRequest += securedAuthenticator.ConfigureRequest;

			TimeSeriesConvention.HandleForbiddenResponseAsync = (forbiddenResponse, credentials) =>
			{
				if (credentials.ApiKey == null)
				{
					AssertForbiddenCredentialSupportWindowsAuth(forbiddenResponse);
					return null;
				}

				return null;
			};

			TimeSeriesConvention.HandleUnauthorizedResponseAsync = (unauthorizedResponse, credentials) =>
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