using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.OAuth;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.Counters.Actions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Counters
{
	/// <summary>
	/// implements administration level counters functionality
	/// </summary>
	public class CounterStore : ICounterStore
	{
		public CounterStore()
		{
			JsonSerializer = JsonExtensions.CreateDefaultJsonSerializer();
			JsonRequestFactory = new HttpJsonRequestFactory(Constants.NumberOfCachedRequests);
			Convention = new Convention();
			Credentials = new OperationCredentials(null, CredentialCache.DefaultNetworkCredentials);
		}

		public void Initialize(bool ensureDefaultCounterExists = false)
		{
			InitializeSecurity();

			if (ensureDefaultCounterExists && !string.IsNullOrWhiteSpace(DefaultCounterName))
			{
				CreateCounterAsync(new CountersDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\" + DefaultCounterName}
					},
				}, DefaultCounterName).Wait();
			}
		}

		public OperationCredentials Credentials { get; set; }

		public HttpJsonRequestFactory JsonRequestFactory { get; set; }

		public string Url { get; set; }

		public string DefaultCounterName { get; set; }

		public Convention Convention { get; set; }

		public JsonSerializer JsonSerializer { get; set; }

		/// <summary>
		/// Create new counter storage on the server.
		/// </summary>
		/// <param name="countersDocument">Settings for the counter storage. If null, default settings will be used, and the name specified in the client ctor will be used</param>
		/// <param name="counterName">Override counter storage name specified in client ctor. If null, the name already specified will be used</param>
		public async Task CreateCounterAsync(CountersDocument countersDocument, string counterName, bool shouldUpateIfExists = false, CancellationToken token = default(CancellationToken))
		{
			if (countersDocument == null)
				throw new ArgumentNullException("countersDocument");

			var urlTemplate = "{0}/admin/cs/{1}";
			if (shouldUpateIfExists)
				urlTemplate += "?update=true";

			var requestUriString = String.Format(urlTemplate, Url, counterName);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Put))
			{
				try
				{
					await request.WriteAsync(RavenJObject.FromObject(countersDocument)).WithCancellation(token).ConfigureAwait(false);
				}
				catch (ErrorResponseException e)
				{
					if (e.StatusCode == HttpStatusCode.Conflict)
						throw new InvalidOperationException("Cannot create counter storage with the name '" + counterName + "' because it already exists. Use CreateOrUpdateCounterStorageAsync in case you want to update an existing counter storage", e);

					throw;
				}					
			}
		}

		public async Task DeleteCounterStorageAsync(string counterName, bool hardDelete = false, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/admin/cs/{1}?hard-delete={2}", Url, counterName, hardDelete);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Delete))
			{
				try
				{
					await request.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
				}
				catch (ErrorResponseException e)
				{
					if (e.StatusCode == HttpStatusCode.NotFound)
						throw new InvalidOperationException(string.Format("Counter storage with specified name ({0}) doesn't exist", counterName));
					throw;
				}
			}
		}

		public CountersBatchOperation BatchOperation(string counterName, CountersBatchOptions options = null)
		{
			 return new CountersBatchOperation(this,counterName,options);
		}

		public CountersClient NewCounterClient(string counterName)
		{
			return new CountersClient(this,counterName);
		}

		public async Task<string[]> GetCounterStoragesNamesAsync(CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/cs/conterStorages", Url);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
				return response.ToObject<string[]>(JsonSerializer);
			}
		}

		public async Task<List<CounterStorageStats>> GetCounterStoragesStatsAsync(CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/cs/stats", Url);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
				return response.ToObject<List<CounterStorageStats>>(JsonSerializer);
			}
		}

		protected HttpJsonRequest CreateHttpJsonRequest(string requestUriString, string httpVerb, bool disableRequestCompression = false, bool disableAuthentication = false)
		{
			return JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, httpVerb, Credentials, Convention)
			{
				DisableRequestCompression = disableRequestCompression,
				DisableAuthentication = disableAuthentication
			});
		}

		public ProfilingInformation ProfilingInformation { get; private set; }


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

		private void AssertUnauthorizedCredentialSupportWindowsAuth(HttpResponseMessage response, ICredentials credentials)
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

		public virtual void Dispose()
		{
			
		}
	}
}