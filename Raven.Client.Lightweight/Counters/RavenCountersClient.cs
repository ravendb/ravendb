using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.OAuth;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.Counters.Connections;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Counters
{
    public class RavenCountersClient : IDisposable, IHoldProfilingInformation
    {
        private OperationCredentials credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication;
		private readonly CounterConvention convention;

		//private readonly RemoteFileSystemChanges notifications;
		//private readonly IFileSystemClientReplicationInformer replicationInformer; //todo: implement replication and failover management on the client side
		//private int readStripingBase;
		
        private HttpJsonRequestFactory JsonRequestFactory =
              new HttpJsonRequestFactory(DefaultNumberOfCachedRequests);

        private const int DefaultNumberOfCachedRequests = 2048;

        /// <summary>
        /// Notify when the failover status changed
        /// </summary>
		/*public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged
		{
			add { replicationInformer.FailoverStatusChanged += value; }
			remove { replicationInformer.FailoverStatusChanged -= value; }
		}
		
		/// <summary>
		/// Allow access to the replication informer used to determine how we replicate requests
		/// </summary>
		public IFileSystemClientReplicationInformer ReplicationInformer
		{
			get { return replicationInformer; }
		}
		 */

		public ProfilingInformation ProfilingInformation { get; private set; }

        public OperationCredentials PrimaryCredentials
        {
            get { return credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication; }
        }

		public RavenCountersClient(string serverUrl, string counterStorageName, ICredentials credentials = null, string apiKey = null)
        {
            try
            {
                ServerUrl = serverUrl;
                if (ServerUrl.EndsWith("/"))
                    ServerUrl = ServerUrl.Substring(0, ServerUrl.Length - 1);

				CounterStorageName = counterStorageName;
                Credentials = credentials ?? CredentialCache.DefaultNetworkCredentials;
                ApiKey = apiKey;

				convention = new CounterConvention();
                //replicationInformer = new RavenFileSystemReplicationInformer(convention, JsonRequestFactory);
                //readStripingBase = replicationInformer.GetReadStripingBase();
	            //todo: implement remote counter changes
                //notifications = new RemoteFileSystemChanges(serverUrl, apiKey, credentials, jsonRequestFactory, convention, replicationInformer, () => { });
                               
                InitializeSecurity();
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public string ServerUrl { get; private set; }

        public string CounterStorageName { get; private set; }

        public string CounterStorageUrl
        {
            get { return string.Format("{0}/counters/{1}", ServerUrl, CounterStorageName); }
        }

        public ICredentials Credentials { get; private set; }

        public string ApiKey { get; private set; }

        private void InitializeSecurity()
        {
            if (convention.HandleUnauthorizedResponseAsync != null)
                return; // already setup by the user

            if (string.IsNullOrEmpty(ApiKey) == false)
            {
                Credentials = null;
            }

            credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication = new OperationCredentials(ApiKey, Credentials);

            var basicAuthenticator = new BasicAuthenticator(JsonRequestFactory.EnableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers);
            var securedAuthenticator = new SecuredAuthenticator();

            JsonRequestFactory.ConfigureRequest += basicAuthenticator.ConfigureRequest;
            JsonRequestFactory.ConfigureRequest += securedAuthenticator.ConfigureRequest;

            convention.HandleForbiddenResponseAsync = (forbiddenResponse, credentials) =>
            {
                if (credentials.ApiKey == null)
                {
                    AssertForbiddenCredentialSupportWindowsAuth(forbiddenResponse);
                    return null;
                }

                return null;
            };

            convention.HandleUnauthorizedResponseAsync = (unauthorizedResponse, credentials) =>
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
                    oauthSource = ServerUrl + "/OAuth/API-Key";

                return securedAuthenticator.DoOAuthRequestAsync(ServerUrl, oauthSource, credentials.ApiKey);
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

		public StatsClient Stats
		{
			get
			{
				return new StatsClient(this, Convention);
			}
		}

		public ReplicationClient Replication
		{
			get { return new ReplicationClient(this, Convention); }
		}

		public CounterClient Counter
		{
			get
			{
				return new CounterClient(this, Convention);
			}
		}

		public AdminClient Admin
		{
			get
			{
				return new AdminClient(this, Convention);
			}
		}

		public CounterConvention Convention
		{
			get { return Convention; }
		}

        public class StatsClient : IHoldProfilingInformation
        {
			private readonly OperationCredentials credentials;
			private readonly HttpJsonRequestFactory jsonRequestFactory;
			private readonly string counterStorageUrl;
			private readonly CounterConvention convention;

            public StatsClient(RavenCountersClient countersClient, CounterConvention convention)
            {
				credentials = countersClient.PrimaryCredentials;
				jsonRequestFactory = countersClient.JsonRequestFactory;
				counterStorageUrl = countersClient.CounterStorageUrl;
				this.convention = convention;
            }

			public ProfilingInformation ProfilingInformation { get; private set; }

            public async Task<List<CounterStorageStats>> GetCounterStorageStats()
            {
                var requestUriString = string.Format("{0}/stats", counterStorageUrl);

	            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Get, credentials, convention)))
	            {
		            try
		            {
			            var response = await request.ReadResponseJsonAsync();
			            return new JsonSerializer().Deserialize<List<CounterStorageStats>>(new RavenJTokenReader(response));
		            }
		            catch (Exception e)
		            {
			            throw e;
			            //throw e.TryThrowBetterError();
		            }
	            }
            }

            public async Task<List<CountersStorageMetrics>> GetCounterStorageMetrics()
            {
                var requestUriString = string.Format("{0}/metrics", counterStorageUrl);

				using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Get, credentials, convention)))
	            {
		            try
		            {
			            var response = await request.ReadResponseJsonAsync();
			            return new JsonSerializer().Deserialize<List<CountersStorageMetrics>>(new RavenJTokenReader(response));
		            }
		            catch (Exception e)
		            {
			            throw e;
			            //throw e.TryThrowBetterError();
		            }
	            }
            }

            public async Task<List<CounterStorageReplicationStats>> GetCounterStoragRelicationStats()
            {
                var requestUriString = string.Format("{0}/replications/stats", counterStorageUrl);

				using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Get, credentials, convention)))
	            {
		            try
		            {
			            var response = await request.ReadResponseJsonAsync();
			            return new JsonSerializer().Deserialize<List<CounterStorageReplicationStats>>(new RavenJTokenReader(response));
		            }
		            catch (Exception e)
		            {
			            throw e;
			            //throw e.TryThrowBetterError();
		            }
	            }
            }
        }

        public class ReplicationClient : IHoldProfilingInformation
        {
			private readonly OperationCredentials credentials;
			private readonly HttpJsonRequestFactory jsonRequestFactory;
			private readonly string counterStorageUrl;
			private readonly RavenCountersClient countersClient;
			private readonly CounterConvention convention;

			public ReplicationClient(RavenCountersClient countersClient, CounterConvention convention)
            {
				credentials = countersClient.PrimaryCredentials;
				jsonRequestFactory = countersClient.JsonRequestFactory;
				counterStorageUrl = countersClient.CounterStorageUrl;
				this.countersClient = countersClient;
				this.convention = convention;
            }

			public ProfilingInformation ProfilingInformation { get; private set; }

			public async Task<CounterStorageReplicationDocument> GetReplications()
			{
				var requestUriString = String.Format("{0}/replications/get", counterStorageUrl);

				using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Get, credentials, convention)))
				{
					try
					{
						var response = await request.ReadResponseJsonAsync();
						return response.Value<CounterStorageReplicationDocument>();
					}
					catch (Exception e)
					{
						throw e;
						//throw e.TryThrowBetterError();
					}
				}
			}

			public async Task SaveReplications(CounterStorageReplicationDocument newReplicationDocument)
			{
				var requestUriString = String.Format("{0}/replications/save", counterStorageUrl);

				using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Post, credentials, convention)))
				{
					try
					{
						await request.WriteAsync(RavenJObject.FromObject(newReplicationDocument));
						var response = await request.ReadResponseJsonAsync();
					}
					catch (Exception e)
					{
						throw e;
						//throw e.TryThrowBetterError();
					}
				}
			}
        }

	    public class CounterClient: IHoldProfilingInformation
	    {
			private readonly OperationCredentials credentials;
			private readonly HttpJsonRequestFactory jsonRequestFactory;
			private readonly string counterStorageUrl;
			private readonly RavenCountersClient countersClient;
			private readonly CounterConvention convention;

			public CounterClient(RavenCountersClient countersClient, CounterConvention convention)
            {
				credentials = countersClient.PrimaryCredentials;
				jsonRequestFactory = countersClient.JsonRequestFactory;
				counterStorageUrl = countersClient.CounterStorageUrl;
				this.countersClient = countersClient;
				this.convention = convention;
            }

			public ProfilingInformation ProfilingInformation { get; private set; }

			public async Task Change(string group, string counterName, long delta)
		    {
				var requestUriString = String.Format("{0}/change?group={1}&counterName={2}&delta={3}",
					counterStorageUrl, group, counterName, delta);

				using (var request = countersClient.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Post, credentials, convention)))
				{
					try
					{
						var response = await request.ReadResponseJsonAsync();
					}
					catch (Exception e)
					{
						throw e;
						//throw e.TryThrowBetterError();
					}
				}
		    }

			public async Task Reset(string group, string counterName)
		    {
				var requestUriString = String.Format("{0}/change?group={1}&counterName={2}",
					counterStorageUrl, group, counterName);

				using (var request = countersClient.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Post, credentials, convention)))
				{
					try
					{
						var response = await request.ReadResponseJsonAsync();
					}
					catch (Exception e)
					{
						throw e;
						//throw e.TryThrowBetterError();
					}
				}
		    }

			public void Increment(string group, string counterName)
			{
				Change(group, counterName, 1);
			}

			public void Decrement(string group, string counterName)
		    {
				Change(group, counterName, -1);
		    }

			public async Task<long> GetOverallTotal(string group, string counterName)
		    {
				var requestUriString = String.Format("{0}/getCounterOverallTotal?group={1}&counterName={2}",
					counterStorageUrl, group, counterName);

				using (var request = countersClient.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Get, credentials, convention)))
				{
					try
					{
						var response = await request.ReadResponseJsonAsync();
						return response.Value<long>();
					}
					catch (Exception e)
					{
						throw e;
						//throw e.TryThrowBetterError();
					}
				}
		    }

			public async Task<List<CounterView.ServerValue>> GetServersValues(string group, string counterName)
		    {
				var requestUriString = String.Format("{0}/getCounterServersValues?group={1}&counterName={2}",
					counterStorageUrl, group, counterName);

				using (var request = countersClient.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Get, credentials, convention)))
				{
					try
					{
						var response = await request.ReadResponseJsonAsync();
						return response.Value<List<CounterView.ServerValue>>();
					}
					catch (Exception e)
					{
						throw e;
						//throw e.TryThrowBetterError();
					}
				}
		    }

		    public CounterBatch CreateBatch()
		    {
				return new CounterBatch(countersClient, convention);
		    }

			public class CounterBatch : IHoldProfilingInformation
		    {
			    private readonly OperationCredentials credentials;
			    private readonly HttpJsonRequestFactory jsonRequestFactory;
				private readonly string counterStorageUrl;
			    private readonly CounterConvention convention;

			    private readonly ConcurrentDictionary<string, long> counterData = new ConcurrentDictionary<string, long>();

			    public CounterBatch(RavenCountersClient countersClient, CounterConvention convention)
			    {
				    credentials = countersClient.PrimaryCredentials;
				    jsonRequestFactory = countersClient.JsonRequestFactory;
					counterStorageUrl = countersClient.CounterStorageUrl;
				    this.convention = convention;
			    }

				public ProfilingInformation ProfilingInformation { get; private set; }

			    public void Change(string group, string counterName, long delta)
			    {
				    string counterFullName = String.Join(Constants.GroupSeperatorString, new[] { group, counterName });

				    counterData.AddOrUpdate(counterFullName, delta, (key, existingVal) => existingVal + delta);
			    }

			    public void Increment(string group, string counterName)
			    {
				    Change(group, counterName, 1);
			    }

			    public void Decrement(string group, string counterName)
			    {
				    Change(group, counterName, -1);
			    }

				public async Task Write()
			    {
					var counterChanges = new List<CounterChanges>();
				    counterData.ForEach(keyValue =>
				    {
					    var newCounterChange = 
							new CounterChanges
								{
									FullCounterName = keyValue.Key,
									Delta = keyValue.Value
								};
						counterChanges.Add(newCounterChange);
				    });

					var requestUriString = String.Format("{0}/batch", counterStorageUrl);

					using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Post, credentials, convention)))
				    {
					    try
					    {
						    await request.WriteAsync(RavenJObject.FromObject(counterChanges));
						    var response = await request.ReadResponseJsonAsync();
					    }
					    catch (Exception e)
					    {
						    throw e;
						    //throw e.TryThrowBetterError();
					    }
				    }
			    }			
		    }
	    }

		public class AdminClient : IHoldProfilingInformation
        {
			private readonly OperationCredentials credentials;
			private readonly HttpJsonRequestFactory jsonRequestFactory;
			private readonly string serverUrl;
			private readonly string counterStorageName;
			private readonly CounterConvention convention;

			public AdminClient(RavenCountersClient countersClient, CounterConvention convention)
            {
				credentials = countersClient.PrimaryCredentials;
				jsonRequestFactory = countersClient.JsonRequestFactory;
				serverUrl = countersClient.ServerUrl;
				counterStorageName = countersClient.CounterStorageName;
				this.convention = convention;
            }

			public ProfilingInformation ProfilingInformation { get; private set; }

			public async Task<string[]> GetCounterStoragesNames()
            {
				var requestUriString = string.Format("{0}/counterStorage/conterStorages", serverUrl);

				using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Get, credentials, convention)))
				{
					try
					{
						var response = await request.ReadResponseJsonAsync();
						return new JsonSerializer().Deserialize<string[]>(new RavenJTokenReader(response));
					}
					catch (Exception e)
					{
						//throw e.TryThrowBetterError();
						throw e;
					}
				}
            }

            public async Task<List<CounterStorageStats>> GetCounterStoragesStats()
            {
				var requestUriString = string.Format("{0}/counterStorage/stats", serverUrl);

				using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Get, credentials, convention)))
	            {
		            try
		            {
			            var response = await request.ReadResponseJsonAsync();
			            return new JsonSerializer().Deserialize<List<CounterStorageStats>>(new RavenJTokenReader(response));
		            }
		            catch (Exception e)
		            {
			            throw e;
			            //throw e.TryThrowBetterError();
		            }
	            }
            }

			public async Task CreateCounterStorageAsync(DatabaseDocument databaseDocument, string newCounterStorageName = null)
			{
				var requestUriString = string.Format("{0}/counterstorage/admin/{1}", serverUrl,
													 newCounterStorageName ?? counterStorageName);

				using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Put, credentials, convention)))
				{
					try
					{
						await request.WriteAsync(RavenJObject.FromObject(databaseDocument));
					}
					catch (ErrorResponseException e)
					{
						//if (e.StatusCode == HttpStatusCode.Conflict)
						//	throw new InvalidOperationException("Cannot create counter storage with the name '" + newCounterStorageName + "' because it already exists. Use CreateOrUpdateCounterStorageAsync in case you want to update an existing counter storage", e)
						//		.TryThrowBetterError();

						throw e;
					}
					catch (Exception e)
					{
						throw e;
						//throw e.TryThrowBetterError();
					}
				}
			}

			public async Task CreateOrUpdateCounterStorageAsync(DatabaseDocument databaseDocument, string newCounterStorageName = null)
			{
				var requestUriString = string.Format("{0}/counterstorage/admin/{1}?update=true", serverUrl,
													 newCounterStorageName ?? counterStorageName);

				using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Put, credentials, convention)))
				{
					try
					{
						await request.WriteAsync(RavenJObject.FromObject(databaseDocument));
					}
					catch (Exception e)
					{
						throw e;
						//throw e.TryThrowBetterError();
					}
				}
			}

			public async Task DeleteCounterStorageAsync(string counterStorageNameToDelete = null, bool hardDelete = false)
			{
				var requestUriString = string.Format("{0}/counterstorage/admin/{1}?hard-delete={2}", serverUrl,
														counterStorageNameToDelete ?? counterStorageName, hardDelete);

				using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, HttpMethod.Delete, credentials, convention)))
				{
					try
					{
						await request.ExecuteRequestAsync();
					}
					catch (Exception e)
					{
						throw e;
						//throw e.TryThrowBetterError();
					}
				}
			}
        }

        public void Dispose()
        {
            //if (notifications != null)
            //    notifications.Dispose();
        }
    }
}
