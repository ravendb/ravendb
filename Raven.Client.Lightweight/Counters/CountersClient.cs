using System;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.OAuth;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.Counters.Actions;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Client.Counters
{
	public class CountersClient : IHoldProfilingInformation, IDisposable
    {
        private OperationCredentials credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication;

	    //private readonly RemoteFileSystemChanges notifications;
		//private readonly IFileSystemClientReplicationInformer replicationInformer; //todo: implement replication and failover management on the client side
		//private int readStripingBase;

	    internal readonly JsonSerializer JsonSerializer = JsonExtensions.CreateDefaultJsonSerializer();

	    internal readonly HttpJsonRequestFactory JsonRequestFactory =
              new HttpJsonRequestFactory(DefaultNumberOfCachedRequests);

	    private const int DefaultNumberOfCachedRequests = 2048;
		public string ServerUrl { get; private set; }

		public string DefaultStorageName { get; private set; }

		public string CounterStorageUrl { get; private set; }

		public ICredentials Credentials { get; private set; }

		public string ApiKey { get; private set; }

		public ProfilingInformation ProfilingInformation { get; private set; }

		public OperationCredentials PrimaryCredentials
		{
			get { return credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication; }
		}

	    public CountersStats Stats { get; private set; }

	    public ReplicationClient Replication { get; private set; }

	    public CountersCommands Commands { get; private set; }

	    public CountersAdmin Admin { get; private set; }

	    public Convention Conventions { get; private set; }

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


		public CountersClient(string serverUrl, string counterStorageName, ICredentials credentials = null, string apiKey = null)
        {
	        try
            {
                ServerUrl = serverUrl;
                if (ServerUrl.EndsWith("/"))
                    ServerUrl = ServerUrl.Substring(0, ServerUrl.Length - 1);

				CounterStorageUrl = string.Format(CultureInfo.InvariantCulture, "{0}/counters/{1}", ServerUrl, DefaultStorageName);
				DefaultStorageName = counterStorageName;
                Credentials = credentials ?? CredentialCache.DefaultNetworkCredentials;
                ApiKey = apiKey;

				Conventions = new Convention();
                //replicationInformer = new RavenFileSystemReplicationInformer(convention, JsonRequestFactory);
                //readStripingBase = replicationInformer.GetReadStripingBase();
	            //todo: implement remote counter changes
                //notifications = new RemoteFileSystemChanges(serverUrl, apiKey, credentials, jsonRequestFactory, convention, replicationInformer, () => { });
                               
                InitializeSecurity();
				InitializeActions();
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }

        }

	    private void InitializeActions()
	    {
		    Admin = new CountersAdmin(this, Conventions);
		    Stats = new CountersStats(this, Conventions);
		    Replication = new ReplicationClient(this, Conventions);
		    Commands = new CountersCommands(this, Conventions);
	    }

	    private void InitializeSecurity()
        {
            if (Conventions.HandleUnauthorizedResponseAsync != null)
                return; // already setup by the user

		    if (string.IsNullOrEmpty(ApiKey) == false)
			    Credentials = null;

		    credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication = new OperationCredentials(ApiKey, Credentials);

            var basicAuthenticator = new BasicAuthenticator(JsonRequestFactory.EnableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers);
            var securedAuthenticator = new SecuredAuthenticator();

            JsonRequestFactory.ConfigureRequest += basicAuthenticator.ConfigureRequest;
            JsonRequestFactory.ConfigureRequest += securedAuthenticator.ConfigureRequest;

            Conventions.HandleForbiddenResponseAsync = (forbiddenResponse, credentials) =>
            {
                if (credentials.ApiKey == null)
                {
                    AssertForbiddenCredentialSupportWindowsAuth(forbiddenResponse);
                    return null;
                }

                return null;
            };

            Conventions.HandleUnauthorizedResponseAsync = (unauthorizedResponse, credentials) =>
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

	    public void Dispose()
        {
            //if (notifications != null)
            //    notifications.Dispose();
        }
    }
}
