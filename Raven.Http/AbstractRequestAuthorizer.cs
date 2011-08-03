using System;
using System.Net;
using Raven.Http.Abstractions;

namespace Raven.Http
{
    public abstract class AbstractRequestAuthorizer
    {
        private Func<IRavenHttpConfiguration> settings;
        private Func<IResourceStore> database;
        protected HttpServer server;
        private Func<string> tenantId;

        public IResourceStore ResourceStore { get { return database(); } }
        public IRavenHttpConfiguration Settings { get { return settings(); } }
        public string TenantId { get { return tenantId(); } }

        public void Initialize(Func<IResourceStore> databaseGetter, Func<IRavenHttpConfiguration> settingsGetter, Func<string> tenantIdGetter, HttpServer theServer)
        {
            this.server = theServer;
            this.database = databaseGetter;
            this.settings = settingsGetter;
            this.tenantId = tenantIdGetter;
        }
        
        public abstract bool Authorize(IHttpContext context);

    	public static bool IsGetRequest(string httpMethod, string requestPath)
		{
			return (httpMethod == "GET" || httpMethod == "HEAD") ||
				   httpMethod == "POST" && (requestPath == "/multi_get/" || requestPath == "/multi_get");
		}
    }
}